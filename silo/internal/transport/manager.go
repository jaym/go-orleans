package transport

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	gogoproto "github.com/gogo/protobuf/proto"

	"github.com/jaym/go-orleans/future"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type Manager struct {
	lock       sync.RWMutex
	log        logr.Logger
	handler    TransportHandler
	transports map[cluster.Location]*managedTransport
	nodeNames  []string
}

func NewManager(log logr.Logger, handler TransportHandler) *Manager {
	return &Manager{
		log:        log,
		handler:    handler,
		transports: make(map[cluster.Location]*managedTransport),
		nodeNames:  []string{},
	}
}

type TransportHandler interface {
	ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, payload []byte, promise InvokeMethodPromise)
	ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, payload []byte, registrationTimeout time.Duration, promise RegisterObserverPromise)
	ReceiveObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte)
	ReceiveUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, promise UnsubscribeObserverPromise)
}

type managedTransport struct {
	internal *managedTransportInternal
}

type InvokeMethodResponse struct {
	Response []byte
}
type InvokeMethodPromise = future.Promise[InvokeMethodResponse]
type InvokeMethodFuture = future.Future[InvokeMethodResponse]

type RegisterObserverResponse struct{}
type RegisterObserverPromise = future.Promise[RegisterObserverResponse]
type RegisterObserverFuture = future.Future[RegisterObserverResponse]

type UnsubscribeObserverResponse struct{}
type UnsubscribeObserverPromise = future.Promise[UnsubscribeObserverResponse]
type UnsubscribeObserverFuture = future.Future[UnsubscribeObserverResponse]

type managedTransportInternal struct {
	TransportHandler
	log                         logr.Logger
	transport                   cluster.Transport
	lock                        sync.Mutex
	stopped                     bool
	bgWorkerCancel              context.CancelFunc
	deadlineHeap                *DeadlineHeap
	invokeMethodPromises        map[string]InvokeMethodPromise
	registerObserverPromises    map[string]RegisterObserverPromise
	unsubscribeObserverPromises map[string]UnsubscribeObserverPromise
}

func (h *managedTransportInternal) backgroundCleanupWorker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
	LOOP:
		for {
			select {
			case <-ctx.Done():
				break LOOP
			case <-ticker.C:
				func() {
					h.lock.Lock()
					defer h.lock.Unlock()
					if h.stopped {
						return
					}
					h.deadlineHeap.Expire(h.expirePromises)
				}()
			}
		}
	}()
}

var ErrTransportStopped = errors.New("transport stopped")

func (h *managedTransportInternal) stop() error {
	errTransportStop := h.transport.Stop()
	h.bgWorkerCancel()
	h.lock.Lock()
	defer h.lock.Unlock()

	for _, p := range h.invokeMethodPromises {
		p.Reject(ErrTransportStopped)
	}
	h.invokeMethodPromises = nil

	for _, p := range h.registerObserverPromises {
		p.Reject(ErrTransportStopped)
	}
	h.registerObserverPromises = nil

	for _, p := range h.unsubscribeObserverPromises {
		p.Reject(ErrTransportStopped)
	}
	h.unsubscribeObserverPromises = nil

	h.stopped = true

	return errTransportStop
}

func (h *managedTransportInternal) registerRegisterObserverPromise(uuid string, deadline time.Time) RegisterObserverFuture {

	f, p := future.NewFuture[RegisterObserverResponse](deadline)
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.stopped {
		p.Reject(ErrTransportStopped)
	} else {
		h.deadlineHeap.ExpireAndAdd(RegisterObserverRequestType, uuid, deadline, h.expirePromises)
		h.registerObserverPromises[uuid] = p
	}
	return f
}

func (h *managedTransportInternal) registerUnsubscribeObserverPromise(uuid string, deadline time.Time) UnsubscribeObserverFuture {
	f, p := future.NewFuture[UnsubscribeObserverResponse](deadline)
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.stopped {
		p.Reject(ErrTransportStopped)
	} else {
		h.deadlineHeap.ExpireAndAdd(UnregisterObserverRequestType, uuid, deadline, h.expirePromises)
		h.unsubscribeObserverPromises[uuid] = p
	}
	return f
}

func (h *managedTransportInternal) expirePromises(typ RequestType, uuid string) {
	// must only be called while holding the lock
	switch typ {
	case InvokeMethodRequestType:
		if p, ok := h.invokeMethodPromises[uuid]; ok {
			p.Reject(context.DeadlineExceeded)
			delete(h.invokeMethodPromises, uuid)
		}
	case RegisterObserverRequestType:
		if p, ok := h.registerObserverPromises[uuid]; ok {
			p.Reject(context.DeadlineExceeded)
			delete(h.registerObserverPromises, uuid)
		}
	case UnregisterObserverRequestType:
		if p, ok := h.unsubscribeObserverPromises[uuid]; ok {
			p.Reject(context.DeadlineExceeded)
			delete(h.unsubscribeObserverPromises, uuid)
		}
	}
}

func (h *managedTransportInternal) registerInvokeMethodPromise(uuid string, deadline time.Time) InvokeMethodFuture {
	f, p := future.NewFuture[InvokeMethodResponse](deadline)
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.stopped {
		p.Reject(ErrTransportStopped)
	} else {
		h.deadlineHeap.ExpireAndAdd(InvokeMethodRequestType, uuid, deadline, h.expirePromises)
		h.invokeMethodPromises[uuid] = p
	}
	return f
}

func (h *managedTransportInternal) ReceiveAckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errData []byte) {
	h.lock.Lock()
	defer h.lock.Unlock()

	p, ok := h.registerObserverPromises[uuid]
	if !ok {
		h.log.V(1).Info("promise not found", "handler", "ack-register-observer", "receiver", receiver, "uuid", uuid)
		return
	}
	rejectOrResolve(p, errData, RegisterObserverResponse{})
	delete(h.invokeMethodPromises, uuid)
}

func (h *managedTransportInternal) ReceiveInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, errData []byte) {
	h.lock.Lock()
	defer h.lock.Unlock()

	p, ok := h.invokeMethodPromises[uuid]
	if !ok {
		h.log.V(1).Info("promise not found", "handler", "invoke-method-response", "receiver", receiver, "uuid", uuid)
		return
	}
	rejectOrResolve(p, errData, InvokeMethodResponse{
		Response: payload,
	})

	delete(h.invokeMethodPromises, uuid)
}

func (h *managedTransportInternal) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte, deadline time.Time) {
	p := future.NewFuncPromise(deadline, func(imr InvokeMethodResponse, errIn error) {
		if err := h.transport.EnqueueInvokeMethodResponse(context.TODO(), sender, uuid, payload, encodeError(errIn)); err != nil {
			h.log.V(0).Error(err, "failed to response to invoke method request",
				"sender", sender,
				"receiver", receiver,
				"method", method, "uuid", uuid)
		}
	})
	h.TransportHandler.ReceiveInvokeMethodRequest(ctx, sender, receiver, method, payload, p)
}

func (h *managedTransportInternal) ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte, opts cluster.EnqueueRegisterObserverRequestOptions, deadline time.Time) {
	p := future.NewFuncPromise(deadline, func(_ RegisterObserverResponse, e error) {
		if err := h.transport.EnqueueAckRegisterObserver(context.TODO(), observer, uuid, encodeError(e)); err != nil {
			h.log.V(0).Error(err, "failed to response to ack observer registration",
				"observer", observer,
				"observable", observable,
				"name", name,
				"uuid", uuid)
		}
	})
	h.TransportHandler.ReceiveRegisterObserverRequest(ctx, observer, observable, name, payload, opts.RegistrationTimeout, p)
}

func (h *managedTransportInternal) ReceiveUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, deadline time.Time) {
	p := future.NewFuncPromise(deadline, func(_ UnsubscribeObserverResponse, e error) {
		if err := h.transport.EnqueueAckUnsubscribeObserver(context.TODO(), observer, uuid, encodeError(e)); err != nil {
			h.log.V(0).Error(err, "failed to response to ack unsubscribe observer",
				"observer", observer,
				"observable", observable,
				"name", name,
				"uuid", uuid)
		}
	})
	h.TransportHandler.ReceiveUnsubscribeObserverRequest(ctx, observer, observable, name, p)
}

func (h *managedTransportInternal) ReceiveAckUnsubscribeObserver(ctx context.Context, receiver grain.Identity, uuid string, errData []byte) {
	h.lock.Lock()
	defer h.lock.Unlock()

	p, ok := h.unsubscribeObserverPromises[uuid]
	if !ok {
		h.log.V(1).Info("promise not found", "handler", "ack-register-observer", "receiver", receiver, "uuid", uuid)
		return
	}
	rejectOrResolve(p, errData, UnsubscribeObserverResponse{})
	delete(h.invokeMethodPromises, uuid)
}

func (m *Manager) AddTransport(nodeName cluster.Location, creator func() (cluster.Transport, error)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.transports[nodeName]; ok {
		return errors.New("transport already exists")
	}

	transport, err := creator()
	if err != nil {
		return err
	}

	bgWorkerCxt, bgWorkerCancel := context.WithCancel(context.Background())
	mt := &managedTransport{
		internal: &managedTransportInternal{
			log:                         m.log.WithName(string(nodeName)),
			transport:                   transport,
			TransportHandler:            m.handler,
			bgWorkerCancel:              bgWorkerCancel,
			deadlineHeap:                NewDeadlineHeap(),
			invokeMethodPromises:        make(map[string]InvokeMethodPromise),
			registerObserverPromises:    make(map[string]RegisterObserverPromise),
			unsubscribeObserverPromises: make(map[string]UnsubscribeObserverPromise),
		},
	}
	mt.internal.backgroundCleanupWorker(bgWorkerCxt)
	m.transports[nodeName] = mt
	m.nodeNames = append(m.nodeNames, string(nodeName))
	return transport.Listen(mt.internal)
}

func (m *Manager) RemoveTransport(nodeName cluster.Location) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if tp, ok := m.transports[nodeName]; ok {
		if err := tp.internal.stop(); err != nil {
			m.log.Error(err, "failed to stop transport", "node", nodeName)
		}
		delete(m.transports, nodeName)
		nodeNames := make([]string, 0, len(m.transports))
		for l := range m.transports {
			nodeNames = append(nodeNames, string(l))
		}
		m.nodeNames = nodeNames
	}
}

func (m *Manager) IsMember(nodeName cluster.Location) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	_, ok := m.transports[nodeName]
	return ok
}

func (m *Manager) InvokeMethod(ctx context.Context, sender grain.Identity, receiver cluster.GrainAddress, method string, uuid string, payload []byte) (InvokeMethodFuture, error) {
	t, err := m.getTransport(receiver.Location)
	if err != nil {
		return nil, err
	}

	deadline, _ := ctx.Deadline()

	f := t.internal.registerInvokeMethodPromise(uuid, deadline)
	if err := t.internal.transport.EnqueueInvokeMethodRequest(ctx, sender, receiver.Identity, method, uuid, payload); err != nil {
		// TODO: if an error is returned, its possible that we never resolved the promise
		return nil, err
	}
	return f, nil
}

func (m *Manager) RegisterObserver(ctx context.Context, observer grain.Identity, observable cluster.GrainAddress, name string, uuid string, payload []byte, registrationTimeout time.Duration) (RegisterObserverFuture, error) {
	t, err := m.getTransport(observable.Location)
	if err != nil {
		return nil, err
	}

	deadline, _ := ctx.Deadline()

	f := t.internal.registerRegisterObserverPromise(uuid, deadline)
	if err := t.internal.transport.EnqueueRegisterObserverRequest(ctx, observer, observable.Identity, name, uuid, payload, cluster.EnqueueRegisterObserverRequestOptions{
		RegistrationTimeout: registrationTimeout,
	}); err != nil {
		// TODO: if an error is returned, its possible that we never resolved the promise
		return nil, err
	}
	return f, nil
}

func (m *Manager) UnsubscribeObserver(ctx context.Context, observer grain.Identity, observable cluster.GrainAddress, name string, uuid string) (UnsubscribeObserverFuture, error) {
	t, err := m.getTransport(observable.Location)
	if err != nil {
		return nil, err
	}

	deadline, _ := ctx.Deadline()

	f := t.internal.registerUnsubscribeObserverPromise(uuid, deadline)
	if err := t.internal.transport.EnqueueUnsubscribeObserverRequest(ctx, observer, observable.Identity, name, uuid); err != nil {
		// TODO: if an error is returned, its possible that we never resolved the promise
		return nil, err
	}
	return f, nil
}

func (m *Manager) ObserverNotificationAsync(ctx context.Context, sender grain.Identity, receivers []cluster.GrainAddress, observableType string, name string, payload []byte) error {
	locations := map[cluster.Location][]grain.Identity{}
	for _, r := range receivers {
		locations[r.Location] = append(locations[r.Location], r.Identity)
	}
	var combinedErrors error
	for location, rs := range locations {
		t, err := m.getTransport(location)
		if err != nil {
			combinedErrors = errors.CombineErrors(combinedErrors, err)
			continue
		}
		if err := t.internal.transport.EnqueueObserverNotification(ctx, sender, rs, observableType, name, payload); err != nil {
			combinedErrors = errors.CombineErrors(combinedErrors, err)
		}
	}
	return combinedErrors
}

func (m *Manager) RandomNode() string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	idx := rand.Intn(len(m.nodeNames))
	return m.nodeNames[idx]
}

func (m *Manager) getTransport(l cluster.Location) (*managedTransport, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	t, ok := m.transports[l]
	if !ok {
		return nil, fmt.Errorf("no transport for node %s", l)
	}
	return t, nil
}

func encodeError(err error) []byte {
	if err == nil {
		return nil
	}
	encodedErr := errors.EncodeError(context.Background(), err)
	if data, err := gogoproto.Marshal(&encodedErr); err != nil {
		panic(err)
	} else {
		return data
	}
}

func rejectOrResolve[T any](p future.Promise[T], errData []byte, val T) {
	if len(errData) > 0 {
		var encodedError errors.EncodedError
		if err := gogoproto.Unmarshal(errData, &encodedError); err != nil {
			p.Reject(err)
			return
		}
		decodedErr := errors.DecodeError(context.Background(), encodedError)
		p.Reject(decodedErr)
	} else {
		p.Resolve(val)
	}
}
