package transport

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"

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

type InvokeMethodPromise interface {
	Resolve(payload []byte, errData []byte)
	Deadline() time.Time
}

type invokeMethodFuncPromise struct {
	deadline time.Time
	f        func(payload []byte, err []byte)
}

func (p invokeMethodFuncPromise) Resolve(payload []byte, errData []byte) {
	if len(errData) > 0 {
		p.f(nil, errData)
	} else {
		p.f(payload, nil)
	}
}

func (p invokeMethodFuncPromise) Deadline() time.Time {
	return p.deadline
}

type invokeMethodChanFuture struct {
	c chan struct {
		payload []byte
		errData []byte
	}
}

func (f invokeMethodChanFuture) Await(ctx context.Context) ([]byte, []byte, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case v := <-f.c:
		return v.payload, v.errData, nil
	}
}

type InvokeMethodFuture interface {
	Await(ctx context.Context) (payload []byte, errData []byte, err error)
}

type RegisterObserverPromise interface {
	Resolve(errData []byte)
	Deadline() time.Time
}

type RegisterObserverFuture interface {
	Await(ctx context.Context) (errData []byte, err error)
}

type observerFuncPromise struct {
	deadline time.Time
	f        func(err []byte)
}

func (p observerFuncPromise) Resolve(errData []byte) {
	if len(errData) > 0 {
		p.f(errData)
	} else {
		p.f(nil)
	}
}

func (p observerFuncPromise) Deadline() time.Time {
	return p.deadline
}

type observerChanFuture struct {
	c chan struct {
		errData []byte
	}
}

func (f observerChanFuture) Await(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v := <-f.c:
		return v.errData, nil
	}
}

type UnsubscribeObserverPromise interface {
	Resolve(errData []byte)
}

type UnsubscribeObserverFuture interface {
	Await(ctx context.Context) (errData []byte, err error)
}

type managedTransport struct {
	internal *managedTransportInternal
}

type managedTransportInternal struct {
	TransportHandler
	log                         logr.Logger
	transport                   cluster.Transport
	lock                        sync.Mutex
	stopped                     bool
	invokeMethodPromises        map[string]InvokeMethodPromise
	registerObserverPromises    map[string]RegisterObserverPromise
	unsubscribeObserverPromises map[string]UnsubscribeObserverPromise
}

func (h *managedTransportInternal) stop() error {
	errTransportStop := h.transport.Stop()
	h.lock.Lock()
	defer h.lock.Unlock()

	for _, p := range h.invokeMethodPromises {
		// TODO: better error handling
		p.Resolve(nil, []byte("transport stopped"))
	}
	h.invokeMethodPromises = nil

	for _, p := range h.registerObserverPromises {
		// TODO: better error handling
		p.Resolve([]byte("transport stopped"))
	}
	h.registerObserverPromises = nil

	for _, p := range h.unsubscribeObserverPromises {
		// TODO: better error handling
		p.Resolve([]byte("transport stopped"))
	}
	h.unsubscribeObserverPromises = nil

	h.stopped = true

	return errTransportStop
}

func (h *managedTransportInternal) registerRegisterObserverPromise(uuid string) RegisterObserverFuture {
	f := observerChanFuture{
		c: make(chan struct {
			errData []byte
		}, 1),
	}
	p := observerFuncPromise{
		f: func(errData []byte) {
			f.c <- struct{ errData []byte }{errData: errData}
		},
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.stopped {
		p.Resolve([]byte("transport stopped"))
	} else {
		h.registerObserverPromises[uuid] = p
	}
	return f
}

func (h *managedTransportInternal) registerUnsubscribeObserverPromise(uuid string) RegisterObserverFuture {
	f := observerChanFuture{
		c: make(chan struct {
			errData []byte
		}, 1),
	}
	p := observerFuncPromise{
		f: func(errData []byte) {
			f.c <- struct{ errData []byte }{errData: errData}
		},
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.stopped {
		p.Resolve([]byte("transport stopped"))

	} else {
		h.unsubscribeObserverPromises[uuid] = p
	}
	return f
}

func (h *managedTransportInternal) registerInvokeMethodPromise(uuid string) InvokeMethodFuture {
	f := invokeMethodChanFuture{
		c: make(chan struct {
			payload []byte
			errData []byte
		}, 1),
	}
	p := invokeMethodFuncPromise{
		f: func(payload []byte, errData []byte) {
			f.c <- struct {
				payload []byte
				errData []byte
			}{payload: payload, errData: errData}
		},
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.stopped {
		p.Resolve(nil, []byte("transport stopped"))
	} else {
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
	p.Resolve(errData)
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
	p.Resolve(payload, errData)

	delete(h.invokeMethodPromises, uuid)
}

func (h *managedTransportInternal) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte, deadline time.Time) {
	p := invokeMethodFuncPromise{
		deadline: deadline,
		f: func(payload []byte, err []byte) {
			if err := h.transport.EnqueueInvokeMethodResponse(context.TODO(), sender, uuid, payload, err); err != nil {
				h.log.V(0).Error(err, "failed to response to invoke method request",
					"sender", sender,
					"receiver", receiver,
					"method", method, "uuid", uuid)
			}
		}}
	h.TransportHandler.ReceiveInvokeMethodRequest(ctx, sender, receiver, method, payload, p)
}

func (h *managedTransportInternal) ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte, opts cluster.EnqueueRegisterObserverRequestOptions, deadline time.Time) {
	p := observerFuncPromise{
		deadline: deadline,
		f: func(errOut []byte) {
			if err := h.transport.EnqueueAckRegisterObserver(context.TODO(), observer, uuid, errOut); err != nil {
				h.log.V(0).Error(err, "failed to response to ack observer registration",
					"observer", observer,
					"observable", observable,
					"name", name,
					"uuid", uuid)
			}
		}}
	h.TransportHandler.ReceiveRegisterObserverRequest(ctx, observer, observable, name, payload, opts.RegistrationTimeout, p)
}

func (h *managedTransportInternal) ReceiveUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, deadline time.Time) {
	p := observerFuncPromise{
		deadline: deadline,
		f: func(errOut []byte) {
			if err := h.transport.EnqueueAckUnsubscribeObserver(context.TODO(), observer, uuid, errOut); err != nil {
				h.log.V(0).Error(err, "failed to response to ack unsubscribe observer",
					"observer", observer,
					"observable", observable,
					"name", name,
					"uuid", uuid)
			}
		}}
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
	p.Resolve(errData)
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

	mt := &managedTransport{
		internal: &managedTransportInternal{
			log:                         m.log.WithName(string(nodeName)),
			transport:                   transport,
			TransportHandler:            m.handler,
			invokeMethodPromises:        make(map[string]InvokeMethodPromise),
			registerObserverPromises:    make(map[string]RegisterObserverPromise),
			unsubscribeObserverPromises: make(map[string]UnsubscribeObserverPromise),
		},
	}
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

	f := t.internal.registerInvokeMethodPromise(uuid)
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

	f := t.internal.registerRegisterObserverPromise(uuid)
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

	f := t.internal.registerUnsubscribeObserverPromise(uuid)
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
