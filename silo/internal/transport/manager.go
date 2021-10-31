package transport

import (
	"context"
	"fmt"
	"sync"

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
}

func NewManager(log logr.Logger, handler TransportHandler) *Manager {
	return &Manager{
		log:        log,
		handler:    handler,
		transports: make(map[cluster.Location]*managedTransport),
	}
}

type TransportHandler interface {
	ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, payload []byte, promise InvokeMethodPromise)
	ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, payload []byte, promise RegisterObserverPromise)
	ReceiveObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte)
}

type InvokeMethodPromise interface {
	Resolve(payload []byte, errData []byte)
}

type invokeMethodFuncPromise struct {
	f func(payload []byte, err []byte)
}

func (p invokeMethodFuncPromise) Resolve(payload []byte, errData []byte) {
	if len(errData) > 0 {
		p.f(nil, errData)
	} else {
		p.f(payload, nil)
	}
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
}

type RegisterObserverFuture interface {
	Await(ctx context.Context) (errData []byte, err error)
}

type registerObserverFuncPromise struct {
	f func(err []byte)
}

func (p registerObserverFuncPromise) Resolve(errData []byte) {
	if len(errData) > 0 {
		p.f(errData)
	} else {
		p.f(nil)
	}
}

type registerObserverChanFuture struct {
	c chan struct {
		errData []byte
	}
}

func (f registerObserverChanFuture) Await(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v := <-f.c:
		return v.errData, nil
	}
}

type managedTransport struct {
	internal *managedTransportInternal
}

type managedTransportInternal struct {
	TransportHandler
	log                      *logr.Logger
	transport                cluster.Transport
	lock                     sync.Mutex
	invokeMethodPromises     map[string]InvokeMethodPromise
	registerObserverPromises map[string]RegisterObserverPromise
}

func (h *managedTransportInternal) registerRegisterObserverPromise(uuid string) RegisterObserverFuture {
	f := registerObserverChanFuture{
		c: make(chan struct {
			errData []byte
		}, 1),
	}
	p := registerObserverFuncPromise{
		f: func(errData []byte) {
			f.c <- struct{ errData []byte }{errData: errData}
		},
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	h.registerObserverPromises[uuid] = p
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
	h.invokeMethodPromises[uuid] = p
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

func (h *managedTransportInternal) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) {
	p := invokeMethodFuncPromise{f: func(payload []byte, err []byte) {
		if err := h.transport.EnqueueInvokeMethodResponse(context.TODO(), sender, uuid, payload, err); err != nil {
			h.log.V(0).Error(err, "failed to response to invoke method request",
				"sender", sender,
				"receiver", receiver,
				"method", method, "uuid", uuid)
		}
	}}
	h.TransportHandler.ReceiveInvokeMethodRequest(ctx, sender, receiver, method, payload, p)
}

func (h *managedTransportInternal) ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte) {
	p := registerObserverFuncPromise{f: func(errOut []byte) {
		if err := h.transport.EnqueueAckRegisterObserver(context.TODO(), observer, uuid, errOut); err != nil {
			h.log.V(0).Error(err, "failed to response to ack observer registration",
				"observer", observer,
				"observable", observable,
				"name", name,
				"uuid", uuid)
		}
	}}
	h.TransportHandler.ReceiveRegisterObserverRequest(ctx, observer, observable, name, payload, p)
}

func (m *Manager) AddTransport(nodeName cluster.Location, transport cluster.Transport) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.transports[nodeName]; ok {
		return errors.New("transport already exists")
	}

	mt := &managedTransport{
		internal: &managedTransportInternal{
			transport:                transport,
			TransportHandler:         m.handler,
			invokeMethodPromises:     make(map[string]InvokeMethodPromise),
			registerObserverPromises: make(map[string]RegisterObserverPromise),
		},
	}
	m.transports[nodeName] = mt
	return transport.Listen(mt.internal)
}

func (m *Manager) RemoveTransport(nodeName cluster.Location) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.transports, nodeName)
}

func (m *Manager) InvokeMethod(ctx context.Context, sender grain.Identity, receiver cluster.GrainAddress, method string, uuid string, payload []byte) (InvokeMethodFuture, error) {
	t, err := m.getTransport(receiver.Location)
	if err != nil {
		return nil, err
	}

	if err := t.internal.transport.EnqueueInvokeMethodRequest(ctx, sender, receiver.Identity, method, uuid, payload); err != nil {
		return nil, err
	}
	return t.internal.registerInvokeMethodPromise(uuid), nil
}

func (m *Manager) RegisterObserver(ctx context.Context, observer grain.Identity, observable cluster.GrainAddress, name string, uuid string, payload []byte) (RegisterObserverFuture, error) {
	t, err := m.getTransport(observable.Location)
	if err != nil {
		return nil, err
	}
	if err := t.internal.transport.EnqueueRegisterObserverRequest(ctx, observer, observable.Identity, name, uuid, payload); err != nil {
		return nil, err
	}
	return t.internal.registerRegisterObserverPromise(uuid), nil
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
		}
		if err := t.internal.transport.EnqueueObserverNotification(ctx, sender, rs, observableType, name, payload); err != nil {
			combinedErrors = errors.CombineErrors(combinedErrors, err)
		}
	}
	return combinedErrors
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
