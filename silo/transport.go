package silo

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type siloTransportHandler struct {
	log               logr.Logger
	codec             codec.Codec
	codecV2           codec.CodecV2
	localGrainManager *GrainActivationManagerImpl
}

func (s siloTransportHandler) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, payload []byte, promise transport.InvokeMethodPromise) {
	dec, err := s.codecV2.Unpack(payload)
	if err != nil {
		promise.Reject(err)
		s.log.Error(err, "failed to enqueue invoke method", "sender", sender, "receiver", receiver, "method", method)
		return
	}
	ser := s.codecV2.Pack()
	err = s.localGrainManager.EnqueueInvokeMethodRequest(sender, receiver, method, promise.Deadline(), dec, ser,
		func(e error) {
			if e != nil {
				promise.Reject(e)
			} else {
				data, err := ser.ToBytes()
				if err != nil {
					promise.Reject(err)
				} else {
					promise.Resolve(transport.InvokeMethodResponse{
						Response: data,
					})
				}
			}
		})
	if err != nil {
		promise.Reject(err)
		s.log.Error(err, "failed to enqueue invoke method", "sender", sender, "receiver", receiver, "method", method)
	}
}

func (s siloTransportHandler) ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, payload []byte, registrationTimeout time.Duration, promise transport.RegisterObserverPromise) {
	err := s.localGrainManager.EnqueueRegisterObserverRequest(RegisterObserverRequest{
		Observer:            observer,
		Observable:          observable,
		Name:                name,
		In:                  payload,
		RegistrationTimeout: registrationTimeout,
		ResolveFunc: func(e error) {
			if e != nil {
				promise.Reject(e)
			} else {
				promise.Resolve(transport.RegisterObserverResponse{})
			}
		},
	})
	if err != nil {
		promise.Reject(err)
		s.log.Error(err, "failed to enqueue register observer", "observer", observer, "observable", observable, "name", name)
	}
}

func (s siloTransportHandler) ReceiveInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, grainType string, methodName string, payload []byte) {
	dec, err := s.codecV2.Unpack(payload)
	if err != nil {
		s.log.Error(err, "failed to enqueue one way method request", "sender", sender, "receivers", receivers, "name", methodName)
		return
	}
	err = s.localGrainManager.EnqueueInvokeOneWayMethodRequest(sender, receivers, methodName, dec)
	if err != nil {
		s.log.Error(err, "failed to enqueue observer notification", "sender", sender, "receivers", receivers, "name", methodName)
	}
}
func (s siloTransportHandler) ReceiveObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte) {
	err := s.localGrainManager.EnqueueObserverNotification(ObserverNotification{
		Sender:         sender,
		Receivers:      receivers,
		ObservableType: observableType,
		Name:           name,
		In:             payload,
	})
	if err != nil {
		s.log.Error(err, "failed to enqueue observer notification", "sender", sender, "receivers", receivers, "name", name)
	}
}

func (s siloTransportHandler) ReceiveUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, promise transport.UnsubscribeObserverPromise) {
	err := s.localGrainManager.EnqueueUnsubscribeObserverRequest(UnsubscribeObserverRequest{
		Observer:   observer,
		Observable: observable,
		Name:       name,
		ResolveFunc: func(e error) {
			if e != nil {
				promise.Reject(e)
			} else {
				promise.Resolve(transport.UnsubscribeObserverResponse{})
			}
		},
	})
	if err != nil {
		promise.Reject(err)
		s.log.Error(err, "failed to enqueue unregister observer", "observer", observer, "observable", observable, "name", name)
	}
}

type localTransport struct {
	log               logr.Logger
	localGrainManager *GrainActivationManagerImpl
	h                 cluster.TransportHandler
}

func (t *localTransport) Listen(handler cluster.TransportHandler) error {
	t.h = handler
	return nil
}

func (*localTransport) Stop() error { return nil }

func deadline(ctx context.Context) time.Time {
	t, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}
	return t
}

func (t *localTransport) EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error {
	t.h.ReceiveInvokeMethodRequest(ctx, sender, receiver, method, uuid, payload, deadline(ctx))
	return nil
}

func (t *localTransport) EnqueueInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, grainType string, name string, payload []byte) error {
	t.h.ReceiveInvokeOneWayMethodRequest(ctx, sender, receivers, grainType, name, payload)
	return nil
}

func (t *localTransport) EnqueueRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte, opts cluster.EnqueueRegisterObserverRequestOptions) error {
	t.h.ReceiveRegisterObserverRequest(ctx, observer, observable, name, uuid, payload, opts, deadline(ctx))
	return nil
}

func (t *localTransport) EnqueueObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte) error {
	t.h.ReceiveObserverNotification(ctx, sender, receivers, observableType, name, payload)
	return nil
}

func (t *localTransport) EnqueueAckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte) error {
	t.h.ReceiveAckRegisterObserver(ctx, receiver, uuid, errOut)
	return nil
}

func (t *localTransport) EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error {
	t.h.ReceiveInvokeMethodResponse(ctx, receiver, uuid, payload, err)
	return nil
}

func (t *localTransport) EnqueueUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string) error {
	t.h.ReceiveUnsubscribeObserverRequest(ctx, observer, observable, name, uuid, deadline(ctx))
	return nil
}

func (t *localTransport) EnqueueAckUnsubscribeObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte) error {
	t.h.ReceiveAckUnsubscribeObserver(ctx, receiver, uuid, errOut)
	return nil
}
