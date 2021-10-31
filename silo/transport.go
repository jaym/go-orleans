package silo

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type siloTransportHandler struct {
	log               logr.Logger
	codec             codec.Codec
	localGrainManager *GrainActivationManagerImpl
}

func (s siloTransportHandler) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, payload []byte, promise transport.InvokeMethodPromise) {
	err := s.localGrainManager.EnqueueInvokeMethodRequest(InvokeMethodRequest{
		Sender:   sender,
		Receiver: receiver,
		Method:   method,
		in:       payload,
		ResolveFunc: func(i interface{}, e error) {
			if e != nil {
				promise.Resolve(nil, encodeError(ctx, e))
			} else {
				data, err := s.codec.Encode(i)
				promise.Resolve(data, encodeError(ctx, err))
			}
		},
	})
	if err != nil {
		promise.Resolve(nil, encodeError(ctx, err))
		s.log.Error(err, "failed to enqueue invoke method", "sender", sender, "receiver", receiver, "method", method)
	}
}

func (s siloTransportHandler) ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, payload []byte, promise transport.RegisterObserverPromise) {
	err := s.localGrainManager.EnqueueRegisterObserverRequest(RegisterObserverRequest{
		Observer:   observer,
		Observable: observable,
		Name:       name,
		In:         payload,
		ResolveFunc: func(e error) {
			if e != nil {
				promise.Resolve(encodeError(ctx, e))
			} else {
				promise.Resolve(nil)
			}
		},
	})
	if err != nil {
		promise.Resolve(encodeError(ctx, err))
		s.log.Error(err, "failed to enqueue register observer", "observer", observer, "observable", observable, "name", name)
	}
}

func encodeError(ctx context.Context, err error) []byte {
	if err == nil {
		return nil
	}
	encodedErr := errors.EncodeError(ctx, err)
	if data, err := gogoproto.Marshal(&encodedErr); err != nil {
		panic(err)
	} else {
		return data
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

type localTransport struct {
	log               logr.Logger
	codec             codec.Codec
	localGrainManager *GrainActivationManagerImpl
	h                 cluster.TransportHandler
}

func (t *localTransport) Listen(handler cluster.TransportHandler) error {
	t.h = handler
	return nil
}

func (*localTransport) Stop() error { return nil }

func (t *localTransport) EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error {
	t.h.ReceiveInvokeMethodRequest(ctx, sender, receiver, method, uuid, payload)
	return nil
}

func (t *localTransport) EnqueueRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte) error {
	t.h.ReceiveRegisterObserverRequest(ctx, observer, observable, name, uuid, payload)
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
