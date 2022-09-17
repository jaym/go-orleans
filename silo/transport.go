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

func (s siloTransportHandler) ReceiveInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, grainType string, methodName string, payload []byte) {
	dec, err := s.codecV2.Unpack(payload)
	if err != nil {
		s.log.Error(err, "failed to enqueue one way method request", "sender", sender, "receivers", receivers, "name", methodName)
		return
	}
	err = s.localGrainManager.EnqueueInvokeOneWayMethodRequest(sender, receivers, methodName, dec)
	if err != nil {
		s.log.Error(err, "failed to enqueue one way method request", "sender", sender, "receivers", receivers, "name", methodName)
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

func (t *localTransport) EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error {
	t.h.ReceiveInvokeMethodResponse(ctx, receiver, uuid, payload, err)
	return nil
}
