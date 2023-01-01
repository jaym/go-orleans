package cluster

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
)

type TransportFactory interface {
	CreateTransport(Node) (Transport, error)
}

type TransportHandler interface {
	ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte, deadline time.Time)
	ReceiveInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte)
	ReceiveInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, name string, payload []byte)
}

type StopFunc func() error

type EnqueueRegisterObserverRequestOptions struct {
	RegistrationTimeout time.Duration
}

type Transport interface {
	Listen(TransportHandler) error
	Stop() error

	EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error
	EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error
	EnqueueInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, methodName string, payload []byte) error
}
