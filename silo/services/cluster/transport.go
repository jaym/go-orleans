package cluster

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

type TransportFactory interface {
	CreateTransport(Node) (Transport, error)
}

type TransportHandler interface {
	ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte)
	ReceiveInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte)

	ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte)
	ReceiveAckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte)
	ReceiveObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte)

	ReceiveUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string)
	ReceiveAckUnsubscribeObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte)
}

type StopFunc func() error

type Transport interface {
	Listen(TransportHandler) error
	Stop() error

	EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error
	EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error

	EnqueueRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte) error
	EnqueueAckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte) error
	EnqueueObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte) error

	EnqueueUnsubscribeObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string) error
	EnqueueAckUnsubscribeObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte) error
}
