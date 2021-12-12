package grain

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
)

type InvokeMethodResp interface {
	Get(out interface{}) error
}

type InvokeMethodFuture interface {
	Await(ctx context.Context) (InvokeMethodResp, error)
}

type RegisterObserverFuture interface {
	Await(ctx context.Context) error
}

type UnsubscribeObserverFuture interface {
	Await(ctx context.Context) error
}

type RegisterObserverOptions struct {
	// RegistrationTimeout is how long the observer registration will
	// live for. The caller is responsible for refreshing it before this
	// timeout expires
	RegistrationTimeout time.Duration
}

type RegisterObserverOption func(*RegisterObserverOptions)

func WithRegisterObserverRegistrationTimeout(d time.Duration) RegisterObserverOption {
	return func(o *RegisterObserverOptions) {
		o.RegistrationTimeout = d
	}
}

type SiloClient interface {
	InvokeMethod(ctx context.Context, toIdentity Identity, grainType string, method string,
		in proto.Message) InvokeMethodFuture
	RegisterObserver(ctx context.Context, observer Identity, observable Identity, name string, in proto.Message, opts ...RegisterObserverOption) RegisterObserverFuture
	UnsubscribeObserver(ctx context.Context, observer Identity, observable Identity, name string) UnsubscribeObserverFuture
	NotifyObservers(ctx context.Context, observableType string, observableName string, receiver []Identity, out proto.Message) error
}
