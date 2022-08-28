package grain

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
)

type InvokeMethodResp interface {
	Get(out interface{}) error
}

type InvokeMethodRespV2 interface {
	Get(func(Deserializer) error) error
}

type InvokeMethodFuture interface {
	Await(ctx context.Context) (InvokeMethodResp, error)
}

type InvokeMethodFutureV2 interface {
	Await(ctx context.Context) (InvokeMethodRespV2, error)
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

	InvokeOneWayMethod(ctx context.Context, toIdentity []Identity, grainType string, method string,
		ser func(Serializer) error)
	InvokeMethodV2(ctx context.Context, toIdentity Identity, grainType string, method string,
		ser func(Serializer) error) InvokeMethodFutureV2
}

type Deserializer interface {
	Int64() (int64, error)
	Bool() (bool, error)
	String() (string, error)
	Bytes() ([]byte, error)
	Interface(interface{}) error
}

type Serializer interface {
	Int64(int64)
	Bool(bool)
	String(string)
	Bytes([]byte)
	Interface(interface{}) error
}

type Activation interface {
	InvokeMethod(ctx context.Context, method string,
		d Deserializer, respSerializer Serializer) error
}
