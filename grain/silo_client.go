package grain

import (
	"context"

	"google.golang.org/protobuf/proto"
)

type InvokeMethodResp interface {
	Get(out interface{}) error
}

type InvokeMethodFuture interface {
	Await(ctx context.Context) (InvokeMethodResp, error)
	ResolveValue(interface{}) error
	ResolveError(error) error
}

type RegisterObserverFuture interface {
	Await(ctx context.Context) error
	Resolve() error
	ResolveError(error) error
}

type SiloClient interface {
	InvokeMethod(ctx context.Context, toAddress Address, grainType string, method string,
		in proto.Message) InvokeMethodFuture

	RegisterObserver(ctx context.Context, observer Address, observable Address, name string, in proto.Message) RegisterObserverFuture
	AckRegisterObserver(ctx context.Context, receiver Address, uuid string, errOut error) error
	SendResponse(ctx context.Context, receiver Address, uuid string, out proto.Message) error
	NotifyObservers(ctx context.Context, observableType string, observableName string, receiver []Address, out proto.Message) error
}
