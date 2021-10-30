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
}

type RegisterObserverFuture interface {
	Await(ctx context.Context) error
}

type SiloClient interface {
	InvokeMethod(ctx context.Context, toIdentity Identity, grainType string, method string,
		in proto.Message) InvokeMethodFuture
	RegisterObserver(ctx context.Context, observer Identity, observable Identity, name string, in proto.Message) RegisterObserverFuture
	NotifyObservers(ctx context.Context, observableType string, observableName string, receiver []Identity, out proto.Message) error
}
