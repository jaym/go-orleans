package grain

import (
	"context"
)

type InvokeMethodRespV2 interface {
	Get(func(Deserializer) error) error
}

type InvokeMethodFutureV2 interface {
	Await(ctx context.Context) (InvokeMethodRespV2, error)
}

type SiloClient interface {
	InvokeOneWayMethod(ctx context.Context, toIdentity []Identity, method string,
		ser func(Serializer) error)
	InvokeMethodV2(ctx context.Context, toIdentity Identity, method string,
		ser func(Serializer) error) InvokeMethodFutureV2
}

type Deserializer interface {
	CloneAndReset() Deserializer
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
	InvokeMethod(ctx context.Context, method string, sender Identity,
		d Deserializer, respSerializer Serializer) error
}
