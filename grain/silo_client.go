package grain

import (
	"context"

	"golang.org/x/exp/constraints"
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
	UInt64() (uint64, error)
	Bool() (bool, error)
	String() (string, error)
	Bytes() ([]byte, error)
	Interface(interface{}) error
	StringList() ([]string, error)
	BytesList() ([][]byte, error)
	BoolList() ([]bool, error)
	Int64List() ([]int64, error)
	UInt64List() ([]uint64, error)
}

type Serializer interface {
	Int64(int64)
	UInt64(uint64)
	Bool(bool)
	String(string)
	Bytes([]byte)
	Interface(interface{}) error
	StringList([]string)
	BytesList([][]byte)
	BoolList([]bool)
	Int64List([]int64)
	UInt64List([]uint64)
}

func SerializeIntList[T constraints.Signed](serializer Serializer, a []T) {
	c := make([]int64, len(a))
	for i, v := range a {
		c[i] = int64(v)
	}
	serializer.Int64List(c)
}

func SerializeUIntList[T constraints.Unsigned](serializer Serializer, a []T) {
	c := make([]uint64, len(a))
	for i, v := range a {
		c[i] = uint64(v)
	}
	serializer.UInt64List(c)
}

func DeserializeIntList[T constraints.Signed](deserializer Deserializer) ([]T, error) {
	l, err := deserializer.Int64List()
	if err != nil {
		return nil, err
	}
	c := make([]T, len(l))
	for i, v := range l {
		c[i] = T(v)
	}
	return c, nil
}

func DeserializeUIntList[T constraints.Unsigned](deserializer Deserializer) ([]T, error) {
	l, err := deserializer.UInt64List()
	if err != nil {
		return nil, err
	}
	c := make([]T, len(l))
	for i, v := range l {
		c[i] = T(v)
	}
	return c, nil
}

type Activation interface {
	InvokeMethod(ctx context.Context, method string, sender Identity,
		d Deserializer, respSerializer Serializer) error
}
