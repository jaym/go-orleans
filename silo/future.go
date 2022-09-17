package silo

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/silo/internal/transport"
)

type invokeMethodFutureV2 struct {
	codec codec.CodecV2
	f     transport.InvokeMethodFuture
}

func newInvokeMethodFutureV2(codec codec.CodecV2, f transport.InvokeMethodFuture) *invokeMethodFutureV2 {
	return &invokeMethodFutureV2{
		codec: codec,
		f:     f,
	}
}

func (f *invokeMethodFutureV2) Await(ctx context.Context) (grain.InvokeMethodRespV2, error) {
	res, err := f.f.Await(ctx)
	if err != nil {
		return nil, err
	}

	return &InvokeMethodRespV2{codec: f.codec, data: res.Response}, nil
}

type invokeMethodFailedFutureV2 struct {
	err error
}

func (f invokeMethodFailedFutureV2) Await(ctx context.Context) (grain.InvokeMethodRespV2, error) {
	return nil, f.err
}

type InvokeMethodRespV2 struct {
	codec codec.CodecV2
	data  []byte
}

func (resp InvokeMethodRespV2) Get(f func(d grain.Deserializer) error) error {
	dec, err := resp.codec.Unpack(resp.data)
	if err != nil {
		return err
	}
	return f(dec)
}
