package silo

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/silo/internal/transport"
)

type InvokeMethodResp struct {
	codec codec.Codec
	data  []byte
}

func (resp InvokeMethodResp) Get(out interface{}) error {
	return resp.codec.Decode(resp.data, out)
}

type invokeMethodFuture struct {
	codec codec.Codec
	f     transport.InvokeMethodFuture
}

func newInvokeMethodFuture(codec codec.Codec, f transport.InvokeMethodFuture) *invokeMethodFuture {
	return &invokeMethodFuture{
		codec: codec,
		f:     f,
	}
}

func (f *invokeMethodFuture) Await(ctx context.Context) (grain.InvokeMethodResp, error) {
	res, err := f.f.Await(ctx)
	if err != nil {
		return nil, err
	}

	return &InvokeMethodResp{codec: f.codec, data: res.Response}, nil
}

type invokeMethodFailedFuture struct {
	err error
}

func (f invokeMethodFailedFuture) Await(ctx context.Context) (grain.InvokeMethodResp, error) {
	return nil, f.err
}

type RegisterObserverResp struct {
	Err error
}

type registerObserverFuture struct {
	codec codec.Codec
	f     transport.RegisterObserverFuture
}

func newRegisterObserverFuture(codec codec.Codec, f transport.RegisterObserverFuture) *registerObserverFuture {
	return &registerObserverFuture{
		codec: codec,
		f:     f,
	}
}

func (f *registerObserverFuture) Await(ctx context.Context) error {
	errData, err := f.f.Await(ctx)
	if err != nil {
		return err
	}
	if len(errData) > 0 {
		var encodedError errors.EncodedError
		if err := proto.Unmarshal(errData, &encodedError); err != nil {
			return err
		}
		return errors.DecodeError(ctx, encodedError)
	}
	return nil
}

type registerObserverFailedFuture struct {
	err error
}

func (f registerObserverFailedFuture) Await(ctx context.Context) error {
	return f.err
}
