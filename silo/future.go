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
	payload, errData, err := f.f.Await(ctx)
	if err != nil {
		return nil, err
	}
	if len(errData) > 0 {
		var encodedError errors.EncodedError
		if err := proto.Unmarshal(errData, &encodedError); err != nil {
			return nil, err
		}
		return nil, errors.DecodeError(ctx, encodedError)
	}
	return &InvokeMethodResp{codec: f.codec, data: payload}, nil
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
