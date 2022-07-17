package silo

import (
	"context"

	"github.com/jaym/go-orleans/future"
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

type unitFuture[T any] struct {
	f future.Future[T]
}

func newUnitFuture[T any](codec codec.Codec, f future.Future[T]) *unitFuture[T] {
	return &unitFuture[T]{
		f: f,
	}
}

func (f *unitFuture[T]) Await(ctx context.Context) error {
	_, err := f.f.Await(ctx)
	if err != nil {
		return err
	}

	return nil
}

type registerObserverFailedFuture struct {
	err error
}

func (f registerObserverFailedFuture) Await(ctx context.Context) error {
	return f.err
}
