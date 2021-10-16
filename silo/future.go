package silo

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"google.golang.org/protobuf/proto"
)

type InvokeMethodResp struct {
	data []byte
	err  error
}

func (resp InvokeMethodResp) Get(out interface{}) error {
	if resp.err != nil {
		return resp.err
	}
	return proto.Unmarshal(resp.data, out.(proto.Message))
}

type invokeMethodFuture struct {
	id       string
	respChan chan InvokeMethodResp
	timeout  time.Duration
	codec    codec.Codec
}

func newInvokeMethodFuture(codec codec.Codec, id string, timeout time.Duration) *invokeMethodFuture {
	if timeout == 0 {
		panic("timeout must be specified")
	}
	return &invokeMethodFuture{
		id:       id,
		respChan: make(chan InvokeMethodResp, 1),
		timeout:  timeout,
		codec:    codec,
	}
}

func (f *invokeMethodFuture) ResolveValue(out interface{}) error {
	data, err := f.codec.Encode(out)
	if err != nil {
		return err
	}
	resp := InvokeMethodResp{
		data: data,
	}

	f.resolve(resp)
	return nil
}

func (f *invokeMethodFuture) ResolveError(err error) error {
	resp := InvokeMethodResp{
		err: err,
	}

	f.resolve(resp)
	return nil
}

func (f *invokeMethodFuture) resolve(resp InvokeMethodResp) {
	select {
	case f.respChan <- resp:
	default:
		// TODO: error handling
		panic("multiple resolve")
	}
}

func (f *invokeMethodFuture) Await(ctx context.Context) (grain.InvokeMethodResp, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-f.respChan:
		return r, nil
	}
}

type RegisterObserverResp struct {
	Err error
}

type registerObserverFuture struct {
	id       string
	respChan chan RegisterObserverResp
	timeout  time.Duration
}

func newRegisterObserverFuture(id string, timeout time.Duration) *registerObserverFuture {
	if timeout == 0 {
		panic("timeout must be specified")
	}
	return &registerObserverFuture{
		id:       id,
		respChan: make(chan RegisterObserverResp, 1),
		timeout:  timeout,
	}
}

func (f *registerObserverFuture) Resolve() error {
	f.resolve(RegisterObserverResp{})
	return nil
}

func (f *registerObserverFuture) ResolveError(err error) error {
	f.resolve(RegisterObserverResp{Err: err})
	return nil
}

func (f *registerObserverFuture) resolve(resp RegisterObserverResp) {
	select {
	case f.respChan <- resp:
	default:
		// TODO: error handling
		panic("multiple resolve")
	}
}

func (f *registerObserverFuture) Await(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-f.respChan:
		return r.Err
	}
}
