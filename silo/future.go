package silo

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
)

type InvokeMethodFuture interface {
	Await(ctx context.Context) (*InvokeMethodResp, error)
	Resolve(resp InvokeMethodResp) error
}

type InvokeMethodResp struct {
	data []byte
	err  error
}

func (resp *InvokeMethodResp) Get(out interface{}) error {
	if resp.err != nil {
		return resp.err
	}
	return proto.Unmarshal(resp.data, out.(proto.Message))
}

type invokeMethodFuture struct {
	id       string
	respChan chan InvokeMethodResp
	timeout  time.Duration
}

func newInvokeMethodFuture(id string, timeout time.Duration) *invokeMethodFuture {
	if timeout == 0 {
		panic("timeout must be specified")
	}
	return &invokeMethodFuture{
		id:       id,
		respChan: make(chan InvokeMethodResp, 1),
		timeout:  timeout,
	}
}

func (f *invokeMethodFuture) Resolve(resp InvokeMethodResp) error {
	select {
	case f.respChan <- resp:
		return nil
	default:
		// TODO: error handling
		panic("multiple resolve")
	}
}

func (f *invokeMethodFuture) Await(ctx context.Context) (*InvokeMethodResp, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-f.respChan:
		return &r, nil
	}
}

type RegisterObserverFuture interface {
	Await(ctx context.Context) (*RegisterObserverResp, error)
	Resolve(resp RegisterObserverResp) error
}

type RegisterObserverResp struct {
	Data []byte
	Err  error
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

func (f *registerObserverFuture) Resolve(resp RegisterObserverResp) error {
	select {
	case f.respChan <- resp:
		return nil
	default:
		// TODO: error handling
		panic("multiple resolve")
	}
}

func (f *registerObserverFuture) Await(ctx context.Context) (*RegisterObserverResp, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-f.respChan:
		return &r, nil
	}
}
