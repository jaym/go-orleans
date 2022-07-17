package future

import (
	"context"
	"time"
)

type Promise[T any] interface {
	Deadline() time.Time
	Reject(error)
	Resolve(T)
}

type Future[T any] interface {
	Await(ctx context.Context) (T, error)
}

func Map[T any, U any](f Future[T], mapper func(T) (U, error)) Future[U] {
	return futureMapper[T, U]{
		f:      f,
		mapper: mapper,
	}
}

func NewFuture[T any](deadline time.Time) (Future[T], Promise[T]) {
	f := futureImpl[T]{
		make(chan struct {
			err error
			val T
		}, 1),
	}
	p := funcPromise[T]{
		deadline: deadline,
		f: func(val T, err error) {
			select {
			case f.c <- struct {
				err error
				val T
			}{
				err: err,
				val: val,
			}:
			default:
			}
		},
	}
	return f, p
}

func NewFuncPromise[T any](deadline time.Time, f func(T, error)) Promise[T] {
	return funcPromise[T]{
		deadline: deadline,
		f:        f,
	}
}

type futureImpl[T any] struct {
	c chan struct {
		err error
		val T
	}
}

func (f futureImpl[T]) Await(ctx context.Context) (T, error) {
	var defaultVal T

	select {
	case <-ctx.Done():
		return defaultVal, ctx.Err()
	case msg := <-f.c:
		if msg.err != nil {
			return defaultVal, msg.err
		}
		return msg.val, nil
	}
}

type futureMapper[T any, U any] struct {
	f      Future[T]
	mapper func(T) (U, error)
}

func (f futureMapper[T, U]) Await(ctx context.Context) (U, error) {
	var defaultVal U

	v, err := f.f.Await(ctx)
	if err != nil {
		return defaultVal, err
	}

	u, err := f.mapper(v)
	if err != nil {
		return defaultVal, err
	}
	return u, nil
}

type funcPromise[T any] struct {
	deadline time.Time
	f        func(val T, err error)
}

func (p funcPromise[T]) Reject(err error) {
	var defaultVal T
	p.f(defaultVal, err)
}

func (p funcPromise[T]) Resolve(val T) {
	p.f(val, nil)
}

func (p funcPromise[T]) Deadline() time.Time {
	return p.deadline
}
