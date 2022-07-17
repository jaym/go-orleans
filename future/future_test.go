package future_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jaym/go-orleans/future"
	"github.com/stretchr/testify/require"
)

func TestFuture(t *testing.T) {
	t.Run("test resolve async", func(t *testing.T) {
		f, p := future.NewFuture[int](time.Time{})

		go func() {
			p.Resolve(17)
		}()

		v, err := f.Await(context.Background())
		require.NoError(t, err)
		require.Equal(t, 17, v)
	})

	t.Run("test resolve inline", func(t *testing.T) {
		f, p := future.NewFuture[int](time.Time{})

		p.Resolve(17)

		v, err := f.Await(context.Background())
		require.NoError(t, err)
		require.Equal(t, 17, v)
	})

	t.Run("test reject async", func(t *testing.T) {
		f, p := future.NewFuture[int](time.Time{})
		testErr := errors.New("test err")
		go func() {
			p.Reject(testErr)
		}()

		v, err := f.Await(context.Background())
		require.Equal(t, testErr, err)
		require.Equal(t, 0, v)
	})

	t.Run("test reject inline", func(t *testing.T) {
		f, p := future.NewFuture[int](time.Time{})
		testErr := errors.New("test err")
		p.Reject(testErr)

		v, err := f.Await(context.Background())
		require.Equal(t, testErr, err)
		require.Equal(t, 0, v)
	})

	t.Run("test context", func(t *testing.T) {
		f, _ := future.NewFuture[int](time.Time{})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := f.Await(ctx)
		require.Equal(t, context.Canceled, err)
	})
}

func TestMapper(t *testing.T) {
	t.Run("test resolve", func(t *testing.T) {
		f0, p := future.NewFuture[int](time.Time{})
		f := future.Map(f0, func(i int) (string, error) {
			return strconv.Itoa(i), nil
		})
		go func() {
			p.Resolve(17)
		}()

		v, err := f.Await(context.Background())
		require.NoError(t, err)
		require.Equal(t, "17", v)
	})

	t.Run("test resolve error", func(t *testing.T) {
		f0, p := future.NewFuture[int](time.Time{})
		testErr := errors.New("test err")
		f := future.Map(f0, func(i int) (string, error) {
			return "", testErr
		})
		go func() {
			p.Resolve(17)
		}()

		v, err := f.Await(context.Background())
		require.Equal(t, testErr, err)
		require.Equal(t, "", v)
	})

	t.Run("test reject", func(t *testing.T) {
		f0, p := future.NewFuture[int](time.Time{})
		testErr := errors.New("test err")
		wrongErr := errors.New("wrong err")
		f := future.Map(f0, func(i int) (string, error) {
			return "", wrongErr
		})
		go func() {
			p.Reject(testErr)
		}()

		v, err := f.Await(context.Background())
		require.Equal(t, testErr, err)
		require.Equal(t, "", v)
	})
}
