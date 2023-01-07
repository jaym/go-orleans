package activation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGrainTimer(t *testing.T) {
	ctx := context.Background()
	t.Run("starts off triggered", func(t *testing.T) {
		g := newGrainTimer()
		<-g.t.C
		g.triggerDue(ctx)
	})

	t.Run("registering timer", func(t *testing.T) {
		t.Run("ticks only once", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			wg := sync.WaitGroup{}
			wg.Add(1)
			g.RegisterTimer("foo", time.Duration(1), func(ctx context.Context) {
				wg.Done()
			})
			<-g.t.C
			g.triggerDue(ctx)
			wg.Wait()
			require.Len(t, g.h, 0)
			require.Len(t, g.timers, 0)
		})

		t.Run("overwrites existing", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			wg := sync.WaitGroup{}
			wg.Add(1)
			g.RegisterTimer("foo", time.Duration(time.Hour), func(ctx context.Context) {
				require.Fail(t, "wrong function called")
			})
			g.RegisterTimer("foo", time.Duration(1), func(ctx context.Context) {
				wg.Done()
			})
			<-g.t.C
			g.triggerDue(ctx)
			wg.Wait()
			require.Len(t, g.timers, 0)
		})

		t.Run("reinitializes timer", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			wg := sync.WaitGroup{}
			wg.Add(1)
			g.RegisterTimer("foo", time.Duration(time.Hour), func(ctx context.Context) {
				require.Fail(t, "wrong function called")
			})
			g.RegisterTimer("bar", time.Duration(1), func(ctx context.Context) {
				wg.Done()
			})
			<-g.t.C
			g.triggerDue(ctx)
			wg.Wait()
			require.Len(t, g.h, 1)
			require.Len(t, g.timers, 1)
		})

		t.Run("can cancel", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			g.RegisterTimer("foo", time.Duration(1), func(ctx context.Context) {
				require.Fail(t, "wrong function called")
			})
			<-g.t.C
			canceled := g.Cancel("foo")
			require.True(t, canceled)
			g.triggerDue(ctx)
			require.Len(t, g.h, 0)
			require.Len(t, g.timers, 0)
		})
	})

	t.Run("registering ticker", func(t *testing.T) {
		t.Run("ticks continuously", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			wg := sync.WaitGroup{}
			wg.Add(2)
			g.RegisterTicker("foo", time.Duration(1), func(ctx context.Context) {
				wg.Done()
			})
			<-g.t.C
			g.triggerDue(ctx)
			<-g.t.C
			g.triggerDue(ctx)
			wg.Wait()
			require.Len(t, g.h, 1)
			require.Len(t, g.timers, 1)
		})

		t.Run("overwrites existing", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			wg := sync.WaitGroup{}
			wg.Add(1)
			g.RegisterTicker("foo", time.Duration(time.Hour), func(ctx context.Context) {
				require.Fail(t, "wrong function called")
			})
			g.RegisterTicker("foo", time.Duration(1), func(ctx context.Context) {
				wg.Done()
			})
			<-g.t.C
			g.triggerDue(ctx)
			wg.Wait()
			require.Len(t, g.h, 2)
			require.Len(t, g.timers, 1)
		})

		t.Run("can cancel", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			g.RegisterTicker("foo", time.Duration(1), func(ctx context.Context) {
				require.Fail(t, "wrong function called")
			})
			<-g.t.C
			canceled := g.Cancel("foo")
			require.True(t, canceled)
			g.triggerDue(ctx)
			require.Len(t, g.h, 0)
			require.Len(t, g.timers, 0)
		})

		t.Run("reinitializes timer", func(t *testing.T) {
			g := newGrainTimer()
			defer g.destroy()
			<-g.t.C

			wg := sync.WaitGroup{}
			wg.Add(1)
			g.RegisterTicker("foo", time.Duration(time.Hour), func(ctx context.Context) {
				require.Fail(t, "wrong function called")
			})
			g.RegisterTicker("bar", time.Duration(1), func(ctx context.Context) {
				wg.Done()
			})
			<-g.t.C
			g.triggerDue(ctx)
			wg.Wait()
			require.Len(t, g.h, 2)
			require.Len(t, g.timers, 2)
		})
	})
}
