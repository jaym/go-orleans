package activation

import (
	"container/heap"
	"context"
	"time"

	"github.com/segmentio/ksuid"
)

type timerHeapEntry struct {
	deadline time.Time
	name     string
	instance string
}

type timerHeap []timerHeapEntry

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timerHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(timerHeapEntry))
}

func (h *timerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var nowProvider func() time.Time = time.Now

type timerInfo struct {
	name     string
	instance string
	isTicker bool
	duration time.Duration
	f        func(ctx context.Context)
}

type grainTimer struct {
	t      *time.Timer
	timers map[string]timerInfo
	h      timerHeap
}

func newGrainTimer() *grainTimer {
	return &grainTimer{
		t:      time.NewTimer(0),
		timers: map[string]timerInfo{},
		h:      make(timerHeap, 0, 1),
	}
}

func (g *grainTimer) triggerDue(ctx context.Context) {
	now := nowProvider()

	for len(g.h) > 0 {

		next := g.h[0]
		if next.deadline.After(now) {
			break
		}
		heap.Pop(&g.h)

		ti, ok := g.timers[next.name]
		isValid := ok && ti.instance == next.instance
		if !isValid {
			continue
		}

		ti.f(ctx)

		triggerEnd := nowProvider()

		if ti.isTicker {
			next.deadline = triggerEnd.Add(ti.duration)
			heap.Push(&g.h, next)
		} else {
			delete(g.timers, next.name)
		}
	}
	if len(g.h) > 0 {
		now := nowProvider()
		dur := g.h[0].deadline.Sub(now)
		if dur < 0 {
			dur = 0
		}
		g.t.Reset(dur)
	}
}

func (g *grainTimer) destroy() {
	g.t.Stop()
}

func (g *grainTimer) reinitTimer(deadline time.Time, d time.Duration) {
	if d == 0 {
		panic("duration provided must be greater than 0")
	}
	if len(g.h) == 0 || deadline.Before(g.h[0].deadline) {
		if !g.t.Stop() {
			select {
			case <-g.t.C:
			default:
			}
		}
		g.t.Reset(d)
	}
}

func (g *grainTimer) RegisterTimer(name string, d time.Duration, f func(ctx context.Context)) error {
	deadline := nowProvider().Add(d)
	g.reinitTimer(deadline, d)
	instance := ksuid.New().String()

	heap.Push(&g.h, timerHeapEntry{
		deadline: deadline,
		name:     name,
		instance: instance,
	})

	g.timers[name] = timerInfo{
		name:     name,
		instance: instance,
		isTicker: false,
		duration: d,
		f:        f,
	}

	return nil
}

func (g *grainTimer) RegisterTicker(name string, d time.Duration, f func(ctx context.Context)) error {
	deadline := nowProvider().Add(d)
	g.reinitTimer(deadline, d)

	instance := ksuid.New().String()

	heap.Push(&g.h, timerHeapEntry{
		deadline: deadline,
		name:     name,
		instance: instance,
	})

	g.timers[name] = timerInfo{
		name:     name,
		instance: instance,
		isTicker: true,
		duration: d,
		f:        f,
	}
	return nil
}

func (g *grainTimer) Cancel(name string) bool {
	if _, ok := g.timers[name]; ok {
		delete(g.timers, name)
		return true
	}

	return false
}
