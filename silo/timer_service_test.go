package silo

import (
	"container/heap"
	"context"
	stdlog "log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/stdr"
	"github.com/stretchr/testify/assert"
)

func TestTimerEntryHeap(t *testing.T) {
	queue := make(timerEntryHeap, 0, 512)
	heap.Init(&queue)

	now := time.Now()

	grainAdder := Address{
		Location:  "local",
		GrainType: "type1",
		ID:        "u1",
	}

	entries := []*timerEntry{
		{
			grainAddr: grainAdder,
			name:      "tmr1",
			triggerAt: now.Add(time.Second),
		},
		{
			grainAddr: grainAdder,
			name:      "tmr2",
			triggerAt: now.Add(-time.Second),
		},
		{
			grainAddr: grainAdder,
			name:      "tmr3",
			triggerAt: now.Add(-2 * time.Second),
		},
		{
			grainAddr: grainAdder,
			name:      "tmr4",
			triggerAt: now.Add(2 * time.Second),
		},
		{
			grainAddr: grainAdder,
			name:      "tmr5",
			triggerAt: now.Add(-3 * time.Second),
		},
	}

	for i := range entries {
		heap.Push(&queue, entries[i])
	}

	assert.Equal(t, "tmr5", heap.Pop(&queue).(*timerEntry).name)
	assert.Equal(t, "tmr3", heap.Pop(&queue).(*timerEntry).name)
	assert.Equal(t, "tmr2", heap.Pop(&queue).(*timerEntry).name)
	assert.Equal(t, "tmr1", heap.Pop(&queue).(*timerEntry).name)
	assert.Equal(t, "tmr4", heap.Pop(&queue).(*timerEntry).name)
}

type triggerable struct {
	l sync.Mutex

	wg       *sync.WaitGroup
	received []triggerExpectation
}

func (trig *triggerable) TriggerGrainTimer(grainAddr Address, name string) {
	trig.l.Lock()
	defer trig.l.Unlock()
	trig.received = append(trig.received, triggerExpectation{grainAddr: grainAddr, name: name})
	trig.wg.Done()
}

type triggerExpectation struct {
	grainAddr Address
	name      string
}

func (trig *triggerable) expect(t *testing.T, e []triggerExpectation) {
	t.Helper()

	trig.wg.Wait()

	trig.l.Lock()
	defer trig.l.Unlock()

	assert.ElementsMatch(t, e, trig.received)

	trig.received = nil
}

func TestTimerService(t *testing.T) {
	now := time.Now()
	nowProvider := func() time.Time {
		return now
	}
	trig := &triggerable{wg: &sync.WaitGroup{}}
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})
	s := newTimerServiceImpl(log, trig.TriggerGrainTimer).(*timerServiceImpl)
	s.nowProvider = nowProvider
	s.Start()

	g1Addr := Address{
		Location:  "local",
		GrainType: "type1",
		ID:        "id1",
	}
	g2Addr := Address{
		Location:  "local",
		GrainType: "type2",
		ID:        "id2",
	}
	trig.wg.Add(1)
	s.RegisterTimer(g1Addr, "tmr1", time.Second)
	s.RegisterTimer(g2Addr, "tmr1", time.Second)
	s.Cancel(g2Addr, "tmr1")
	now = now.Add(2 * time.Second)
	trig.expect(t, []triggerExpectation{
		{
			grainAddr: g1Addr,
			name:      "tmr1",
		},
	})

	trig.wg.Add(3)
	s.RegisterTimer(g1Addr, "tmr1", time.Second)
	s.RegisterTimer(g2Addr, "tmr1", time.Second)
	s.RegisterTimer(g2Addr, "tmr2", time.Second)
	s.RegisterTimer(g2Addr, "tmr3", 10*time.Second)
	now = now.Add(2 * time.Second)

	trig.expect(t, []triggerExpectation{
		{
			grainAddr: g1Addr,
			name:      "tmr1",
		},
		{
			grainAddr: g2Addr,
			name:      "tmr1",
		},
		{
			grainAddr: g2Addr,
			name:      "tmr2",
		},
	})

	s.Stop(context.Background())
}
