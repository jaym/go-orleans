package silo

import (
	"container/heap"
	"context"
	stdlog "log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-logr/stdr"
	"github.com/jaym/go-orleans/grain"
	"github.com/stretchr/testify/assert"
)

func TestTimerEntryHeap(t *testing.T) {
	queue := make(timerEntryHeap, 0, 512)
	heap.Init(&queue)

	now := time.Now()

	grainAdder := grain.Identity{
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
	l        sync.Mutex
	retry    bool
	wg       *sync.WaitGroup
	received []triggerExpectation
}

func (trig *triggerable) TriggerGrainTimer(grainAddr grain.Identity, name string, try int) bool {
	trig.l.Lock()
	defer trig.l.Unlock()
	trig.received = append(trig.received, triggerExpectation{grainAddr: grainAddr, name: name, try: try})
	trig.wg.Done()
	return trig.retry
}

type triggerExpectation struct {
	grainAddr grain.Identity
	name      string
	try       int
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
	c := clock.NewMock()

	trig := &triggerable{wg: &sync.WaitGroup{}}
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})
	s := newTimerServiceImpl(log, c, trig.TriggerGrainTimer).(*timerServiceImpl)
	s.Start()

	g1Addr := grain.Identity{
		GrainType: "type1",
		ID:        "id1",
	}
	g2Addr := grain.Identity{
		GrainType: "type2",
		ID:        "id2",
	}
	trig.wg.Add(1)
	s.RegisterTimer(g1Addr, "tmr1", time.Second)
	s.RegisterTimer(g2Addr, "tmr1", time.Second)
	s.Cancel(g2Addr, "tmr1")
	c.Add(2 * time.Second)
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
	c.Add(2 * time.Second)

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

func TestTimerServiceRetryTimer(t *testing.T) {
	c := clock.NewMock()

	trig := &triggerable{wg: &sync.WaitGroup{}, retry: true}
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})
	s := newTimerServiceImpl(log, c, trig.TriggerGrainTimer).(*timerServiceImpl)
	s.Start()

	g1Addr := grain.Identity{
		GrainType: "type1",
		ID:        "id1",
	}

	trig.wg.Add(4)
	s.RegisterTimer(g1Addr, "tmr1", time.Second)

	c.Add(time.Second + 1)
	c.Add(time.Second + 1)
	c.Add(2*time.Second + 1)
	c.Add(4*time.Second + 1)
	trig.expect(t, []triggerExpectation{
		{
			grainAddr: g1Addr,
			name:      "tmr1",
			try:       0,
		},
		{
			grainAddr: g1Addr,
			name:      "tmr1",
			try:       1,
		},
		{
			grainAddr: g1Addr,
			name:      "tmr1",
			try:       2,
		},
		{
			grainAddr: g1Addr,
			name:      "tmr1",
			try:       3,
		},
	})

	s.Stop(context.Background())
}

func TestTimerServiceRetryTicker(t *testing.T) {
	c := clock.NewMock()

	trig := &triggerable{wg: &sync.WaitGroup{}, retry: true}
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})
	s := newTimerServiceImpl(log, c, trig.TriggerGrainTimer).(*timerServiceImpl)
	s.Start()

	g1Addr := grain.Identity{
		GrainType: "type1",
		ID:        "id1",
	}

	trig.wg.Add(4)
	s.RegisterTicker(g1Addr, "ticker1", 2*time.Second)

	c.Add(2*time.Second + 1)
	c.Add(1*time.Second + 1)
	c.Add(2*time.Second + 1)
	c.Add(2*time.Second + 1)
	trig.expect(t, []triggerExpectation{
		{
			grainAddr: g1Addr,
			name:      "ticker1",
			try:       0,
		},
		{
			grainAddr: g1Addr,
			name:      "ticker1",
			try:       1,
		},
		{
			grainAddr: g1Addr,
			name:      "ticker1",
			try:       2,
		},
		{
			grainAddr: g1Addr,
			name:      "ticker1",
			try:       3,
		},
	})

	s.Stop(context.Background())
}
