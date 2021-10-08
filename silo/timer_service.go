package silo

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrTimerAlreadyRegistered = errors.New("timer already registered")

type TimerService interface {
	Start()
	Stop(context.Context) error
	RegisterTimer(addr Address, name string, d time.Duration) error
	RegisterTicker(addr Address, name string, d time.Duration) error
	Cancel(addr Address, name string) bool
}

type timerEntry struct {
	grainAddr Address
	name      string
	d         time.Duration
	repeat    bool

	triggerAt time.Time
	canceled  bool
}

type timerServiceCtlMsgType int

const (
	timerServiceCtlMsgTypeRegister timerServiceCtlMsgType = iota
	timerServiceCtlMsgTypeCancel
	timerServiceCtlMsgTypeStop
)

type timerServiceReigsterTimerReq struct {
	addr   Address
	name   string
	d      time.Duration
	repeat bool
	resp   chan error
}

type timerServiceCancelTimerReq struct {
	addr Address
	name string
	resp chan bool
}

type timerServiceControlMessage struct {
	msgType  timerServiceCtlMsgType
	register *timerServiceReigsterTimerReq
	cancel   *timerServiceCancelTimerReq
}

type timerServiceImpl struct {
	grainTimerTrigger GrainTimerTrigger

	nowProvider func() time.Time
	ctlChan     chan timerServiceControlMessage
	stopChan    chan struct{}

	timerEntries map[string]*timerEntry
	queue        *timerEntryHeap
}

type GrainTimerTrigger func(grainAddr Address, name string)

func newTimerServiceImpl(grainTimerTrigger GrainTimerTrigger) TimerService {
	ctlChan := make(chan timerServiceControlMessage)
	queue := make(timerEntryHeap, 0, 512)
	heap.Init(&queue)
	s := &timerServiceImpl{
		grainTimerTrigger: grainTimerTrigger,
		nowProvider:       time.Now,
		ctlChan:           ctlChan,
		stopChan:          make(chan struct{}),
		timerEntries:      make(map[string]*timerEntry),
		queue:             &queue,
	}
	return s
}

func (s *timerServiceImpl) Start() {
	s.start()
}

func (s *timerServiceImpl) start() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)

	LOOP:
		for {
			select {
			case msg := <-s.ctlChan:
				switch msg.msgType {
				case timerServiceCtlMsgTypeRegister:
					err := s.register(msg.register.addr, msg.register.name, msg.register.d, msg.register.repeat)
					msg.register.resp <- err
					close(msg.register.resp)
				case timerServiceCtlMsgTypeCancel:
					canceled := s.cancel(msg.cancel.addr, msg.cancel.name)
					msg.cancel.resp <- canceled
					close(msg.cancel.resp)
				case timerServiceCtlMsgTypeStop:
					break LOOP
				}
			case <-ticker.C:
				now := s.nowProvider()
				for {
					if len(*s.queue) == 0 {
						break
					}

					if (*s.queue)[0].triggerAt.After(now) {
						break
					}
					v := heap.Pop(s.queue).(*timerEntry)
					if !v.canceled {
						s.grainTimerTrigger(v.grainAddr, v.name)
						if v.repeat {
							v.triggerAt = s.nowProvider().Add(v.d)
							heap.Push(s.queue, v)
							continue
						}
						entryName := s.entryName(v.grainAddr.GrainType, v.grainAddr.ID, v.name)
						delete(s.timerEntries, entryName)
					}
				}
			}
		}
		ticker.Stop()
		close(s.stopChan)
	}()
}

func (s *timerServiceImpl) Stop(ctx context.Context) error {
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeStop,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopChan:
	}
	return nil
}

func (s *timerServiceImpl) RegisterTimer(addr Address, name string, d time.Duration) error {
	respChan := make(chan error, 1)
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeRegister,
		register: &timerServiceReigsterTimerReq{
			addr: addr,
			name: name,
			d:    d,
			resp: respChan,
		},
	}
	return <-respChan
}

func (s *timerServiceImpl) Cancel(addr Address, name string) bool {
	respChan := make(chan bool, 1)
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeCancel,
		cancel: &timerServiceCancelTimerReq{
			addr: addr,
			name: name,
			resp: respChan,
		},
	}
	return <-respChan
}

func (s *timerServiceImpl) RegisterTicker(addr Address, name string, d time.Duration) error {
	respChan := make(chan error, 1)
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeRegister,
		register: &timerServiceReigsterTimerReq{
			addr:   addr,
			name:   name,
			d:      d,
			repeat: true,
			resp:   respChan,
		},
	}
	return <-respChan
}

func (s *timerServiceImpl) register(addr Address, name string, d time.Duration, repeat bool) error {
	entryName := s.entryName(addr.GrainType, addr.ID, name)
	if e, ok := s.timerEntries[entryName]; ok {
		if !e.canceled {
			return ErrTimerAlreadyRegistered
		}
	}
	entry := &timerEntry{
		grainAddr: addr,
		name:      name,
		triggerAt: s.nowProvider().Add(d),
	}
	s.timerEntries[entryName] = entry
	heap.Push(s.queue, entry)
	return nil
}

func (s *timerServiceImpl) cancel(addr Address, name string) bool {
	entryName := s.entryName(addr.GrainType, addr.ID, name)
	entry, ok := s.timerEntries[entryName]

	if !ok {
		return false
	}

	entry.canceled = true

	delete(s.timerEntries, entryName)

	return true
}

func (*timerServiceImpl) entryName(grainType string, grainID string, name string) string {
	return fmt.Sprintf("%s/%s/%s", grainType, grainID, name)
}

type timerEntryHeap []*timerEntry

func (h timerEntryHeap) Len() int           { return len(h) }
func (h timerEntryHeap) Less(i, j int) bool { return h[i].triggerAt.Before(h[j].triggerAt) }
func (h timerEntryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timerEntryHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*timerEntry))
}

func (h *timerEntryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type GrainTimerService interface {
	Trigger(name string)
	RegisterTimer(name string, d time.Duration, f func()) error
	RegisterTicker(name string, d time.Duration, f func()) error
	Cancel(name string) bool
}

type grainTimerServiceImpl struct {
	grainAddress Address
	timerService TimerService
	timers       map[string]func()
}

func (g *grainTimerServiceImpl) RegisterTimer(name string, d time.Duration, f func()) error {
	if err := g.timerService.RegisterTimer(g.grainAddress, name, d); err != nil {
		return err
	}
	g.timers[name] = f
	return nil
}

func (g *grainTimerServiceImpl) RegisterTicker(name string, d time.Duration, f func()) error {
	if err := g.timerService.RegisterTicker(g.grainAddress, name, d); err != nil {
		return err
	}
	g.timers[name] = f
	return nil
}

func (g *grainTimerServiceImpl) Trigger(name string) {
	if f, ok := g.timers[name]; ok {
		delete(g.timers, name)
		f()
	}
}

func (g *grainTimerServiceImpl) Cancel(name string) bool {
	if _, ok := g.timers[name]; ok {
		delete(g.timers, name)
		return g.timerService.Cancel(g.grainAddress, name)
	}
	return false
}
