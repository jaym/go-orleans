package silo

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	grainservices "github.com/jaym/go-orleans/grain/services"
)

type timerEntry struct {
	grainAddr grain.Address
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
	addr   grain.Address
	name   string
	d      time.Duration
	repeat bool
	resp   chan error
}

type timerServiceCancelTimerReq struct {
	addr grain.Address
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
	log               logr.Logger

	nowProvider func() time.Time
	ctlChan     chan timerServiceControlMessage
	stopChan    chan struct{}

	timerEntries map[string]*timerEntry
	queue        *timerEntryHeap
}

type GrainTimerTrigger func(grainAddr grain.Address, name string)

func newTimerServiceImpl(log logr.Logger, grainTimerTrigger GrainTimerTrigger) grainservices.TimerService {
	ctlChan := make(chan timerServiceControlMessage)
	queue := make(timerEntryHeap, 0, 512)
	heap.Init(&queue)
	s := &timerServiceImpl{
		grainTimerTrigger: grainTimerTrigger,
		log:               log,
		nowProvider:       time.Now,
		ctlChan:           ctlChan,
		stopChan:          make(chan struct{}),
		timerEntries:      make(map[string]*timerEntry),
		queue:             &queue,
	}
	return s
}

func (s *timerServiceImpl) Start() {
	s.log.V(3).Info("starting timer service")
	s.start()
}

func (s *timerServiceImpl) start() {
	go func() {
		s.log.V(3).Info("started timer service")
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
					s.log.V(1).Info("stopping background thread")
					break LOOP
				default:
					s.log.V(0).Info("unknown ctl chan message", "type", msg.msgType)
				}
			case <-ticker.C:
				now := s.nowProvider()
				for {
					if len(*s.queue) == 0 {
						s.log.V(5).Info("nothing to trigger: none available")
						break
					}

					if (*s.queue)[0].triggerAt.After(now) {
						s.log.V(5).Info("nothing to trigger: none ready")
						break
					}
					v := heap.Pop(s.queue).(*timerEntry)
					if !v.canceled {
						s.log.V(4).Info("triggering grain", "address", v.grainAddr, "triggerName", v.name)
						s.grainTimerTrigger(v.grainAddr, v.name)
						if v.repeat {
							s.log.V(4).Info("reregistering timer", "address", v.grainAddr, "triggerName", v.name)
							v.triggerAt = s.nowProvider().Add(v.d)
							heap.Push(s.queue, v)
							continue
						}
						s.log.V(4).Info("removing timer", "address", v.grainAddr, "triggerName", v.name)
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

func (s *timerServiceImpl) RegisterTimer(addr grain.Address, name string, d time.Duration) error {
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

func (s *timerServiceImpl) Cancel(addr grain.Address, name string) bool {
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

func (s *timerServiceImpl) RegisterTicker(addr grain.Address, name string, d time.Duration) error {
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

func (s *timerServiceImpl) register(addr grain.Address, name string, d time.Duration, repeat bool) error {
	s.log.V(4).Info("processing register", "address", addr, "name", name, "duration", d, "repeat", repeat)

	entryName := s.entryName(addr.GrainType, addr.ID, name)
	if e, ok := s.timerEntries[entryName]; ok {
		if !e.canceled {
			return grainservices.ErrTimerAlreadyRegistered
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

func (s *timerServiceImpl) cancel(addr grain.Address, name string) bool {
	s.log.V(4).Info("processing cancel", "address", addr, "name", name)

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
	grainAddress grain.Address
	timerService grainservices.TimerService
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
