package silo

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	grainservices "github.com/jaym/go-orleans/grain/services"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type timerEntry struct {
	grainAddr grain.Identity
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
	ident  grain.Identity
	name   string
	d      time.Duration
	repeat bool
	resp   chan error
}

type timerServiceCancelTimerReq struct {
	ident grain.Identity
	name  string
	resp  chan bool
}

type timerServiceControlMessage struct {
	msgType  timerServiceCtlMsgType
	register *timerServiceReigsterTimerReq
	cancel   *timerServiceCancelTimerReq
}

type timerServiceImpl struct {
	grainTimerTrigger timer.GrainTimerTrigger
	log               logr.Logger

	nowProvider func() time.Time
	ctlChan     chan timerServiceControlMessage
	stopChan    chan struct{}

	timerEntries map[string]*timerEntry
	queue        *timerEntryHeap
}

func newTimerServiceImpl(log logr.Logger, grainTimerTrigger timer.GrainTimerTrigger) timer.TimerService {
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
					err := s.register(msg.register.ident, msg.register.name, msg.register.d, msg.register.repeat)
					msg.register.resp <- err
					close(msg.register.resp)
				case timerServiceCtlMsgTypeCancel:
					canceled := s.cancel(msg.cancel.ident, msg.cancel.name)
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
						s.log.V(4).Info("triggering grain", "identity", v.grainAddr, "triggerName", v.name)
						s.grainTimerTrigger(v.grainAddr, v.name)
						if v.repeat {
							s.log.V(4).Info("reregistering timer", "identity", v.grainAddr, "triggerName", v.name)
							v.triggerAt = s.nowProvider().Add(v.d)
							heap.Push(s.queue, v)
							continue
						}
						s.log.V(4).Info("removing timer", "identity", v.grainAddr, "triggerName", v.name)
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

func (s *timerServiceImpl) RegisterTimer(ident grain.Identity, name string, d time.Duration) error {
	respChan := make(chan error, 1)
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeRegister,
		register: &timerServiceReigsterTimerReq{
			ident: ident,
			name:  name,
			d:     d,
			resp:  respChan,
		},
	}
	return <-respChan
}

func (s *timerServiceImpl) Cancel(ident grain.Identity, name string) bool {
	respChan := make(chan bool, 1)
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeCancel,
		cancel: &timerServiceCancelTimerReq{
			ident: ident,
			name:  name,
			resp:  respChan,
		},
	}
	return <-respChan
}

func (s *timerServiceImpl) RegisterTicker(ident grain.Identity, name string, d time.Duration) error {
	respChan := make(chan error, 1)
	s.ctlChan <- timerServiceControlMessage{
		msgType: timerServiceCtlMsgTypeRegister,
		register: &timerServiceReigsterTimerReq{
			ident:  ident,
			name:   name,
			d:      d,
			repeat: true,
			resp:   respChan,
		},
	}
	return <-respChan
}

func (s *timerServiceImpl) register(ident grain.Identity, name string, d time.Duration, repeat bool) error {
	s.log.V(4).Info("processing register", "identity", ident, "name", name, "duration", d, "repeat", repeat)

	entryName := s.entryName(ident.GrainType, ident.ID, name)
	if e, ok := s.timerEntries[entryName]; ok {
		if !e.canceled {
			return grainservices.ErrTimerAlreadyRegistered
		}
	}
	entry := &timerEntry{
		grainAddr: ident,
		name:      name,
		triggerAt: s.nowProvider().Add(d),
		repeat:    repeat,
	}
	s.timerEntries[entryName] = entry
	heap.Push(s.queue, entry)
	return nil
}

func (s *timerServiceImpl) cancel(ident grain.Identity, name string) bool {
	s.log.V(4).Info("processing cancel", "identity", ident, "name", name)

	entryName := s.entryName(ident.GrainType, ident.ID, name)
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

type grainTimerServiceImpl struct {
	grainIdentity grain.Identity
	timerService  timer.TimerService
	timers        map[string]func()
}

func (g *grainTimerServiceImpl) RegisterTimer(name string, d time.Duration, f func()) error {
	if err := g.timerService.RegisterTimer(g.grainIdentity, name, d); err != nil {
		return err
	}
	g.timers[name] = f
	return nil
}

func (g *grainTimerServiceImpl) RegisterTicker(name string, d time.Duration, f func()) error {
	if err := g.timerService.RegisterTicker(g.grainIdentity, name, d); err != nil {
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
		return g.timerService.Cancel(g.grainIdentity, name)
	}
	return false
}
