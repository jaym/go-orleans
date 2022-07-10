package silo

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
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
	try       int

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

	clock    clock.Clock
	ctlChan  chan timerServiceControlMessage
	stopChan chan struct{}

	timerEntries map[string]*timerEntry
	queue        *timerEntryHeap
}

func newTimerServiceImpl(log logr.Logger, c clock.Clock, grainTimerTrigger timer.GrainTimerTrigger) timer.TimerService {
	ctlChan := make(chan timerServiceControlMessage)
	queue := make(timerEntryHeap, 0, 512)
	heap.Init(&queue)
	s := &timerServiceImpl{
		grainTimerTrigger: grainTimerTrigger,
		log:               log,
		clock:             c,
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
		ticker := s.clock.Ticker(time.Second)

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
				now := s.clock.Now()
				for {
					if len(*s.queue) == 0 {
						s.log.V(16).Info("nothing to trigger: none available")
						break
					}

					if (*s.queue)[0].triggerAt.After(now) {
						s.log.V(16).Info("nothing to trigger: none ready")
						break
					}
					v := heap.Pop(s.queue).(*timerEntry)
					if !v.canceled {
						s.log.V(4).Info("triggering grain", "identity", v.grainAddr, "triggerName", v.name)
						retry := s.grainTimerTrigger(v.grainAddr, v.name, v.try)
						if v.repeat && retry {
							s.log.V(4).Info("retrying ticker", "identity", v.grainAddr, "triggerName", v.name)

							nextTriggerTime := now.Add(v.d)
							nextRetryTime := s.retryBackoffTime(v, now)
							if nextRetryTime.Before(nextTriggerTime) {
								v.triggerAt = nextRetryTime
							} else {
								v.triggerAt = nextTriggerTime
							}
							v.try++

							heap.Push(s.queue, v)
							continue
						} else if v.repeat {
							s.log.V(4).Info("reregistering ticker", "identity", v.grainAddr, "triggerName", v.name)
							v.triggerAt = now.Add(v.d)
							v.try = 0
							heap.Push(s.queue, v)
							continue
						} else if retry {
							v.triggerAt = s.retryBackoffTime(v, now)
							v.try++

							s.log.V(4).Info("retrying timer", "identity", v.grainAddr, "triggerName", v.name)
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

func (*timerServiceImpl) retryBackoffTime(v *timerEntry, now time.Time) time.Time {
	backoff := (1 << v.try)
	if backoff > 60 {
		backoff = 60
	}
	return now.Add(time.Duration(backoff) * time.Second)
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
		triggerAt: s.clock.Now().Add(d),
		repeat:    repeat,
		d:         d,
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
