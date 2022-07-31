package transport

import (
	"container/heap"
	"time"

	"github.com/benbjohnson/clock"
)

type RequestType int

const (
	InvokeMethodRequestType       RequestType = 1
	RegisterObserverRequestType   RequestType = 2
	UnregisterObserverRequestType RequestType = 3
)

type Item struct {
	deadline time.Time

	typ  RequestType
	uuid string
}

type DeadlineHeap struct {
	h     deadlineHeap
	clock clock.Clock
}

func NewDeadlineHeap(c clock.Clock) *DeadlineHeap {
	h := &DeadlineHeap{
		h:     make(deadlineHeap, 0, 256),
		clock: c,
	}
	heap.Init(&h.h)
	return h
}

func (h *DeadlineHeap) ExpireAndAdd(typ RequestType, uuid string, deadline time.Time, f func(typ RequestType, uuid string)) {
	h.Expire(f)
	heap.Push(&h.h, &Item{
		deadline: deadline,
		typ:      typ,
		uuid:     uuid,
	})
}

func (h *DeadlineHeap) Expire(f func(typ RequestType, uuid string)) {
	now := h.clock.Now()
	for {
		if len(h.h) == 0 {
			return
		}
		if item := h.h[0]; !item.deadline.After(now) {
			f(item.typ, item.uuid)
			heap.Pop(&h.h)
		} else {
			break
		}
	}
}

type deadlineHeap []*Item

func (h deadlineHeap) Len() int { return len(h) }

func (h deadlineHeap) Less(i, j int) bool {
	return h[i].deadline.Before(h[j].deadline)
}

func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *deadlineHeap) Push(x any) {
	item := x.(*Item)
	*h = append(*h, item)
}

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}
