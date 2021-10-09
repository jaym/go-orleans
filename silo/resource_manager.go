package silo

import (
	"container/list"
	"errors"
	"time"
)

type EvictTrigger func(addresses []Address)

var ErrNoCapacity = errors.New("No capacity")

type resourceManagerEntry struct {
	grainAddr   Address
	lastTouched time.Time
	element     *list.Element
}

type resourceManagerCtlMsgType int

const (
	resourceManagerTouchMsgType resourceManagerCtlMsgType = iota
	resourceManagerRemoveMsgType
)

type resourceManagerTouchMsg struct {
	grainAddr Address
	resp      chan error
}

type resourceManagerRemoveMsg struct {
	grainAddr Address
	resp      chan error
}

type resourceManagerCtlMsg struct {
	msgType resourceManagerCtlMsgType
	touch   *resourceManagerTouchMsg
	remove  *resourceManagerRemoveMsg
}

type retryEntry struct {
	grainAddr Address
	expireAt  time.Time
	resp      chan error
}

type resourceManager struct {
	nowProvider  func() time.Time
	evictTrigger EvictTrigger

	capacity int
	used     int

	recencyList *list.List
	grains      map[Address]*resourceManagerEntry
	retryList   []retryEntry

	ctlChan   chan resourceManagerCtlMsg
	evictChan chan []Address
}

func newResourceManager(capacity int, evictTrigger EvictTrigger) *resourceManager {
	return &resourceManager{
		nowProvider:  time.Now,
		evictTrigger: evictTrigger,
		capacity:     capacity,
		recencyList:  list.New(),
		grains:       make(map[Address]*resourceManagerEntry),
		ctlChan:      make(chan resourceManagerCtlMsg, 512),
		evictChan:    make(chan []Address),
		retryList:    make([]retryEntry, 0, 32),
	}
}

func (r *resourceManager) Start() {
	r.start()
}

func (r *resourceManager) Touch(grainAddr Address) error {
	respChan := make(chan error, 1)
	r.ctlChan <- resourceManagerCtlMsg{
		msgType: resourceManagerTouchMsgType,
		touch: &resourceManagerTouchMsg{
			grainAddr: grainAddr,
			resp:      respChan,
		},
	}
	return <-respChan
}

func (r *resourceManager) Remove(grainAddr Address) error {
	respChan := make(chan error, 1)
	r.ctlChan <- resourceManagerCtlMsg{
		msgType: resourceManagerRemoveMsgType,
		remove: &resourceManagerRemoveMsg{
			grainAddr: grainAddr,
			resp:      respChan,
		},
	}
	return <-respChan
}

func (r *resourceManager) start() {
	go func() {
		for addresesToEvict := range r.evictChan {
			r.evictTrigger(addresesToEvict)
			time.Sleep(1 * time.Second)
		}
	}()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)

		for {
			select {
			case msg := <-r.ctlChan:
				switch msg.msgType {
				case resourceManagerTouchMsgType:
					err := r.touch(msg.touch.grainAddr)
					if err == ErrNoCapacity {
						if r.addRetryEntry(msg.touch) {
							continue
						}
					}
					msg.touch.resp <- err
				case resourceManagerRemoveMsgType:
					r.remove(msg.remove.grainAddr)
					msg.remove.resp <- nil
				}
			case <-ticker.C:
				r.purgeRetryList()
				if len(r.retryList) > 0 {
					r.evict()
				}
			}
		}
	}()
}

func (r *resourceManager) addRetryEntry(msg *resourceManagerTouchMsg) bool {
	r.purgeRetryList()
	if len(r.retryList)+1 > 32 {
		return false
	}
	r.retryList = append(r.retryList, retryEntry{
		grainAddr: msg.grainAddr,
		expireAt:  r.nowProvider().Add(5000 * time.Millisecond),
		resp:      msg.resp,
	})
	return true
}

func (r *resourceManager) purgeRetryList() {
	i := 0
	now := r.nowProvider()
	for i < len(r.retryList) {
		if now.After(r.retryList[i].expireAt) {
			r.retryList[i].resp <- ErrNoCapacity
			i++
		} else {
			break
		}
	}
	if i > 0 {
		k := i
		for j := 0; j < len(r.retryList)-i; j++ {
			r.retryList[j] = r.retryList[k]
			k++
		}
		r.retryList = r.retryList[:len(r.retryList)-i]
	}
}

func (r *resourceManager) touch(grainAddr Address) error {
	entry, ok := r.grains[grainAddr]
	if !ok {
		if r.used >= r.capacity {
			r.evict()

			return ErrNoCapacity
		}
		entry = &resourceManagerEntry{
			lastTouched: r.nowProvider(),
			grainAddr:   grainAddr,
		}
		element := r.recencyList.PushFront(entry)
		entry.element = element
		r.grains[grainAddr] = entry
		r.used++

		return nil
	}
	entry.lastTouched = r.nowProvider()
	r.recencyList.MoveToFront(entry.element)

	return nil
}

func (r *resourceManager) evict() int {
	desiredEviction := r.used - int(0.2*float32(r.capacity))
	if desiredEviction <= 0 {
		return 0
	}
	numEvicted := 0
	element := r.recencyList.Back()
	addresesToEvict := make([]Address, 0, desiredEviction)
	for element != nil && numEvicted < desiredEviction {
		entry := element.Value.(*resourceManagerEntry)
		addresesToEvict = append(addresesToEvict, entry.grainAddr)
		numEvicted++
		element = element.Prev()
	}
	select {
	case r.evictChan <- addresesToEvict:
		return numEvicted
	default:
		return 0
	}
}

func (r *resourceManager) remove(grainAddr Address) {
	entry, ok := r.grains[grainAddr]
	if !ok {
		return
	}

	delete(r.grains, entry.grainAddr)
	r.used--
	r.recencyList.Remove(entry.element)

	if len(r.retryList) > 0 {
		e := r.retryList[0]
		err := r.touch(e.grainAddr)
		e.resp <- err
		if len(r.retryList) > 1 {
			copy(r.retryList[0:len(r.retryList)-1], r.retryList[1:len(r.retryList)])
		}
		r.retryList = r.retryList[:len(r.retryList)-1]
	}
}
