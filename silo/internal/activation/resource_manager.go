package activation

import (
	"container/list"
	"sync"
	"time"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/resourcemanager"
)

type EvictTrigger func(ident grain.Identity, onComplete func(error))

type resourceManagerEntry struct {
	grainAddr   grain.Identity
	lastTouched time.Time
	element     *list.Element
}

type resourceManagerCtlMsgType int

const (
	resourceManagerTouchMsgType resourceManagerCtlMsgType = iota
	resourceManagerRemoveMsgType
)

type resourceManagerTouchMsg struct {
	grainAddr grain.Identity
	resp      chan error
}

type resourceManagerRemoveMsg struct {
	grainAddr grain.Identity
	resp      chan error
}

type resourceManagerCtlMsg struct {
	msgType resourceManagerCtlMsgType
	touch   *resourceManagerTouchMsg
	remove  *resourceManagerRemoveMsg
}

type retryEntry struct {
	grainAddr grain.Identity
	expireAt  time.Time
	resp      chan error
}

type ResourceManager struct {
	nowProvider  func() time.Time
	evictTrigger EvictTrigger

	capacity           int
	used               int
	wantedMinAvailable float32

	recencyList *list.List
	grains      map[grain.Identity]*resourceManagerEntry
	retryList   []retryEntry

	ctlChan   chan resourceManagerCtlMsg
	evictChan chan []grain.Identity

	ignoredGrainTypes []string
}

func NewResourceManager(capacity int, evictTrigger EvictTrigger) *ResourceManager {
	return &ResourceManager{
		nowProvider:        time.Now,
		evictTrigger:       evictTrigger,
		capacity:           capacity,
		wantedMinAvailable: 0.2,
		recencyList:        list.New(),
		grains:             make(map[grain.Identity]*resourceManagerEntry),
		ctlChan:            make(chan resourceManagerCtlMsg, 512),
		evictChan:          make(chan []grain.Identity),
		retryList:          make([]retryEntry, 0, 32),
	}
}

func (r *ResourceManager) Start() {
	r.start()
}

func (r *ResourceManager) isIgnored(grainAddr grain.Identity) bool {
	for _, ignored := range r.ignoredGrainTypes {
		if ignored == grainAddr.GrainType {
			return true
		}
	}
	return false
}

func (r *ResourceManager) Touch(grainAddr grain.Identity) error {
	if r.isIgnored(grainAddr) {
		return nil
	}
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

func (r *ResourceManager) Remove(grainAddr grain.Identity) error {
	if r.isIgnored(grainAddr) {
		return nil
	}
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

func (r *ResourceManager) start() {
	go func() {
		for idents := range r.evictChan {
			wg := sync.WaitGroup{}
			wg.Add(len(idents))
			for _, ident := range idents {
				r.evictTrigger(ident, func(error) { wg.Done() })
			}
			wg.Wait()
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
					if err == resourcemanager.ErrNoCapacity {
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

func (r *ResourceManager) addRetryEntry(msg *resourceManagerTouchMsg) bool {
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

func (r *ResourceManager) purgeRetryList() {
	i := 0
	now := r.nowProvider()
	for i < len(r.retryList) {
		if now.After(r.retryList[i].expireAt) {
			r.retryList[i].resp <- resourcemanager.ErrNoCapacity
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

func (r *ResourceManager) touch(grainAddr grain.Identity) error {
	entry, ok := r.grains[grainAddr]
	if !ok {
		if r.used >= r.capacity {
			r.evict()

			return resourcemanager.ErrNoCapacity
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

func (r *ResourceManager) evict() int {
	desiredFree := int(r.wantedMinAvailable * float32(r.capacity))
	freeCapacity := r.capacity - r.used
	desiredEviction := desiredFree - freeCapacity
	if desiredEviction <= 0 {
		return 0
	}
	numEvicted := 0
	element := r.recencyList.Back()
	identsToEvict := make([]grain.Identity, 0, desiredEviction)
	for element != nil && numEvicted < desiredEviction {
		entry := element.Value.(*resourceManagerEntry)
		identsToEvict = append(identsToEvict, entry.grainAddr)
		numEvicted++
		element = element.Prev()
	}
	select {
	case r.evictChan <- identsToEvict:
		return numEvicted
	default:
		return 0
	}
}

func (r *ResourceManager) remove(grainAddr grain.Identity) {
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
