package services

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/jaym/go-orleans/grain"
)

type RegisteredObserver[T any] interface {
	grain.GrainReference
	ExpiresAt() time.Time
	Get() T
}

type observerRegistration[T any] struct {
	grain.Identity
	expiresAt time.Time
	val       T
}

func (o observerRegistration[T]) ExpiresAt() time.Time {
	return o.expiresAt
}

func (o observerRegistration[T]) Get() T {
	return o.val
}

type InMemoryGrainObserverManager[T any, N proto.Message] struct {
	grainType           string
	observableName      string
	registeredObservers map[grain.Identity]observerRegistration[T]
	registrationTimeout time.Duration
	nowProvider         func() time.Time
}

func NewInMemoryGrainObserverManager[T any, N proto.Message](ident grain.Identity, observableName string, registrationTimeout time.Duration) *InMemoryGrainObserverManager[T, N] {
	return &InMemoryGrainObserverManager[T, N]{
		grainType:           ident.GrainType,
		observableName:      observableName,
		registeredObservers: make(map[grain.Identity]observerRegistration[T]),
		registrationTimeout: registrationTimeout,
		nowProvider:         time.Now,
	}
}

func (m *InMemoryGrainObserverManager[T, N]) List() []RegisteredObserver[T] {
	observers := make([]RegisteredObserver[T], len(m.registeredObservers))
	i := 0
	for _, reg := range m.registeredObservers {
		observers[i] = reg
		i++
	}
	return observers
}

func (m *InMemoryGrainObserverManager[T, N]) Add(ref grain.GrainReference, val T) {
	now := m.nowProvider()
	expiresAt := now.Add(m.registrationTimeout)
	m.registeredObservers[ref.GetIdentity()] = observerRegistration[T]{
		Identity:  ref.GetIdentity(),
		expiresAt: expiresAt,
		val:       val,
	}
}

func (m *InMemoryGrainObserverManager[T, N]) Remove(ref grain.GrainReference) {
	delete(m.registeredObservers, ref.GetIdentity())
}

func (m *InMemoryGrainObserverManager[T, N]) RemoveExpired() []RegisteredObserver[T] {
	expired := make([]RegisteredObserver[T], 0, 8)
	now := m.nowProvider()

	for ident, or := range m.registeredObservers {
		if or.ExpiresAt().Before(now) {
			expired = append(expired, or)
			delete(m.registeredObservers, ident)
		}
	}
	return expired
}
