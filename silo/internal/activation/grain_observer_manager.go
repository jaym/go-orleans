package activation

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
	"google.golang.org/protobuf/proto"
)

type EncodedRegisteredObserver struct {
	grain.Identity
	observableName string
	expiresAt      time.Time
	val            []byte
}

func NewRegisteredObserver(identity grain.Identity, observableName string, expiresAt time.Time, val interface{}) (*EncodedRegisteredObserver, error) {
	// TODO: should not need to marshal
	data, err := proto.Marshal(val.(proto.Message))
	if err != nil {
		return nil, err
	}
	return &EncodedRegisteredObserver{
		Identity:       identity,
		observableName: observableName,
		expiresAt:      expiresAt,
		val:            data,
	}, nil
}

func (o *EncodedRegisteredObserver) Get(v interface{}) error {
	return proto.Unmarshal(o.val, v.(proto.Message))
}

func (o *EncodedRegisteredObserver) ObservableName() string {
	return o.observableName
}

func (o *EncodedRegisteredObserver) ExpiresAt() time.Time {
	return o.expiresAt
}

type grainObserverManager struct {
	registeredObservers map[string][]grain.RegisteredObserver
	siloClient          grain.SiloClient
	owner               grain.Identity
	loaded              bool
}

func newGrainObserverManager(owner grain.Identity, siloClient grain.SiloClient) *grainObserverManager {
	m := &grainObserverManager{
		owner:               owner,
		siloClient:          siloClient,
		registeredObservers: make(map[string][]grain.RegisteredObserver),
	}

	return m
}

func (m *grainObserverManager) List(ctx context.Context, observableName string) ([]grain.RegisteredObserver, error) {
	persistedObservers := m.registeredObservers[observableName]
	observers := make([]grain.RegisteredObserver, 0, len(persistedObservers))
	now := time.Now()
	changed := false
	for _, o := range persistedObservers {
		if now.Before(o.ExpiresAt()) {
			changed = true
			observers = append(observers, o)
		}
	}
	if changed {
		observersCopy := make([]grain.RegisteredObserver, len(observers))
		copy(observersCopy, observers)

		m.registeredObservers[observableName] = observersCopy
	}

	return observers, nil
}

func (m *grainObserverManager) Add(ctx context.Context, observableName string, identity grain.Identity, registrationTimeout time.Duration, val proto.Message) (grain.RegisteredObserver, error) {
	var expiresAt time.Time
	if registrationTimeout != 0 {
		expiresAt = time.Now().Add(registrationTimeout)
	}

	o, err := NewRegisteredObserver(identity, observableName, expiresAt, val)
	if err != nil {
		return nil, err
	}
	observables := m.registeredObservers[observableName]
	for i := range observables {
		if observables[i].GetIdentity() == identity {
			observables[i] = o
			return o, nil
		}
	}

	m.registeredObservers[observableName] = append(m.registeredObservers[observableName], o)

	return o, nil
}

func (m *grainObserverManager) Remove(ctx context.Context, observableName string, identity grain.Identity) error {
	observersForObservable, ok := m.registeredObservers[observableName]
	if !ok {
		return nil
	}

	idx := -1
	for i, o := range observersForObservable {
		if o.GetIdentity() == identity {
			idx = i
			break
		}
	}

	if idx == -1 {
		return nil
	}

	if idx >= 0 {
		lastIdx := len(observersForObservable) - 1
		observersForObservable[idx] = observersForObservable[lastIdx]
		m.registeredObservers[observableName] = observersForObservable[:lastIdx]
	}
	return nil
}

func (m *grainObserverManager) Notify(ctx context.Context, observableName string, observers []grain.RegisteredObserver, val proto.Message) error {
	receivers := make([]grain.Identity, len(observers))

	now := time.Now()
	for i := range observers {
		if now.Before(observers[i].ExpiresAt()) {
			receivers[i] = observers[i].GetIdentity()
		}
	}
	return m.siloClient.NotifyObservers(ctx, m.owner.GrainType, observableName, receivers, val)
}
