package activation

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/observer"
	"google.golang.org/protobuf/proto"
)

type EncodedRegisteredObserver struct {
	grain.Identity
	observableName string
	val            []byte
}

func NewRegisteredObserver(identity grain.Identity, observableName string, val interface{}) (*EncodedRegisteredObserver, error) {
	// TODO: should not need to marshal
	data, err := proto.Marshal(val.(proto.Message))
	if err != nil {
		return nil, err
	}
	return &EncodedRegisteredObserver{
		Identity:       identity,
		observableName: observableName,
		val:            data,
	}, nil
}

func (o *EncodedRegisteredObserver) Get(v interface{}) error {
	return proto.Unmarshal(o.val, v.(proto.Message))
}

func (o *EncodedRegisteredObserver) ObservableName() string {
	return o.observableName
}

type grainObserverManager struct {
	registeredObservers map[string][]grain.RegisteredObserver
	siloClient          grain.SiloClient
	owner               grain.Identity
	store               observer.Store
	loaded              bool
}

func newGrainObserverManager(owner grain.Identity, store observer.Store, siloClient grain.SiloClient) *grainObserverManager {
	m := &grainObserverManager{
		owner:               owner,
		siloClient:          siloClient,
		registeredObservers: make(map[string][]grain.RegisteredObserver),
		store:               store,
	}

	return m
}

func (m *grainObserverManager) ensureLoaded(ctx context.Context) error {
	if m.loaded {
		return nil
	}
	observers, err := m.store.List(ctx, m.owner, "")
	if err != nil {
		return err
	}
	for i := range observers {
		o := observers[i]
		observableName := o.ObservableName()
		m.registeredObservers[observableName] = append(m.registeredObservers[observableName], o)
	}
	m.loaded = true

	return nil
}

func (m *grainObserverManager) List(ctx context.Context, observableName string) ([]grain.RegisteredObserver, error) {
	if err := m.ensureLoaded(ctx); err != nil {
		return nil, err
	}

	return m.registeredObservers[observableName], nil
}

func (m *grainObserverManager) Add(ctx context.Context, observableName string, identity grain.Identity, val proto.Message) (grain.RegisteredObserver, error) {
	if err := m.ensureLoaded(ctx); err != nil {
		return nil, err
	}

	o, err := NewRegisteredObserver(identity, observableName, val)
	if err != nil {
		return nil, err
	}
	observables := m.registeredObservers[observableName]
	for i := range observables {
		if observables[i].GetIdentity() == identity {
			err := m.store.Add(ctx, m.owner, observableName, identity, observer.AddWithVal(val))
			if err != nil {
				return nil, err
			}

			observables[i] = o
			return o, nil
		}
	}

	if err := m.store.Add(ctx, m.owner, observableName, identity, observer.AddWithVal(val)); err != nil {
		return nil, err
	}
	m.registeredObservers[observableName] = append(m.registeredObservers[observableName], o)

	return o, nil
}

func (m *grainObserverManager) Remove(ctx context.Context, observableName string, identity grain.Identity) error {
	if err := m.ensureLoaded(ctx); err != nil {
		return err
	}
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

	if err := m.store.Remove(ctx, m.owner, observer.RemoveByObserverGrain(identity)); err != nil {
		return err
	}

	if idx >= 0 {
		lastIdx := len(observersForObservable)
		observersForObservable[idx] = observersForObservable[lastIdx]
		m.registeredObservers[observableName] = observersForObservable[:lastIdx]
	}
	return nil
}

func (m *grainObserverManager) Notify(ctx context.Context, observableName string, observers []grain.RegisteredObserver, val proto.Message) error {
	if err := m.ensureLoaded(ctx); err != nil {
		return err
	}

	receivers := make([]grain.Identity, len(observers))
	for i := range observers {
		receivers[i] = observers[i].GetIdentity()
	}
	return m.siloClient.NotifyObservers(ctx, m.owner.GrainType, observableName, receivers, val)
}
