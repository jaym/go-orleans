package silo

import (
	"context"
	"fmt"

	"github.com/jaym/go-orleans/grain"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/proto"
)

type EncodedRegisteredObserver struct {
	grain.Address
	uuid ksuid.KSUID
	val  []byte
}

func NewRegisteredObserver(address grain.Address, val []byte) (*EncodedRegisteredObserver, error) {
	return &EncodedRegisteredObserver{
		Address: address,
		uuid:    ksuid.New(),
		val:     val,
	}, nil
}

func (o *EncodedRegisteredObserver) UUID() string {
	return o.uuid.String()
}

func (o *EncodedRegisteredObserver) Get(v interface{}) error {
	return proto.Unmarshal(o.val, v.(proto.Message))
}

type InmemoryObserverManager struct {
	registeredObservers map[string][]grain.RegisteredObserver
	siloClient          SiloClient
	owner               grain.Address
}

func NewInmemoryObserverManager(owner grain.Address, siloClient SiloClient) *InmemoryObserverManager {
	return &InmemoryObserverManager{
		owner:               owner,
		siloClient:          siloClient,
		registeredObservers: make(map[string][]grain.RegisteredObserver),
	}
}

func (m *InmemoryObserverManager) List(ctx context.Context, observableName string) ([]grain.RegisteredObserver, error) {
	return m.registeredObservers[observableName], nil
}

func (m *InmemoryObserverManager) Add(ctx context.Context, observableName string, address grain.Address, val []byte) (grain.RegisteredObserver, error) {
	o, err := NewRegisteredObserver(address, val)
	if err != nil {
		return nil, err
	}
	m.registeredObservers[observableName] = append(m.registeredObservers[observableName], o)
	return o, nil
}

func (m *InmemoryObserverManager) Remove(ctx context.Context, observableName string, uuid string) error {
	observersForObservable := m.registeredObservers[observableName]
	for i, ro := range observersForObservable {
		if ro.UUID() == uuid {
			observersForObservable[i] = observersForObservable[len(observersForObservable)-1]
			m.registeredObservers[observableName] = observersForObservable[:len(observersForObservable)-1]
			return nil
		}
	}
	return nil
}

func (m *InmemoryObserverManager) Notify(ctx context.Context, observableName string, observers []grain.RegisteredObserver, val proto.Message) error {
	fmt.Printf("notifying %d observers\n", len(observers))
	receivers := make([]grain.Address, len(observers))
	for i := range observers {
		receivers[i] = observers[i].GetAddress()
	}
	return m.siloClient.NotifyObservers(ctx, m.owner.GrainType, observableName, receivers, val)
}
