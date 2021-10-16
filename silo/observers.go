package silo

import (
	"context"
	"fmt"

	"github.com/jaym/go-orleans/grain"
	"google.golang.org/protobuf/proto"
)

type EncodedRegisteredObserver struct {
	grain.Address
	val []byte
}

func NewRegisteredObserver(address grain.Address, val interface{}) (*EncodedRegisteredObserver, error) {
	// TODO: should not need to marshal
	data, err := proto.Marshal(val.(proto.Message))
	if err != nil {
		return nil, err
	}
	return &EncodedRegisteredObserver{
		Address: address,
		val:     data,
	}, nil
}

func (o *EncodedRegisteredObserver) Get(v interface{}) error {
	return proto.Unmarshal(o.val, v.(proto.Message))
}

type InmemoryObserverManager struct {
	registeredObservers map[string][]grain.RegisteredObserver
	siloClient          grain.SiloClient
	owner               grain.Address
}

func NewInmemoryObserverManager(owner grain.Address, siloClient grain.SiloClient) *InmemoryObserverManager {
	return &InmemoryObserverManager{
		owner:               owner,
		siloClient:          siloClient,
		registeredObservers: make(map[string][]grain.RegisteredObserver),
	}
}

func (m *InmemoryObserverManager) List(ctx context.Context, observableName string) ([]grain.RegisteredObserver, error) {
	return m.registeredObservers[observableName], nil
}

func (m *InmemoryObserverManager) Add(ctx context.Context, observableName string, address grain.Address, val interface{}) (grain.RegisteredObserver, error) {
	o, err := NewRegisteredObserver(address, val)
	if err != nil {
		return nil, err
	}
	observables := m.registeredObservers[observableName]
	for i := range observables {
		if observables[i].GetAddress() == address {
			observables[i] = o

			return observables[i], nil
		}
	}
	m.registeredObservers[observableName] = append(m.registeredObservers[observableName], o)
	return o, nil
}

func (m *InmemoryObserverManager) Notify(ctx context.Context, observableName string, observers []grain.RegisteredObserver, val proto.Message) error {
	fmt.Printf("notifying %d observers\n", len(observers))
	receivers := make([]grain.Address, len(observers))
	for i := range observers {
		receivers[i] = observers[i].GetAddress()
	}
	return m.siloClient.NotifyObservers(ctx, m.owner.GrainType, observableName, receivers, val)
}
