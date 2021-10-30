package graindir

import (
	"context"
	"sync"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type InmemoryGrainDirectory struct {
	lock        sync.RWMutex
	local       cluster.Location
	localGrains map[grain.Identity]struct{}
}

func NewInmemoryGrainDirectory(local cluster.Location) *InmemoryGrainDirectory {
	return &InmemoryGrainDirectory{
		local:       local,
		localGrains: make(map[grain.Identity]struct{}),
	}
}

func (m *InmemoryGrainDirectory) Lookup(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if _, ok := m.localGrains[ident]; ok {
		return cluster.GrainAddress{
			Location: m.local,
			Identity: ident,
		}, nil
	}

	return cluster.GrainAddress{}, cluster.ErrGrainActivationNotFound
}

func (m *InmemoryGrainDirectory) Activate(ctx context.Context, addr cluster.GrainAddress) error {
	if addr.Location != m.local {
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.localGrains[addr.Identity] = struct{}{}

	return nil
}

func (m *InmemoryGrainDirectory) Deactivate(ctx context.Context, addr cluster.GrainAddress) error {
	if addr.Location != m.local {
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.localGrains, addr.Identity)

	return nil
}
