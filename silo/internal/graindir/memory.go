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

type inmemoryGrainDirectoryLock struct {
	l cluster.Location
	d *InmemoryGrainDirectory
}

func (m *InmemoryGrainDirectory) Lock(ctx context.Context, l cluster.Location,
	f cluster.GrainDirectoryLockOnExpirationFunc) (cluster.GrainDirectoryLock, error) {
	return &inmemoryGrainDirectoryLock{
		l: l,
		d: m,
	}, nil
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

func (m *InmemoryGrainDirectory) activate(ctx context.Context, addr cluster.GrainAddress) error {
	if addr.Location != m.local {
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.localGrains[addr.Identity] = struct{}{}

	return nil
}

func (m *InmemoryGrainDirectory) deactivate(ctx context.Context, addr cluster.GrainAddress) error {
	if addr.Location != m.local {
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.localGrains, addr.Identity)

	return nil
}

func (l *inmemoryGrainDirectoryLock) Activate(ctx context.Context, ident grain.Identity) error {
	return l.d.activate(ctx, cluster.GrainAddress{
		Location: l.l,
		Identity: ident,
	})
}

func (l *inmemoryGrainDirectoryLock) Deactivate(ctx context.Context, ident grain.Identity) error {
	return l.d.deactivate(ctx, cluster.GrainAddress{
		Location: l.l,
		Identity: ident,
	})
}

func (l *inmemoryGrainDirectoryLock) Lookup(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	return l.d.Lookup(ctx, ident)
}

func (l *inmemoryGrainDirectoryLock) Unlock(context.Context) error {
	return nil
}

func (l *inmemoryGrainDirectoryLock) Err() error {
	return nil
}
