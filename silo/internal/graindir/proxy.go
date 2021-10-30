package graindir

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type ProxiedGrainDirectory struct {
	local    *InmemoryGrainDirectory
	upstream cluster.GrainDirectory
}

func NewProxiedGrainDirectory(local *InmemoryGrainDirectory, upstream cluster.GrainDirectory) *ProxiedGrainDirectory {
	return &ProxiedGrainDirectory{
		local:    local,
		upstream: upstream,
	}
}

func (m *ProxiedGrainDirectory) Lookup(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	g, err := m.local.Lookup(ctx, ident)
	if err != nil {
		if err != cluster.ErrGrainActivationNotFound {
			return cluster.GrainAddress{}, err
		}
	} else {
		return g, nil
	}

	return m.upstream.Lookup(ctx, ident)
}

func (m *ProxiedGrainDirectory) Activate(ctx context.Context, addr cluster.GrainAddress) error {
	if err := m.upstream.Activate(ctx, addr); err != nil {
		return err
	}

	if err := m.local.Activate(ctx, addr); err != nil {
		panic(err)
	}
	return nil
}

func (m *ProxiedGrainDirectory) Deactivate(ctx context.Context, addr cluster.GrainAddress) error {
	if err := m.upstream.Deactivate(ctx, addr); err != nil {
		return err
	}

	if err := m.local.Deactivate(ctx, addr); err != nil {
		panic(err)
	}
	return nil
}
