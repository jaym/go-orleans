package silo

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type noopDiscovery struct {
}

func (noopDiscovery) Watch(ctx context.Context, d cluster.DiscoveryDelegate) error {
	return nil
}

func (noopDiscovery) Stop(ctx context.Context) error {
	return nil
}

type noopMembershipProtocol struct {
}

func (noopMembershipProtocol) Start(context.Context, cluster.MembershipDelegate) error {
	return nil
}

func (noopMembershipProtocol) Stop(context.Context) error {
	return nil
}

func (noopMembershipProtocol) Join(context.Context, cluster.Node) error {
	return errors.New("unimplemented")
}

func (noopMembershipProtocol) Leave(context.Context) error {
	return nil
}

func (noopMembershipProtocol) ListMembers() ([]cluster.Node, error) {
	return []cluster.Node{}, nil
}
