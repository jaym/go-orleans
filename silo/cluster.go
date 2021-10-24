package silo

import (
	"context"

	"github.com/jaym/go-orleans/silo/services/cluster"
)

type Cluster struct {
}

func NewCluster(ctx context.Context, membershipProtocol cluster.MembershipProtocol) (*Cluster, error) {
	c := &Cluster{}
	if err := membershipProtocol.Join(ctx, nil); err != nil {
		return nil, err
	}
	return c, nil
}
