package silo

import (
	"os"

	"github.com/jaym/go-orleans/silo/internal/graindir"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type siloOptions struct {
	nodeName           cluster.Location
	grainDirectory     cluster.GrainDirectory
	membershipProtocol cluster.MembershipProtocol
	transportFactory   cluster.TransportFactory
	discovery          cluster.Discovery
	maxGrains          int
}

func (so *siloOptions) NodeName() cluster.Location {
	if string(so.nodeName) == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "node"
		}
		return cluster.Location(hostname)
	}
	return so.nodeName
}

func (so *siloOptions) GrainDirectory() cluster.GrainDirectory {
	if so.grainDirectory == nil {
		return graindir.NewInmemoryGrainDirectory(so.NodeName())
	}
	return so.grainDirectory
}

func (so *siloOptions) Discovery() cluster.Discovery {
	if so.discovery == nil {
		return noopDiscovery{}
	}
	return so.discovery
}

func (so *siloOptions) MembershipProtocol() cluster.MembershipProtocol {
	if so.membershipProtocol == nil {
		return noopMembershipProtocol{}
	}
	return so.membershipProtocol
}

func (so *siloOptions) TransportFactory() cluster.TransportFactory {
	return so.transportFactory
}

type SiloOption func(*siloOptions)

func WithNodeName(s string) SiloOption {
	return func(so *siloOptions) {
		so.nodeName = cluster.Location(s)
	}
}

func WithGrainDirectory(d cluster.GrainDirectory) SiloOption {
	return func(so *siloOptions) {
		so.grainDirectory = d
	}
}

func WithMembership(m cluster.MembershipProtocol, transportFactory cluster.TransportFactory) SiloOption {
	return func(so *siloOptions) {
		so.membershipProtocol = m
		so.transportFactory = transportFactory
	}
}

func WithDiscovery(d cluster.Discovery) SiloOption {
	return func(so *siloOptions) {
		so.discovery = d
	}
}

func WithMaxGrains(maxGrains int) SiloOption {
	return func(so *siloOptions) {
		so.maxGrains = maxGrains
	}
}
