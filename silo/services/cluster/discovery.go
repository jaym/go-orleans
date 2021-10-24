package cluster

import "context"

type DiscoveryDelegate interface {
	NotifyDiscovered([]string)
}

type Discovery interface {
	Watch(context.Context, DiscoveryDelegate) error
	Stop(context.Context) error
}
