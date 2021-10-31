package cluster

import "context"

type DiscoveryDelegate interface {
	NotifyDiscovered([]Node)
}

type DiscoveryDelegateCallbacks struct {
	NotifyDiscoveredFunc func([]Node)
}

func (cb *DiscoveryDelegateCallbacks) NotifyDiscovered(nodes []Node) {
	if cb.NotifyDiscoveredFunc != nil {
		cb.NotifyDiscoveredFunc(nodes)
	}
}

type Discovery interface {
	Watch(context.Context, DiscoveryDelegate) error
	Stop(context.Context) error
}
