package cluster

import "context"

type DiscoveryDelegate interface {
	NotifyDiscovered([]string)
}

type DiscoveryDelegateCallbacks struct {
	NotifyDiscoveredFunc func([]string)
}

func (cb *DiscoveryDelegateCallbacks) NotifyDiscovered(nodes []string) {
	if cb.NotifyDiscoveredFunc != nil {
		cb.NotifyDiscoveredFunc(nodes)
	}
}

type Discovery interface {
	Watch(context.Context, DiscoveryDelegate) error
	Stop(context.Context) error
}
