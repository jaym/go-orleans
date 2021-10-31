package static

import (
	"context"
	"sync"
	"time"

	"github.com/jaym/go-orleans/silo/services/cluster"
)

type StaticList struct {
	closeChan chan struct{}
	wg        sync.WaitGroup
	interval  time.Duration
	nodes     []string
}

func New(nodes []string) *StaticList {
	return &StaticList{
		nodes:     nodes,
		interval:  time.Minute,
		closeChan: make(chan struct{}),
	}
}

func (g *StaticList) Watch(ctx context.Context, d cluster.DiscoveryDelegate) error {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()

		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		d.NotifyDiscovered(g.nodes)
	LOOP:
		for {
			select {
			case <-g.closeChan:
				break LOOP
			case <-ticker.C:
				d.NotifyDiscovered(g.nodes)
			}
		}

	}()
	return nil
}

func (g *StaticList) Stop(context.Context) error {
	close(g.closeChan)
	g.wg.Wait()
	return nil
}
