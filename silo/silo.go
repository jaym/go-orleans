package silo

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/generic"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type Silo struct {
	descriptor.Registrar
	localGrainManager  *GrainActivationManagerImpl
	client             *siloClientImpl
	timerService       timer.TimerService
	log                logr.Logger
	nodeName           cluster.Location
	grainDirectory     cluster.GrainDirectory
	transportManager   *transport.Manager
	discovery          cluster.Discovery
	membershipProtocol cluster.MembershipProtocol
	transportFactory   cluster.TransportFactory
}

func NewSilo(log logr.Logger, opts ...SiloOption) *Silo {
	options := siloOptions{
		maxGrains: 1024,
	}
	for _, o := range opts {
		o(&options)
	}

	s := &Silo{
		Registrar: &registrarImpl{
			entries: map[string]registrarEntry{},
		},
		log:                log.WithName("silo"),
		nodeName:           options.NodeName(),
		grainDirectory:     options.GrainDirectory(),
		discovery:          options.Discovery(),
		membershipProtocol: options.MembershipProtocol(),
		transportFactory:   options.TransportFactory(),
	}
	// TODO: do something a little more sensible. With the generic
	// grain, it is expected to pass in an activator for each
	// activation. Setting it to nil is will cause a panic with
	// no information on what went wrong
	s.client = &siloClientImpl{
		log:            s.log.WithName("siloClient"),
		codec:          protobuf.NewCodec(),
		nodeName:       s.nodeName,
		grainDirectory: s.grainDirectory,
	}
	s.timerService = newTimerServiceImpl(s.log.WithName("timerService"), func(grainAddr grain.Identity, name string) {
		err := s.localGrainManager.EnqueueTimerTrigger(TimerTriggerNotification{
			Receiver: grainAddr,
			Name:     name,
		})
		if err != nil {
			if !(errors.Is(err, ErrGrainActivationNotFound) || errors.Is(err, ErrGrainDeactivating)) {
				s.log.V(1).Error(err, "failed to trigger timer notification", "identity", grainAddr, "triggerName", name)
			}
		}
	})
	s.localGrainManager = NewGrainActivationManager(
		s.log.WithName("activation-manager"),
		s.Registrar,
		s.nodeName,
		s.client,
		s.timerService,
		s.grainDirectory,
		options.maxGrains,
	)

	handler := siloTransportHandler{
		log:               s.log.WithName("transport-handler"),
		codec:             s.client.codec,
		localGrainManager: s.localGrainManager,
	}
	s.transportManager = transport.NewManager(s.log.WithName("transport-manager"), handler)
	s.client.transportManager = s.transportManager
	return s
}

func (s *Silo) Start(ctx context.Context) error {
	joinLock := sync.Mutex{}
	err := s.membershipProtocol.Start(context.Background(), &cluster.MembershipProtocolCallbacks{
		NotifyJoinFunc: func(n cluster.Node) {
			joinLock.Lock()
			defer joinLock.Unlock()

			s.log.V(0).Info("node joined", "node", n)
			// TODO: create transport
			if err := s.transportManager.AddTransport(n.Name, func() (cluster.Transport, error) {
				return s.transportFactory.CreateTransport(n)
			}); err != nil {
				s.log.V(0).Error(err, "failed to join node", "node", n)
			}

		},
		NotifyLeaveFunc: func(n cluster.Node) {
			joinLock.Lock()
			defer joinLock.Unlock()
			s.log.V(0).Info("node left", "node", n)
			s.transportManager.RemoveTransport(n.Name)
		},
	})
	if err != nil {
		return err
	}
	err = s.discovery.Watch(context.Background(), &cluster.DiscoveryDelegateCallbacks{
		NotifyDiscoveredFunc: func(nodes []string) {
			s.log.V(4).Info("discovered nodes", "nodes", nodes)
			if err := s.membershipProtocol.Join(context.Background(), nodes); err != nil {
				s.log.V(0).Error(err, "failed to join discovered nodes")
			}
		},
	})
	if err != nil {
		return err
	}

	err = s.transportManager.AddTransport(s.nodeName, func() (cluster.Transport, error) {
		return &localTransport{
			log:               s.log.WithName("local-transport"),
			codec:             s.client.codec,
			localGrainManager: s.localGrainManager,
		}, nil
	})
	if err != nil {
		return err
	}

	return s.localGrainManager.Start(ctx)
}

func (s *Silo) Client() grain.SiloClient {
	return s.client
}

func (s *Silo) CreateGrain() (*generic.Grain, error) {
	g := generic.NewGrain(string(s.nodeName), s.client)

	err := s.localGrainManager.ActivateGenericGrain(g)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Silo) DestroyGrain(g *generic.Grain) error {
	errChan := make(chan error, 1)
	s.localGrainManager.EnqueueEvictGrain(g.Identity, func(e error) { errChan <- e })
	return <-errChan
}

func (s *Silo) Stop(ctx context.Context) error {
	var err error

	// Stop discovering nodes. We're trying to leave the cluster so
	// finding more nodes is not worthwhile
	if errDiscovery := s.discovery.Stop(ctx); errDiscovery != nil {
		err = multierror.Append(err, errDiscovery)
	}

	// TODO: Set silo state to stopping. This is so other nodes don't
	// consider placing grains in this silo while its being shut down

	// Stop grain manager
	if errGrainDir := s.localGrainManager.Stop(ctx); errGrainDir != nil {
		err = multierror.Append(err, errGrainDir)
	}

	// TODO: stop grain directory
	if errLeave := s.membershipProtocol.Leave(ctx); errLeave != nil {
		err = multierror.Append(err, errLeave)
	}
	return err
}
