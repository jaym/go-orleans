package silo

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/ksuid"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type Silo struct {
	descriptor.Registrar
	localGrainManager  *GrainActivationManagerImpl
	client             *siloClientImpl
	timerService       timer.TimerService
	observerStore      observer.Store
	log                logr.Logger
	nodeName           cluster.Location
	grainDirectory     cluster.GrainDirectory
	transportManager   *transport.Manager
	discovery          cluster.Discovery
	membershipProtocol cluster.MembershipProtocol
	transportFactory   cluster.TransportFactory
}

func NewSilo(log logr.Logger, observerStore observer.Store, opts ...SiloOption) *Silo {
	options := siloOptions{}
	for _, o := range opts {
		o(&options)
	}

	s := &Silo{
		Registrar: &registrarImpl{
			entries: map[string]registrarEntry{},
		},
		log:                log.WithName("silo"),
		observerStore:      observerStore,
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
			s.log.V(1).Error(err, "failed to trigger timer notification", "identity", grainAddr, "triggerName", name)
		}
	})
	s.localGrainManager = NewGrainActivationManager(
		s.log.WithName("activation-manager"),
		s.Registrar,
		s.nodeName,
		s.client,
		s.timerService,
		s.observerStore,
		s.grainDirectory)

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

	s.Register(&grain_GrainDesc, nil)

	return s.localGrainManager.Start(ctx)
}

func (s *Silo) Client() grain.SiloClient {
	return s.client
}

func (s *Silo) CreateGrain(activator GenericGrainActivator) (grain.Identity, error) {
	identity := grain.Identity{
		GrainType: "Grain",
		ID:        ksuid.New().String(),
	}
	err := s.localGrainManager.ActivateGrain(ActivateGrainRequest{
		Identity:  identity,
		Activator: activator,
	})
	return identity, err
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
