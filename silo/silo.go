package silo

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
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
	s.localGrainManager = NewGrainActivationManager(s.Registrar, s.client, s.timerService, s.observerStore, s.grainDirectory)

	handler := siloTransportHandler{
		log:               s.log.WithName("transport-handler"),
		codec:             s.client.codec,
		localGrainManager: s.localGrainManager,
	}
	s.transportManager = transport.NewManager(s.log.WithName("transport-manager"), handler)
	s.client.transportManager = s.transportManager

	return s
}

func (s *Silo) Start() error {
	joinLock := sync.Mutex{}
	err := s.membershipProtocol.Start(context.Background(), &cluster.MembershipProtocolCallbacks{
		NotifyJoinFunc: func(n cluster.Node) {
			joinLock.Lock()
			defer joinLock.Unlock()
			if s.transportManager.IsMember(n.Name) {
				s.log.V(0).Info("node joined", "node", n)
				// TODO: create transport
				//if err := s.transportManager.AddTransport(n.Name, nil); err != nil {
				//s.log.V(0).Error(err, "failed to join node", "node", n)
				//}
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

	err = s.transportManager.AddTransport(s.nodeName, &localTransport{
		log:               s.log.WithName("local-transport"),
		codec:             s.client.codec,
		localGrainManager: s.localGrainManager,
	})
	if err != nil {
		return err
	}

	s.Register(&grain_GrainDesc, nil)

	s.timerService.Start()
	s.localGrainManager.Start()
	return nil
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
