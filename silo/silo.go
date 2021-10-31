package silo

import (
	"github.com/go-logr/logr"
	"github.com/segmentio/ksuid"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/silo/internal/graindir"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type Silo struct {
	descriptor.Registrar
	localGrainManager *GrainActivationManagerImpl
	client            *siloClientImpl
	timerService      timer.TimerService
	observerStore     observer.Store
	log               logr.Logger
	nodeName          cluster.Location
	grainDirectory    cluster.GrainDirectory
	transportManager  *transport.Manager
}

func NewSilo(log logr.Logger, observerStore observer.Store) *Silo {
	s := &Silo{
		Registrar: &registrarImpl{
			entries: map[string]registrarEntry{},
		},
		log:           log.WithName("silo"),
		observerStore: observerStore,
	}
	s.nodeName = cluster.Location("mynodename")
	s.grainDirectory = graindir.NewInmemoryGrainDirectory(s.nodeName)
	// TODO: do something a little more sensible. With the generic
	// grain, it is expected to pass in an activator for each
	// activation. Setting it to nil is will cause a panic with
	// no information on what went wrong
	s.Register(&grain_GrainDesc, nil)
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
	s.transportManager.AddTransport(s.nodeName, &localTransport{
		log:               s.log.WithName("local-transport"),
		codec:             s.client.codec,
		localGrainManager: s.localGrainManager,
	})
	s.client.transportManager = s.transportManager

	return s
}

func (s *Silo) Start() error {
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
