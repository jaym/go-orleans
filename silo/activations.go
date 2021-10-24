package silo

import (
	"errors"
	"strings"
	"sync"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/silo/internal/activation"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type GrainActivationManager interface {
	ActivateGrain(ActivateGrainRequest) error
	EnqueueInvokeMethodRequest(InvokeMethodRequest) error
	EnqueueRegisterObserverRequest(RegisterObserverRequest) error
	EnqueueObserverNotification(ObserverNotification) error
	EnqueueTimerTrigger(TimerTriggerNotification) error
	EnqueueEvictGrain(EvictGrainRequest) error
}

type ActivateGrainRequest struct {
	Identity  grain.Identity
	Activator GenericGrainActivator
}

type EvictGrainRequest struct {
	Identity grain.Identity
}

type InvokeMethodRequest struct {
	Sender   grain.Identity
	Receiver grain.Identity
	Method   string
	UUID     string
	in       []byte
}

type RegisterObserverRequest struct {
	Observer   grain.Identity
	Observable grain.Identity
	Name       string
	UUID       string
	In         []byte
}

type ObserverNotification struct {
	Sender         grain.Identity
	Receivers      []grain.Identity
	ObservableType string
	Name           string
	UUID           string
	In             []byte
}

type TimerTriggerNotification struct {
	Receiver grain.Identity
	Name     string
}

type GrainActivationManagerImpl struct {
	lock                sync.Mutex
	grainActivations    map[grain.Identity]*activation.LocalGrainActivation
	localGrainActivator *activation.LocalGrainActivator
}

func NewGrainActivationManager(registrar descriptor.Registrar,
	siloClient grain.SiloClient,
	timerService timer.TimerService,
	observerStore observer.Store,
) *GrainActivationManagerImpl {
	m := &GrainActivationManagerImpl{
		grainActivations: make(map[grain.Identity]*activation.LocalGrainActivation),
	}
	resourceManager := activation.NewResourceManager(8, func(identityes []grain.Identity) {
		for _, a := range identityes {
			m.EnqueueEvictGrain(EvictGrainRequest{
				Identity: a,
			})
		}
	})
	resourceManager.Start()
	deactivateCallback := func(a grain.Identity) {
		resourceManager.Remove(a)
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m.grainActivations, a)
	}

	m.localGrainActivator = activation.NewLocalGrainActivator(
		registrar,
		siloClient,
		timerService,
		resourceManager,
		observerStore,
		deactivateCallback)
	return m
}

func (m *GrainActivationManagerImpl) EnqueueInvokeMethodRequest(req InvokeMethodRequest) error {
	activation, err := m.getActivation(req.Receiver, true)
	if err != nil {
		return err
	}

	return activation.InvokeMethod(req.Sender, req.Method, req.UUID, req.in)
}

func (m *GrainActivationManagerImpl) EnqueueRegisterObserverRequest(req RegisterObserverRequest) error {
	activation, err := m.getActivation(req.Observable, true)
	if err != nil {
		return err
	}

	return activation.RegisterObserver(req.Observer, req.Name, req.UUID, req.In)
}

func (m *GrainActivationManagerImpl) EnqueueObserverNotification(req ObserverNotification) error {
	var errs []string
	for _, receiver := range req.Receivers {
		activation, err := m.getActivation(receiver, true)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		err = activation.NotifyObservable(req.Sender, req.ObservableType, req.Name, req.In)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, " : "))
	}

	return nil
}

func (m *GrainActivationManagerImpl) ActivateGrain(req ActivateGrainRequest) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok := m.grainActivations[req.Identity]
	if !ok {
		var a *activation.LocalGrainActivation
		var err error
		if req.Activator == nil {
			a, err = m.localGrainActivator.ActivateGrainWithDefaultActivator(req.Identity)
		} else {
			a, err = m.localGrainActivator.ActivateGrainWithActivator(req.Identity, req.Activator)
		}
		if err != nil {
			return err
		}
		m.grainActivations[req.Identity] = a
	}
	return nil
}

func (m *GrainActivationManagerImpl) EnqueueTimerTrigger(req TimerTriggerNotification) error {
	activation, err := m.getActivation(req.Receiver, false)
	if err != nil {
		return err
	}
	return activation.NotifyTimer(req.Name)
}

func (m *GrainActivationManagerImpl) EnqueueEvictGrain(req EvictGrainRequest) error {
	activation, err := m.getActivation(req.Identity, false)
	if err != nil {
		if errors.Is(err, ErrGrainActivationNotFound) {
			return nil
		}
		return err
	}
	return activation.Evict()
}

func (m *GrainActivationManagerImpl) getActivation(receiver grain.Identity, allowActivation bool) (*activation.LocalGrainActivation, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	a, ok := m.grainActivations[receiver]
	if !ok {
		if allowActivation {
			var err error
			a, err = m.localGrainActivator.ActivateGrainWithDefaultActivator(receiver)
			if err != nil {
				return nil, err
			}
			m.grainActivations[receiver] = a
		} else {
			return nil, ErrGrainActivationNotFound
		}
	}
	return a, nil
}

var ErrInboxFull = errors.New("inbox full")
var ErrGrainActivationNotFound = errors.New("grain activation not found")
