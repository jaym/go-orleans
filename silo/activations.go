package silo

import (
	"context"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/silo/internal/activation"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type ActivateGrainRequest struct {
	Identity  grain.Identity
	Activator GenericGrainActivator
}

type EvictGrainRequest struct {
	Identity grain.Identity
}

type InvokeMethodRequest struct {
	Sender      grain.Identity
	Receiver    grain.Identity
	Method      string
	in          []byte
	ResolveFunc func(interface{}, error)
}

type RegisterObserverRequest struct {
	Observer    grain.Identity
	Observable  grain.Identity
	Name        string
	UUID        string
	In          []byte
	ResolveFunc func(error)
}

type UnsubscribeObserverRequest struct {
	Observer    grain.Identity
	Observable  grain.Identity
	Name        string
	UUID        string
	ResolveFunc func(error)
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
	log                 logr.Logger
	registrar           descriptor.Registrar
	siloClient          grain.SiloClient
	timerService        timer.TimerService
	observerStore       observer.Store
	grainActivations    map[grain.Identity]*activation.LocalGrainActivation
	localGrainActivator *activation.LocalGrainActivator
	grainDirectory      cluster.GrainDirectory
	grainDirectoryLock  cluster.GrainDirectoryLock
	resourceManager     *activation.ResourceManager
	nodeName            cluster.Location
	serving             bool
	wg                  sync.WaitGroup
}

func NewGrainActivationManager(
	log logr.Logger,
	registrar descriptor.Registrar,
	nodeName cluster.Location,
	siloClient grain.SiloClient,
	timerService timer.TimerService,
	observerStore observer.Store,
	grainDirectory cluster.GrainDirectory,
) *GrainActivationManagerImpl {
	m := &GrainActivationManagerImpl{
		log:              log,
		registrar:        registrar,
		siloClient:       siloClient,
		timerService:     timerService,
		observerStore:    observerStore,
		nodeName:         nodeName,
		grainActivations: make(map[grain.Identity]*activation.LocalGrainActivation),
		grainDirectory:   grainDirectory,
	}
	m.resourceManager = activation.NewResourceManager(8, func(identityes []grain.Identity) {
		for _, a := range identityes {
			m.EnqueueEvictGrain(EvictGrainRequest{
				Identity: a,
			})
		}
	})

	return m
}

func (m *GrainActivationManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	grainDirLock, err := m.grainDirectory.Lock(ctx, m.nodeName, func() {
		panic("grain directory lock lost")
	})
	if err != nil {
		return err
	}
	m.grainDirectoryLock = grainDirLock
	deactivateCallback := func(a grain.Identity) {
		defer m.wg.Done()
		if err := m.grainDirectoryLock.Deactivate(context.TODO(), a); err != nil {
			m.log.Error(err, "failed to deactivate grain", "grain", a)
		}
		m.resourceManager.Remove(a)
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m.grainActivations, a)
	}

	m.localGrainActivator = activation.NewLocalGrainActivator(
		m.registrar,
		m.siloClient,
		m.timerService,
		m.resourceManager,
		m.observerStore,
		deactivateCallback)
	m.resourceManager.Start()
	m.timerService.Start()
	m.serving = true

	return nil
}

func (m *GrainActivationManagerImpl) EnqueueInvokeMethodRequest(req InvokeMethodRequest) error {
	activation, err := m.getActivation(req.Receiver, true)
	if err != nil {
		return err
	}

	return activation.InvokeMethod(req.Sender, req.Method, req.in, req.ResolveFunc)
}

func (m *GrainActivationManagerImpl) EnqueueRegisterObserverRequest(req RegisterObserverRequest) error {
	activation, err := m.getActivation(req.Observable, true)
	if err != nil {
		return err
	}

	return activation.RegisterObserver(req.Observer, req.Name, req.In, req.ResolveFunc)
}

func (m *GrainActivationManagerImpl) EnqueueUnsubscribeObserverRequest(req UnsubscribeObserverRequest) error {
	activation, err := m.getActivation(req.Observable, true)
	if err != nil {
		return err
	}

	return activation.UnsubscribeObserver(req.Observer, req.Name, req.ResolveFunc)
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

func (m *GrainActivationManagerImpl) Stop(ctx context.Context) error {
	var err error

	// TODO: this locking is messy. Figure out what actually should be locked
	// while trying to stop. Or find an easier way to do this.
	m.lock.Lock()
	m.serving = false
	m.lock.Unlock()

	if errTimer := m.timerService.Stop(ctx); errTimer != nil {
		err = multierror.Append(err, errTimer)
	}

	m.lock.Lock()
	for _, g := range m.grainActivations {
		if err := g.Stop(); err != nil {
			m.log.Error(err, "failed to stop grain", "grain", g.Name())
		}
	}
	m.lock.Unlock()

	// TODO: find a better pattern. In the error case, this leaks
	// a goroutine that will forever wait and potentially hide a bug
	doneChan := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(doneChan)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneChan:
	}

	if errUnlock := m.grainDirectoryLock.Unlock(ctx); errUnlock != nil {
		err = multierror.Append(err, errUnlock)
	}

	return err
}

func (m *GrainActivationManagerImpl) getActivation(receiver grain.Identity, allowActivation bool) (*activation.LocalGrainActivation, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.serving {
		return nil, ErrUnavailable
	}
	a, ok := m.grainActivations[receiver]
	if !ok {
		if allowActivation {
			var err error
			// TODO: doing an RPC while holding the lock sucks. Maybe there's a better way to do this
			if err = m.grainDirectoryLock.Activate(context.TODO(), receiver); err != nil {
				return nil, err
			}
			m.wg.Add(1)
			a, err = m.localGrainActivator.ActivateGrainWithDefaultActivator(receiver)
			if err != nil {
				m.wg.Done()
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
var ErrUnavailable = errors.New("unavailable")
