package silo

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/generic"
	"github.com/jaym/go-orleans/silo/internal/activation"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type TimerTriggerNotification struct {
	Receiver grain.Identity
	Name     string
}

type GrainActivationManagerImpl struct {
	lock                sync.Mutex
	log                 logr.Logger
	registrar           descriptor.Registrar
	siloClient          grain.SiloClient
	grainActivations    map[grain.Identity]activation.Activation
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
	grainDirectory cluster.GrainDirectory,
	maxGrains int,
) *GrainActivationManagerImpl {
	m := &GrainActivationManagerImpl{
		log:              log,
		registrar:        registrar,
		siloClient:       siloClient,
		nodeName:         nodeName,
		grainActivations: make(map[grain.Identity]activation.Activation),
		grainDirectory:   grainDirectory,
	}
	m.resourceManager = activation.NewResourceManager(maxGrains, m.EnqueueEvictGrain)

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
		if err := m.resourceManager.Remove(a); err != nil {
			m.log.Error(err, "failed to remove grain from resource manager", "grain", a)
		}
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m.grainActivations, a)
	}

	m.localGrainActivator = activation.NewLocalGrainActivator(
		m.log,
		m.registrar,
		m.siloClient,
		m.resourceManager,
		16,
		deactivateCallback)
	m.resourceManager.Start()
	m.serving = true

	return nil
}

func (m *GrainActivationManagerImpl) EnqueueInvokeMethodRequest(sender grain.Identity,
	receiver grain.Identity, methodName string, deadline time.Time,
	dec grain.Deserializer, ser grain.Serializer, resolve func(error)) error {
	activation, err := m.getActivation(receiver, true)
	if err != nil {
		return err
	}

	return activation.InvokeMethod(sender, methodName, deadline, dec, ser, resolve)
}

func (m *GrainActivationManagerImpl) EnqueueInvokeOneWayMethodRequest(sender grain.Identity,
	receivers []grain.Identity, methodName string, dec grain.Deserializer) error {
	for _, receiver := range receivers {
		activation, err := m.getActivation(receiver, true)
		if err != nil {
			if err != ErrGrainActivationNotFound {
				m.log.V(0).Error(err, "failed to get grain activation", "method", methodName,
					"receiver", receiver.String())
			}
			continue
		}
		// TODO:  deadline
		if err := activation.InvokeOneWayMethod(sender, methodName, time.Now().Add(time.Minute), dec.CloneAndReset()); err != nil {
			m.log.V(0).Error(err, "failed invoke one way method", "method", methodName,
				"receiver", receiver.String())
		}
	}

	return nil
}

func (m *GrainActivationManagerImpl) ActivateGenericGrain(g *generic.Grain) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok := m.grainActivations[g.Identity]
	if !ok {
		a, err := m.localGrainActivator.ActivateGenericGrain(g)
		if err != nil {
			return err
		}
		m.wg.Add(1)
		m.grainActivations[g.Identity] = a
	}
	return nil
}

func (m *GrainActivationManagerImpl) EnqueueEvictGrain(ident grain.Identity, onComplete func(error)) {
	activation, err := m.getActivation(ident, false)
	if err != nil {
		if errors.Is(err, ErrGrainActivationNotFound) {
			onComplete(nil)
			return
		}
		onComplete(err)
		return
	}
	activation.EvictAsync(onComplete)
}

func (m *GrainActivationManagerImpl) Stop(ctx context.Context) error {
	var err error

	// TODO: this locking is messy. Figure out what actually should be locked
	// while trying to stop. Or find an easier way to do this.
	m.lock.Lock()
	m.serving = false

	for _, g := range m.grainActivations {
		g.StopAsync(func(error) {})
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

func (m *GrainActivationManagerImpl) getActivation(receiver grain.Identity, allowActivation bool) (activation.Activation, error) {
	if generic.IsGenericGrain(receiver) {
		allowActivation = false
	}

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

var ErrInboxFull = activation.ErrInboxFull
var ErrGrainActivationNotFound = activation.ErrGrainActivationNotFound
var ErrGrainDeactivating = activation.ErrGrainDeactivating
var ErrUnavailable = errors.New("unavailable")
