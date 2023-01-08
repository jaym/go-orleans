package activation

import (
	"errors"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
)

type GrainActivationFunc func(identity grain.Identity, activatorConfig descriptor.ActivatorConfig, deactivateFunc func(grain.Identity)) (Activation, error)
type StatelessGrainActivation struct {
	state              GrainState
	identity           grain.Identity
	maxActivations     int
	newActivationFunc  GrainActivationFunc
	deactivateCallback func(grain.Identity)
	activatorConfig    descriptor.ActivatorConfig
	log                logr.Logger

	lock        sync.Mutex
	activations []Activation
}

func NewStatelessGrainActivation(log logr.Logger, identity grain.Identity, activatorConfig descriptor.ActivatorConfig,
	deactivateCallback func(grain.Identity), f GrainActivationFunc) *StatelessGrainActivation {
	maxWorkers := 16
	if activatorConfig.MaxWorkers != nil {
		maxWorkers = *activatorConfig.MaxWorkers
	}
	return &StatelessGrainActivation{
		identity:           identity,
		maxActivations:     maxWorkers,
		activatorConfig:    activatorConfig,
		newActivationFunc:  f,
		deactivateCallback: deactivateCallback,
		log:                log,
	}
}

func (a *StatelessGrainActivation) InvokeMethod(sender grain.Identity, method string, deadline time.Time, dec grain.Deserializer,
	ser grain.Serializer, resolve func(err error)) error {
	return a.try(func(activation Activation) error {
		return activation.InvokeMethod(sender, method, deadline, dec, ser, resolve)
	})
}

func (a *StatelessGrainActivation) InvokeOneWayMethod(sender grain.Identity, method string, deadline time.Time, dec grain.Deserializer) error {
	return a.try(func(activation Activation) error {
		return activation.InvokeOneWayMethod(sender, method, deadline, dec)
	})
}

func (a *StatelessGrainActivation) EvictAsync(onComplete func(err error)) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.state = grainStateDeactivating
	wg := sync.WaitGroup{}
	wg.Add(len(a.activations))
	for _, activation := range a.activations {
		activation.EvictAsync(func(err error) {
			wg.Done()
		})
	}
	a.activations = a.activations[:0]
	go func() {
		wg.Wait()
		a.lock.Lock()
		a.state = grainStateDeactivating
		a.lock.Unlock()
		a.deactivateCallback(a.identity)
		onComplete(nil)
	}()
}

func (a *StatelessGrainActivation) StopAsync(onComplete func(error)) {
	a.EvictAsync(onComplete)
}

func (a *StatelessGrainActivation) State() GrainState {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.state
}

func (a *StatelessGrainActivation) try(f func(Activation) error) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	success, err := a.visitActivationsInOrder(f)
	if err != nil {
		return err
	}
	if success {
		return nil
	}

	grown := a.grow()
	for i := 0; i < 3; i++ {
		if grown {
			success, err = a.visitActivationsInOrder(f)
			if err != nil {
				return err
			}
			if success {
				return nil
			}

			// BUG: if a grain is activated, it will not be immedietly ready to
			// take accept a message. Because stateless grains usually will have
			// a channel size of 0, its possible, probably likely, that they will
			// not be accepting a message on the channel. Since trying to invoke
			// does not block, the message will be rejected.
			// So, here is a really horrible hack.
			time.Sleep(time.Millisecond)
		}
	}

	return ErrInboxFull
}

func (a *StatelessGrainActivation) visitActivationsInOrder(f func(Activation) error) (bool, error) {
	for i := range a.activations {
		err := f(a.activations[i])
		if errors.Is(err, ErrInboxFull) {
			continue
		} else if err != nil {
			return false, err
		}
		return true, nil

	}
	return false, nil
}

func (a *StatelessGrainActivation) grow() bool {
	numToActivate := len(a.activations)/2 + 1
	maxCanActivate := a.maxActivations - len(a.activations)
	if numToActivate > maxCanActivate {
		numToActivate = maxCanActivate
	}
	if numToActivate <= 0 {
		return false
	}
	idx := len(a.activations)
	for i := 0; i < numToActivate; i++ {
		newActivation, err := a.newActivationFunc(a.identity, a.activatorConfig, func(grain.Identity) { a.cleanupDead() })
		if err != nil {
			a.log.Error(err, "failed to grow stateless grain activation")
			return i > 0
		}
		a.activations = append(a.activations, newActivation)
		idx++
	}
	return true
}

func (a *StatelessGrainActivation) cleanupDead() {
	a.lock.Lock()
	defer a.lock.Unlock()
	i := 0
	for i < len(a.activations) {
		state := a.activations[i].State()
		if state == grainStateDeactivated {
			a.activations[i] = a.activations[len(a.activations)-1]
			a.activations = a.activations[0 : len(a.activations)-1]
		} else {
			i++
		}
	}
}
