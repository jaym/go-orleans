package activation

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	gcontext "github.com/jaym/go-orleans/context"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/generic"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type grainActivationMessageType int

const (
	invokeMethod grainActivationMessageType = iota + 1
	registerObserver
	unsubscribeObserver
	notifyObserver
	triggerTimer
)

type grainActivationInvokeMethod struct {
	Sender      grain.Identity
	Method      string
	Payload     []byte
	ResolveFunc func(out interface{}, err error)
}

type grainActivationRegisterObserver struct {
	Observer            grain.Identity
	Name                string
	Payload             []byte
	RegistrationTimeout time.Duration
	ResolveFunc         func(err error)
}

type grainActivationUnsubscribeObserver struct {
	Observer    grain.Identity
	Name        string
	ResolveFunc func(err error)
}

type grainActivationTriggerTimer struct {
	Name string
}

type grainActivationNotifyObserver struct {
	Sender         grain.Identity
	ObservableType string
	Name           string
	UUID           string
	Payload        []byte
}

type grainActivationMessage struct {
	messageType         grainActivationMessageType
	invokeMethod        *grainActivationInvokeMethod
	registerObserver    *grainActivationRegisterObserver
	unsubscribeObserver *grainActivationUnsubscribeObserver
	notifyObserver      *grainActivationNotifyObserver
	triggerTimer        *grainActivationTriggerTimer
}

type grainActivationEvict struct {
	mustStop   bool
	onComplete func(error)
}

type grainState int

const (
	grainStateRun grainState = iota
	grainStateDeactivating
	grainStateDeactivated
)

type LocalGrainActivation struct {
	lock       sync.RWMutex
	grainState grainState
	evictChan  chan grainActivationEvict
	inbox      chan grainActivationMessage

	identity          grain.Identity
	description       *descriptor.GrainDescription
	activator         interface{}
	grainTimerService *grainTimerServiceImpl
	grainActivator    *LocalGrainActivator
}

type LocalGrainActivator struct {
	registrar          descriptor.Registrar
	siloClient         grain.SiloClient
	timerService       timer.TimerService
	resourceManager    *ResourceManager
	deactivateCallback func(grain.Identity)
}

func NewLocalGrainActivator(registrar descriptor.Registrar, siloClient grain.SiloClient, timerService timer.TimerService, resourceManager *ResourceManager, deactivateCallback func(grain.Identity)) *LocalGrainActivator {
	return &LocalGrainActivator{
		registrar:          registrar,
		siloClient:         siloClient,
		timerService:       timerService,
		resourceManager:    resourceManager,
		deactivateCallback: deactivateCallback,
	}
}

func (m *LocalGrainActivator) ActivateGrainWithDefaultActivator(identity grain.Identity) (*LocalGrainActivation, error) {
	grainDesc, activator, err := m.registrar.Lookup(identity.GrainType)
	if err != nil {
		return nil, err
	}

	return m.activateGrain(identity, grainDesc, activator)
}

func (m *LocalGrainActivator) ActivateGenericGrain(g *generic.Grain) (*LocalGrainActivation, error) {
	grainDesc := &generic.Descriptor
	return m.activateGrain(g.Identity, grainDesc, g)
}

func (m *LocalGrainActivator) activateGrain(identity grain.Identity, grainDesc *descriptor.GrainDescription, activator interface{}) (*LocalGrainActivation, error) {
	l := &LocalGrainActivation{
		identity:       identity,
		description:    grainDesc,
		activator:      activator,
		inbox:          make(chan grainActivationMessage, 8),
		evictChan:      make(chan grainActivationEvict, 1),
		grainActivator: m,
	}
	l.start()
	return l, nil
}

func (g *LocalGrainActivation) findMethodDesc(name string) (*descriptor.MethodDesc, error) {
	for i := range g.description.Methods {
		if g.description.Methods[i].Name == name {
			return &g.description.Methods[i], nil
		}
	}
	return nil, errors.New("method not found")
}

func (g *LocalGrainActivation) findObserableDesc(grainType, name string) (*descriptor.ObservableDesc, error) {
	var desc *descriptor.GrainDescription
	if grainType == "" {
		desc = g.description
	} else {
		var err error
		desc, _, err = g.grainActivator.registrar.Lookup(grainType)
		if err != nil {
			return nil, err
		}
	}
	for i := range desc.Observables {
		if desc.Observables[i].Name == name {
			return &desc.Observables[i], nil
		}
	}
	return nil, errors.New("observable not found")
}

func (l *LocalGrainActivation) start() {
	ctx := gcontext.WithIdentityContext(context.Background(), l.identity)

	pprof.Do(
		ctx,
		pprof.Labels("grain", l.identity.GrainType, "id", l.identity.ID),
		func(ctx context.Context) {
			go l.loop(ctx)
		},
	)
}

func (l *LocalGrainActivation) loop(ctx context.Context) {
	l.grainTimerService = &grainTimerServiceImpl{
		grainIdentity: l.identity,
		timerService:  l.grainActivator.timerService,
		timers:        map[string]func(){},
	}

	coreServices := &coreGrainService{
		grainTimerServices: l.grainTimerService,
		siloClient:         l.grainActivator.siloClient,
	}

	if err := l.grainActivator.resourceManager.Touch(l.identity); err != nil {
		l.setStateDeactivating()
		l.shutdown(ctx, ErrNoCapacity)
		return
	}

	activation, err := l.description.Activation.Handler(l.activator, ctx, coreServices, l.identity)
	if err != nil {
		l.setStateDeactivating()
		l.shutdown(ctx, err)
		return
	}
LOOP:
	for {
		select {
		case req := <-l.evictChan:
			if l.evict(ctx, activation, req.mustStop, req.onComplete) {
				break LOOP
			}
		default:
		}

		select {
		case req := <-l.evictChan:
			if l.evict(ctx, activation, req.mustStop, req.onComplete) {
				break LOOP
			}
		case msg := <-l.inbox:
			l.processMessage(ctx, activation, msg)
		}
	}
}

func (l *LocalGrainActivation) setStateDeactivating() {
	l.lock.Lock()
	l.grainState = grainStateDeactivating
	close(l.inbox)
	close(l.evictChan)
	l.lock.Unlock()
}

func (l *LocalGrainActivation) setStateDeactivated() {
	l.lock.Lock()
	l.grainState = grainStateDeactivated
	l.lock.Unlock()
}

func (l *LocalGrainActivation) evict(ctx context.Context, activation grain.GrainReference, mustStop bool, onComplete func(error)) bool {
	canEvict := true
	if hasCanEvict, ok := activation.(HasCanEvict); ok {
		canEvict = hasCanEvict.CanEvict(ctx)
	}
	if canEvict || mustStop {
		l.setStateDeactivating()
		if hasDeactivate, ok := activation.(HasDeactivate); ok {
			hasDeactivate.Deactivate(ctx)
		}
		l.shutdown(ctx, ErrGrainDeactivating)
		onComplete(nil)
		return true
	}
	onComplete(ErrGrainDeactivationRefused)
	return false
}

func (l *LocalGrainActivation) processMessage(ctx context.Context, activation grain.GrainReference, msg grainActivationMessage) {
	switch msg.messageType {
	case invokeMethod:
		l.grainActivator.resourceManager.Touch(l.identity)
		req := msg.invokeMethod
		m, err := l.findMethodDesc(req.Method)
		if err != nil {
			req.ResolveFunc(nil, err)
			return
		}
		resp, err := m.Handler(activation, ctx, func(in interface{}) error {
			return proto.Unmarshal(req.Payload, in.(proto.Message))
		})
		req.ResolveFunc(resp, err)
	case registerObserver:
		l.grainActivator.resourceManager.Touch(l.identity)

		req := msg.registerObserver
		o, err := l.findObserableDesc("", req.Name)
		if err != nil {
			req.ResolveFunc(err)
			return
		}
		err = o.RegisterHandler(activation, ctx, req.Observer, req.RegistrationTimeout, func(in interface{}) error {
			return proto.Unmarshal(req.Payload, in.(proto.Message))
		})
		req.ResolveFunc(err)
	case unsubscribeObserver:
		req := msg.unsubscribeObserver
		o, err := l.findObserableDesc("", req.Name)
		if err != nil {
			req.ResolveFunc(err)
			return
		}
		err = o.UnsubscribeHandler(activation, ctx, req.Observer)
		req.ResolveFunc(err)
	case notifyObserver:
		l.grainActivator.resourceManager.Touch(l.identity)

		req := msg.notifyObserver
		o, err := l.findObserableDesc(req.ObservableType, req.Name)
		if err != nil {
			// TODO: logger
			fmt.Printf("err: %v\n", err)
		}

		decoder := func(in interface{}) error {
			return proto.Unmarshal(req.Payload, in.(proto.Message))
		}

		if genericGrain, ok := activation.(*generic.Grain); ok {
			genericGrain.HandleNotification(req.ObservableType, req.Name, req.Sender, decoder)
		} else {
			err = o.Handler(activation, ctx, decoder)
			if err != nil {
				fmt.Printf("err: %v\n", err)
			}
		}

	case triggerTimer:
		req := msg.triggerTimer
		l.grainTimerService.Trigger(req.Name)
	}
}

func (l *LocalGrainActivation) shutdown(ctx context.Context, err error) {
	for msg := range l.inbox {
		switch msg.messageType {
		case invokeMethod:
			req := msg.invokeMethod
			req.ResolveFunc(nil, err)
		case registerObserver:
			req := msg.registerObserver
			req.ResolveFunc(err)
		case unsubscribeObserver:
			req := msg.unsubscribeObserver
			req.ResolveFunc(err)
		case notifyObserver:
		case triggerTimer:
		}
	}

	l.grainActivator.deactivateCallback(l.identity)

	for msg := range l.evictChan {
		msg.onComplete(nil)
	}

	l.setStateDeactivated()
}

func (l *LocalGrainActivation) InvokeMethod(sender grain.Identity, method string, payload []byte, resolve func(out interface{}, err error)) error {
	return l.pushInbox(grainActivationMessage{
		messageType: invokeMethod,
		invokeMethod: &grainActivationInvokeMethod{
			Sender:      sender,
			Method:      method,
			Payload:     payload,
			ResolveFunc: resolve,
		},
	})
}

func (l *LocalGrainActivation) RegisterObserver(observer grain.Identity, observableType string, payload []byte, registrationTimeout time.Duration, resolve func(err error)) error {
	return l.pushInbox(grainActivationMessage{
		messageType: registerObserver,
		registerObserver: &grainActivationRegisterObserver{
			Observer:            observer,
			Name:                observableType,
			ResolveFunc:         resolve,
			RegistrationTimeout: registrationTimeout,
			Payload:             payload,
		},
	})
}

func (l *LocalGrainActivation) UnsubscribeObserver(observer grain.Identity, observableType string, resolve func(err error)) error {
	return l.pushInbox(grainActivationMessage{
		messageType: unsubscribeObserver,
		unsubscribeObserver: &grainActivationUnsubscribeObserver{
			Observer:    observer,
			Name:        observableType,
			ResolveFunc: resolve,
		},
	})
}

func (l *LocalGrainActivation) NotifyObservable(observable grain.Identity, observableType string, name string, payload []byte) error {
	return l.pushInbox(grainActivationMessage{
		messageType: notifyObserver,
		notifyObserver: &grainActivationNotifyObserver{
			Sender:         observable,
			ObservableType: observableType,
			Name:           name,
			Payload:        payload,
		},
	})
}

func (l *LocalGrainActivation) NotifyTimer(name string) error {
	return l.pushInbox(grainActivationMessage{
		messageType: triggerTimer,
		triggerTimer: &grainActivationTriggerTimer{
			Name: name,
		},
	})
}

func (l *LocalGrainActivation) EvictAsync(onComplete func(err error)) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.grainState >= grainStateDeactivating {
		// TODO: this is not completely correct. onComplete should be called as the last
		// thing the grain does, so it really should wait until the grain state is
		// grainStateDeactivated
		onComplete(nil)
		return
	}

	select {
	case l.evictChan <- grainActivationEvict{
		mustStop:   false,
		onComplete: onComplete,
	}:
	default:
		onComplete(ErrInboxFull)
	}
}

func (l *LocalGrainActivation) StopAsync(onComplete func(error)) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.grainState >= grainStateDeactivating {
		onComplete(nil)
		return
	}

	select {
	case l.evictChan <- grainActivationEvict{
		mustStop:   true,
		onComplete: onComplete,
	}:
	default:
		onComplete(ErrInboxFull)
	}
}

func (l *LocalGrainActivation) Name() string {
	return l.identity.String()
}

func (l *LocalGrainActivation) pushInbox(msg grainActivationMessage) error {
	l.lock.RLock()
	defer l.lock.RUnlock()

	if l.grainState >= grainStateDeactivating {
		return ErrGrainDeactivating
	}

	select {
	case l.inbox <- msg:
	default:
		return ErrInboxFull
	}
	return nil
}

var ErrInboxFull = errors.New("inbox full")
var ErrGrainActivationNotFound = errors.New("grain activation not found")
var ErrGrainDeactivating = errors.New("grain is deactivating")
var ErrGrainDeactivationRefused = errors.New("grain deactivation refused")
