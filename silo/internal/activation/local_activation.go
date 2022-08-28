package activation

import (
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"

	gcontext "github.com/jaym/go-orleans/context"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/generic"
	"github.com/jaym/go-orleans/silo/services/resourcemanager"
	"github.com/jaym/go-orleans/silo/services/timer"
)

var defaultDeactivateTimeout time.Duration = 60 * time.Second
var defaultGrainTimerTriggerTimeout time.Duration = 30 * time.Second
var defaultGrainInvocationTimeout time.Duration = 2 * time.Second

type grainActivationMessageType int

const (
	invokeMethodV2 grainActivationMessageType = iota + 1
	invokeOneWayMethod
	triggerTimer
)

type grainActivationInvokeMethodV2 struct {
	Sender      grain.Identity
	Method      string
	Dec         grain.Deserializer
	Ser         grain.Serializer
	Deadline    time.Time
	ResolveFunc func(err error)
}

type grainActivationInvokeOneWayMethod struct {
	Sender   grain.Identity
	Method   string
	Dec      grain.Deserializer
	Deadline time.Time
}

type grainActivationTriggerTimer struct {
	Name string
}

type grainActivationMessage struct {
	messageType        grainActivationMessageType
	triggerTimer       *grainActivationTriggerTimer
	invokeMethodV2     *grainActivationInvokeMethodV2
	invokeOneWayMethod *grainActivationInvokeOneWayMethod
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

	log      logr.Logger
	identity grain.Identity
	// description       *descriptor.GrainDescription
	activator         descriptor.ActivatorFunc
	grainTimerService *grainTimerServiceImpl
	grainActivator    *LocalGrainActivator
}

type LocalGrainActivator struct {
	log                logr.Logger
	registrar          descriptor.Registrar
	siloClient         grain.SiloClient
	timerService       timer.TimerService
	resourceManager    resourcemanager.ResourceManager
	defaultMailboxSize int
	deactivateCallback func(grain.Identity)
}

func NewLocalGrainActivator(log logr.Logger, registrar descriptor.Registrar, siloClient grain.SiloClient,
	timerService timer.TimerService, resourceManager resourcemanager.ResourceManager,
	defaultMailboxSize int,
	deactivateCallback func(grain.Identity)) *LocalGrainActivator {
	return &LocalGrainActivator{
		log:                log,
		registrar:          registrar,
		siloClient:         siloClient,
		timerService:       timerService,
		resourceManager:    resourceManager,
		defaultMailboxSize: defaultMailboxSize,
		deactivateCallback: deactivateCallback,
	}
}

func (m *LocalGrainActivator) ActivateGrainWithDefaultActivator(identity grain.Identity) (*LocalGrainActivation, error) {
	activatorFunc, err := m.registrar.LookupV2(identity.GrainType)
	if err != nil {
		return nil, err
	}

	return m.activateGrain(identity, activatorFunc)
}

func (m *LocalGrainActivator) ActivateGenericGrain(g *generic.Grain) (*LocalGrainActivation, error) {
	return m.activateGrain(g.GetIdentity(), g.Activate)
}

func (m *LocalGrainActivator) activateGrain(identity grain.Identity, activatorFunc descriptor.ActivatorFunc) (*LocalGrainActivation, error) {
	l := &LocalGrainActivation{
		log:      m.log.WithName("grain").WithValues("type", identity.GrainType, "id", identity.ID),
		identity: identity,
		// description:    grainDesc,
		activator:      activatorFunc,
		inbox:          make(chan grainActivationMessage, m.defaultMailboxSize),
		evictChan:      make(chan grainActivationEvict, 1),
		grainActivator: m,
	}
	l.start()
	return l, nil
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
		timers:        map[string]timerInfo{},
	}
	defer l.grainTimerService.Destroy()

	coreServices := &coreGrainService{
		grainTimerServices: l.grainTimerService,
		siloClient:         l.grainActivator.siloClient,
	}

	if err := l.grainActivator.resourceManager.Touch(l.identity); err != nil {
		l.setStateDeactivating()
		l.shutdown(ctx, resourcemanager.ErrNoCapacity)
		return
	}

	// activation, err := l.description.Activation.Handler(l.activator, ctx, coreServices, l.identity)
	activation, err := l.activator(ctx, l.identity, coreServices)
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

func (l *LocalGrainActivation) evict(ctx context.Context, activation grain.Activation, mustStop bool, onComplete func(error)) bool {
	ctx, cancel := context.WithTimeout(ctx, defaultDeactivateTimeout)
	defer cancel()

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

func (l *LocalGrainActivation) processMessage(ctx context.Context, activation grain.Activation, msg grainActivationMessage) {
	switch msg.messageType {
	case invokeMethodV2:
		if err := l.grainActivator.resourceManager.Touch(l.identity); err != nil {
			l.log.Error(err, "failed to touch")
		}
		req := msg.invokeMethodV2

		var cancel context.CancelFunc
		if deadline := req.Deadline; !deadline.IsZero() {
			ctx, cancel = context.WithDeadline(ctx, deadline)
		} else {
			ctx, cancel = context.WithTimeout(ctx, defaultGrainInvocationTimeout)
		}
		defer cancel()

		err := activation.InvokeMethod(ctx, req.Method, req.Sender, req.Dec, req.Ser)

		req.ResolveFunc(err)
	case invokeOneWayMethod:
		req := msg.invokeOneWayMethod

		var cancel context.CancelFunc
		if deadline := req.Deadline; !deadline.IsZero() {
			ctx, cancel = context.WithDeadline(ctx, deadline)
		} else {
			ctx, cancel = context.WithTimeout(ctx, defaultGrainInvocationTimeout)
		}
		defer cancel()

		err := activation.InvokeMethod(ctx, req.Method, req.Sender, req.Dec, nil)
		if err != nil {
			l.log.Error(err, "failed to invoke one way method", "identity", l.identity, "method", req.Method)
		}
	case triggerTimer:
		req := msg.triggerTimer
		ctx, cancel := context.WithTimeout(ctx, defaultGrainTimerTriggerTimeout)
		defer cancel()
		l.grainTimerService.Trigger(ctx, req.Name)
	}
}

func (l *LocalGrainActivation) shutdown(ctx context.Context, err error) {
	for msg := range l.inbox {
		switch msg.messageType {
		case invokeMethodV2:
			req := msg.invokeMethodV2
			req.ResolveFunc(err)
		}
	}

	l.grainActivator.deactivateCallback(l.identity)

	for msg := range l.evictChan {
		msg.onComplete(nil)
	}

	l.setStateDeactivated()
}

func (l *LocalGrainActivation) InvokeMethod(sender grain.Identity, method string, deadline time.Time, dec grain.Deserializer, ser grain.Serializer,
	resolve func(err error)) error {
	return l.pushInbox(grainActivationMessage{
		messageType: invokeMethodV2,
		invokeMethodV2: &grainActivationInvokeMethodV2{
			Sender:      sender,
			Method:      method,
			Deadline:    deadline,
			ResolveFunc: resolve,
			Dec:         dec,
			Ser:         ser,
		},
	})
}

func (l *LocalGrainActivation) InvokeOneWayMethod(sender grain.Identity, method string, deadline time.Time, dec grain.Deserializer) error {
	return l.pushInbox(grainActivationMessage{
		messageType: invokeOneWayMethod,
		invokeOneWayMethod: &grainActivationInvokeOneWayMethod{
			Sender:   sender,
			Method:   method,
			Dec:      dec,
			Deadline: deadline,
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
var ErrGrainMethodNotFound = errors.New("grain method not found")
var ErrGrainObservableNotFound = errors.New("grain observable not found")
