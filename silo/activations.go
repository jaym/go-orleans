package silo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
	"google.golang.org/protobuf/proto"
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
	In         interface{}
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

type grainActivationMessageType int

const (
	invokeMethod grainActivationMessageType = iota + 1
	registerObserver
	notifyObserver
	triggerTimer
	evictGrain
	stop
)

type grainActivationInvokeMethod struct {
	req InvokeMethodRequest
}

type grainActivationRegisterObserver struct {
	req RegisterObserverRequest
}

type grainActivationTriggerTimer struct {
	Receiver grain.Identity
	Name     string
}

type grainActivationNotifyObserver struct {
	Sender         grain.Identity
	Receiver       grain.Identity
	ObservableType string
	Name           string
	UUID           string
	In             []byte
}

type grainActivationMessage struct {
	messageType      grainActivationMessageType
	invokeMethod     *grainActivationInvokeMethod
	registerObserver *grainActivationRegisterObserver
	notifyObserver   *grainActivationNotifyObserver
	triggerTimer     *grainActivationTriggerTimer
	evictGrain       *grainActivationEvict
}

type grainActivationEvict struct {
}

type GrainActivation struct {
	Identity           grain.Identity
	Description        *GrainDescription
	impl               interface{}
	inbox              chan grainActivationMessage
	siloClient         grain.SiloClient
	registrar          Registrar
	timerService       timer.TimerService
	resourceManager    *resourceManager
	observerStore      observer.Store
	deactivateCallback func(grain.Identity)
}

func (g *GrainActivation) findMethodDesc(name string) (*MethodDesc, error) {
	for i := range g.Description.Methods {
		if g.Description.Methods[i].Name == name {
			return &g.Description.Methods[i], nil
		}
	}
	return nil, errors.New("method not found")
}

func (g *GrainActivation) findObserableDesc(grainType, name string) (*ObservableDesc, error) {
	var desc *GrainDescription
	if grainType == "" {
		desc = g.Description
	} else {
		var err error
		desc, _, err = g.registrar.Lookup(grainType)
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

func (g *GrainActivation) Start() {
	g.start()
}

func (g *GrainActivation) start() {
	go func() {
		g.loop()
		g.deactivateCallback(g.Identity)
	}()
}

func (g *GrainActivation) loop() {
	ctx := WithIdentityContext(context.Background(), g.Identity)
	observerManager := newGrainObserverManager(g.Identity, g.observerStore, g.siloClient)

	grainTimerService := &grainTimerServiceImpl{
		grainIdentity: g.Identity,
		timerService:  g.timerService,
		timers:        map[string]func(){},
	}

	coreServices := &coreGrainService{
		grainTimerServices: grainTimerService,
		siloClient:         g.siloClient,
	}

	if err := g.resourceManager.Touch(g.Identity); err != nil {
		return
	}

	activation, err := g.Description.Activation.Handler(g.impl, ctx, coreServices, observerManager, g.Identity)
	if err != nil {
		panic(err)
	}
LOOP:
	for msg := range g.inbox {
		switch msg.messageType {
		case invokeMethod:
			g.resourceManager.Touch(g.Identity)
			req := msg.invokeMethod.req
			m, err := g.findMethodDesc(req.Method)
			if err != nil {
				// encode error
				panic(err)
			}
			resp, err := m.Handler(activation, ctx, func(in interface{}) error {
				return proto.Unmarshal(req.in, in.(proto.Message))
			})
			if err != nil {
				// encode error
				panic(err)
			}
			err = g.siloClient.SendResponse(ctx, req.Sender, req.UUID, resp.(proto.Message))
			if err != nil {
				// encode error
				panic(err)
			}
		case registerObserver:
			g.resourceManager.Touch(g.Identity)

			req := msg.registerObserver.req
			o, err := g.findObserableDesc("", req.Name)
			if err != nil {
				panic(err)
			}
			_, err = observerManager.Add(ctx, o.Name, req.Observer, req.In)
			g.siloClient.AckRegisterObserver(ctx, req.Observer, req.UUID, err)
		case notifyObserver:
			g.resourceManager.Touch(g.Identity)

			req := msg.notifyObserver
			o, err := g.findObserableDesc(req.ObservableType, req.Name)
			if err != nil {
				panic(err)
			}
			err = o.Handler(activation, ctx, func(in interface{}) error {
				return proto.Unmarshal(req.In, in.(proto.Message))
			})
			if err != nil {
				fmt.Printf("err: %v\n", err)
			}
		case triggerTimer:
			req := msg.triggerTimer
			grainTimerService.Trigger(req.Name)
		case evictGrain:
			canEvict := true
			if hasCanEvict, ok := activation.(HasCanEvict); ok {
				canEvict = hasCanEvict.CanEvict(ctx)
			}
			if canEvict {
				if hasDeactivate, ok := activation.(HasDeactivate); ok {
					hasDeactivate.Deactivate(ctx)
				}
				break LOOP
			}
		case stop:
			break LOOP
		}
	}
}

type GrainActivationManagerImpl struct {
	lock               sync.Mutex
	grainActivations   map[grain.Identity]*GrainActivation
	registrar          Registrar
	silo               *Silo
	resourceManager    *resourceManager
	deactivateCallback func(grain.Identity)
}

func NewGrainActivationManager(registrar Registrar, silo *Silo) *GrainActivationManagerImpl {
	m := &GrainActivationManagerImpl{
		grainActivations: make(map[grain.Identity]*GrainActivation),
		registrar:        registrar,
		silo:             silo,
	}
	resourceManager := newResourceManager(8, func(identityes []grain.Identity) {
		for _, a := range identityes {
			m.EnqueueEvictGrain(EvictGrainRequest{
				Identity: a,
			})
		}
	})
	resourceManager.Start()
	m.resourceManager = resourceManager
	m.deactivateCallback = func(a grain.Identity) {
		m.resourceManager.Remove(a)
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m.grainActivations, a)
	}
	return m
}

func (m *GrainActivationManagerImpl) activateGrainWithDefaultActivator(identity grain.Identity) (*GrainActivation, error) {
	grainDesc, activator, err := m.registrar.Lookup(identity.GrainType)
	if err != nil {
		return nil, err
	}

	activation := &GrainActivation{
		Identity:           identity,
		Description:        grainDesc,
		impl:               activator,
		inbox:              make(chan grainActivationMessage, 8),
		siloClient:         m.silo.Client(),
		registrar:          m.registrar,
		timerService:       m.silo.timerService,
		resourceManager:    m.resourceManager,
		deactivateCallback: m.deactivateCallback,
		observerStore:      m.silo.observerStore,
	}
	activation.Start()
	return activation, nil
}

func (m *GrainActivationManagerImpl) activateGrainWithActivator(identity grain.Identity, activator interface{}) (*GrainActivation, error) {
	grainDesc, _, err := m.registrar.Lookup(identity.GrainType)
	if err != nil {
		return nil, err
	}

	activation := &GrainActivation{
		Identity:           identity,
		Description:        grainDesc,
		impl:               activator,
		inbox:              make(chan grainActivationMessage, 8),
		siloClient:         m.silo.Client(),
		registrar:          m.registrar,
		timerService:       m.silo.timerService,
		resourceManager:    m.resourceManager,
		deactivateCallback: m.deactivateCallback,
		observerStore:      m.silo.observerStore,
	}
	activation.Start()
	return activation, nil
}

func (m *GrainActivationManagerImpl) EnqueueInvokeMethodRequest(req InvokeMethodRequest) error {
	activation, err := m.getActivation(req.Receiver, true)
	if err != nil {
		return err
	}

	msg := grainActivationMessage{
		messageType: invokeMethod,
		invokeMethod: &grainActivationInvokeMethod{
			req: req,
		},
	}

	select {
	case activation.inbox <- msg:
	default:
		return errors.New("inbox full")
	}
	return nil
}

func (m *GrainActivationManagerImpl) EnqueueRegisterObserverRequest(req RegisterObserverRequest) error {
	activation, err := m.getActivation(req.Observable, true)
	if err != nil {
		return err
	}

	msg := grainActivationMessage{
		messageType: registerObserver,
		registerObserver: &grainActivationRegisterObserver{
			req: req,
		},
	}

	select {
	case activation.inbox <- msg:
	default:
		return errors.New("inbox full")
	}
	return nil
}

func (m *GrainActivationManagerImpl) EnqueueObserverNotification(req ObserverNotification) error {
	var errs []string
	for _, receiver := range req.Receivers {
		activation, err := m.getActivation(receiver, true)
		if err != nil {
			errs = append(errs, err.Error())
		}
		msg := grainActivationMessage{
			messageType: notifyObserver,
			notifyObserver: &grainActivationNotifyObserver{
				Sender:         req.Sender,
				Receiver:       receiver,
				ObservableType: req.ObservableType,
				Name:           req.Name,
				In:             req.In,
			},
		}

		select {
		case activation.inbox <- msg:
		default:
			errs = append(errs, errors.New("inbox full").Error())
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
		var activation *GrainActivation
		var err error
		if req.Activator == nil {
			activation, err = m.activateGrainWithDefaultActivator(req.Identity)
		} else {
			activation, err = m.activateGrainWithActivator(req.Identity, req.Activator)
		}
		if err != nil {
			return err
		}
		m.grainActivations[req.Identity] = activation
	}
	return nil
}

func (m *GrainActivationManagerImpl) EnqueueTimerTrigger(req TimerTriggerNotification) error {
	activation, err := m.getActivation(req.Receiver, false)
	if err != nil {
		return err
	}
	msg := grainActivationMessage{
		messageType: triggerTimer,
		triggerTimer: &grainActivationTriggerTimer{
			Receiver: req.Receiver,
			Name:     req.Name,
		},
	}
	select {
	case activation.inbox <- msg:
	default:
		return errors.New("inbox full")
	}
	return nil
}

func (m *GrainActivationManagerImpl) EnqueueEvictGrain(req EvictGrainRequest) error {
	activation, err := m.getActivation(req.Identity, false)
	if err != nil {
		return err
	}
	fmt.Printf("Evicting grain %s\n", req.Identity.ID)
	msg := grainActivationMessage{
		messageType: evictGrain,
		evictGrain:  &grainActivationEvict{},
	}
	select {
	case activation.inbox <- msg:
	default:
		return errors.New("inbox full")
	}
	return nil
}

func (m *GrainActivationManagerImpl) getActivation(receiver grain.Identity, allowActivation bool) (*GrainActivation, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	activation, ok := m.grainActivations[receiver]
	if !ok {
		if allowActivation {
			var err error
			activation, err = m.activateGrainWithDefaultActivator(receiver)
			if err != nil {
				return nil, err
			}
			m.grainActivations[receiver] = activation
		} else {
			return nil, errors.New("grain activation not found")
		}
	}
	return activation, nil
}
