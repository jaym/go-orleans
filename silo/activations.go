package silo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
)

type GrainActivationManager interface {
	ActivateGrain(ActivateGrainRequest) error
	EnqueueInvokeMethodRequest(InvokeMethodRequest) error
	EnqueueRegisterObserverRequest(RegisterObserverRequest) error
	EnqueueObserverNotification(ObserverNotification) error
	EnqueueTimerTrigger(TimerTriggerNotification) error
}

type ActivateGrainRequest struct {
	Address   Address
	Activator GenericGrainActivator
}

type InvokeMethodRequest struct {
	Sender   Address
	Receiver Address
	Method   string
	UUID     string
	in       []byte
}

type RegisterObserverRequest struct {
	Observer   Address
	Observable Address
	Name       string
	UUID       string
	In         []byte
}

type ObserverNotification struct {
	Sender         Address
	Receivers      []Address
	ObservableType string
	Name           string
	UUID           string
	In             []byte
}

type TimerTriggerNotification struct {
	Receiver Address
	Name     string
}

type grainActivationMessageType int

const (
	invokeMethod grainActivationMessageType = iota + 1
	registerObserver
	notifyObserver
	triggerTimer
	stop
)

type grainActivationInvokeMethod struct {
	req InvokeMethodRequest
}

type grainActivationRegisterObserver struct {
	req RegisterObserverRequest
}

type grainActivationTriggerTimer struct {
	Receiver Address
	Name     string
}

type grainActivationNotifyObserver struct {
	Sender         Address
	Receiver       Address
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
}

type GrainActivation struct {
	Address      Address
	Description  *GrainDescription
	impl         interface{}
	inbox        chan grainActivationMessage
	siloClient   SiloClient
	registrar    Registrar
	timerService TimerService
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
	go func() {
		ctx := WithAddressContext(context.Background(), g.Address)
		observerManager := NewInmemoryObserverManager(g.Address, g.siloClient)

		grainTimerService := &grainTimerServiceImpl{
			grainAddress: g.Address,
			timerService: g.timerService,
			timers:       map[string]func(){},
		}

		coreServices := &coreGrainService{
			grainTimerServices: grainTimerService,
			siloClient:         g.siloClient,
		}

		activation, err := g.Description.Activation.Handler(g.impl, ctx, coreServices, observerManager, g.Address)
		if err != nil {
			panic(err)
		}
	LOOP:
		for msg := range g.inbox {
			switch msg.messageType {
			case invokeMethod:
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
				req := msg.registerObserver.req
				o, err := g.findObserableDesc("", req.Name)
				if err != nil {
					panic(err)
				}
				_, err = observerManager.Add(ctx, o.Name, req.Observer, req.In)
				g.siloClient.AckRegisterObserver(ctx, req.Observer, req.UUID, err)
			case notifyObserver:
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
			case stop:
				break LOOP
			}
		}
	}()
}

type GrainActivationManagerImpl struct {
	lock             sync.Mutex
	grainActivations map[Address]*GrainActivation
	registrar        Registrar
	silo             *Silo
}

func NewGrainActivationManager(registrar Registrar, silo *Silo) *GrainActivationManagerImpl {
	return &GrainActivationManagerImpl{
		grainActivations: make(map[Address]*GrainActivation),
		registrar:        registrar,
		silo:             silo,
	}
}

func (m *GrainActivationManagerImpl) activateGrainWithDefaultActivator(address Address) (*GrainActivation, error) {
	grainDesc, activator, err := m.registrar.Lookup(address.GrainType)
	if err != nil {
		return nil, err
	}
	activation := &GrainActivation{
		Address:      address,
		Description:  grainDesc,
		impl:         activator,
		inbox:        make(chan grainActivationMessage, 8),
		siloClient:   m.silo.Client(),
		registrar:    m.registrar,
		timerService: m.silo.TimerService(),
	}
	activation.Start()
	return activation, nil
}

func (m *GrainActivationManagerImpl) activateGrainWithActivator(address Address, activator interface{}) (*GrainActivation, error) {
	if activator == nil {
		panic("WHAT")
	}
	grainDesc, _, err := m.registrar.Lookup(address.GrainType)
	if err != nil {
		return nil, err
	}
	activation := &GrainActivation{
		Address:      address,
		Description:  grainDesc,
		impl:         activator,
		inbox:        make(chan grainActivationMessage, 8),
		siloClient:   m.silo.Client(),
		registrar:    m.registrar,
		timerService: m.silo.TimerService(),
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
	_, ok := m.grainActivations[req.Address]
	if !ok {
		var activation *GrainActivation
		var err error
		if req.Activator == nil {
			activation, err = m.activateGrainWithDefaultActivator(req.Address)
		} else {
			activation, err = m.activateGrainWithActivator(req.Address, req.Activator)
		}
		if err != nil {
			return err
		}
		m.grainActivations[req.Address] = activation
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

func (m *GrainActivationManagerImpl) getActivation(receiver Address, allowActivation bool) (*GrainActivation, error) {
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
