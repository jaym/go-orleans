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
	EnqueueInvokeMethodRequest(InvokeMethodRequest) error
	EnqueueRegisterObserverRequest(RegisterObserverRequest) error
	EnqueueObserverNotification(ObserverNotification) error
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

type grainActivationMessageType int

const (
	invokeMethod grainActivationMessageType = iota + 1
	registerObserver
	notifyObserver
	stop
)

type grainActivationInvokeMethod struct {
	req InvokeMethodRequest
}

type grainActivationRegisterObserver struct {
	req RegisterObserverRequest
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
}

type GrainActivation struct {
	Address     Address
	Description *GrainDescription
	impl        interface{}
	inbox       chan grainActivationMessage
	siloClient  SiloClient
	registrar   Registrar
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

		activation, err := g.Description.Activation.Handler(g.impl, ctx, g.siloClient, observerManager, g.Address)
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

func (m *GrainActivationManagerImpl) activateGrain(address Address) (*GrainActivation, error) {
	grainDesc, impl, err := m.registrar.Lookup(address.GrainType)
	if err != nil {
		return nil, err
	}
	activation := &GrainActivation{
		Address:     address,
		Description: grainDesc,
		impl:        impl,
		inbox:       make(chan grainActivationMessage, 8),
		siloClient:  m.silo.Client(),
		registrar:   m.registrar,
	}
	activation.Start()
	return activation, nil
}

func (m *GrainActivationManagerImpl) EnqueueInvokeMethodRequest(req InvokeMethodRequest) error {
	activation, err := m.getActivation(req.Receiver)
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
	activation, err := m.getActivation(req.Observable)
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
		activation, err := m.getActivation(receiver)
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

func (m *GrainActivationManagerImpl) getActivation(receiver Address) (*GrainActivation, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	activation, ok := m.grainActivations[receiver]
	if !ok {
		var err error
		activation, err = m.activateGrain(receiver)
		if err != nil {
			return nil, err
		}
		m.grainActivations[receiver] = activation
	}
	return activation, nil
}
