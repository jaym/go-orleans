package silo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/proto"
)

type GrainDescription struct {
	GrainType   string
	Activation  ActivationDesc
	Methods     []MethodDesc
	Observables []ObservableDesc
}

type Address struct {
	Location  string
	GrainType string
	ID        string
}

func (m Address) GetAddress() Address {
	return m
}

func (a Address) String() string {
	return fmt.Sprintf("%s/%s/%s", a.Location, a.GrainType, a.ID)
}

type Addressable interface {
	GetAddress() Address
}

type ActivationDesc struct {
	Handler ActivationHandler
}

type MethodDesc struct {
	Name    string
	Handler MethodHandler
}

type ObservableDesc struct {
	Name          string
	Handler       ObservableHandler
	NotifyHandler NotifyObserverHandler
}

type ActivationHandler func(activator interface{}, ctx context.Context, coreServices CoreGrainServices, o ObserverManager, address Address) (Addressable, error)
type MethodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error)
type ObservableHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) error
type NotifyObserverHandler func(m ObserverManager, ctx context.Context, o []RegisteredObserver) error

type Registrar interface {
	Register(desc *GrainDescription, impl interface{})
	Lookup(grainType string) (*GrainDescription, interface{}, error)
}

type Silo struct {
	localGrainManager GrainActivationManager
	client            *siloClientImpl
	timerService      TimerService
	log               logr.Logger
}

func NewSilo(log logr.Logger, registrar Registrar) *Silo {
	s := &Silo{
		log: log.WithName("silo"),
	}
	// TODO: do something a little more sensible. With the generic
	// grain, it is expected to pass in an activator for each
	// activation. Setting it to nil is will cause a panic with
	// no information on what went wrong
	registrar.Register(&grain_GrainDesc, nil)
	s.localGrainManager = NewGrainActivationManager(registrar, s)
	s.client = &siloClientImpl{
		futures:                 make(map[string]InvokeMethodFuture),
		registerObserverFutures: make(map[string]RegisterObserverFuture),
		grainManager:            s.localGrainManager,
		log:                     s.log.WithName("siloClient"),
	}
	s.timerService = newTimerServiceImpl(s.log.WithName("timerService"), func(grainAddr Address, name string) {
		err := s.localGrainManager.EnqueueTimerTrigger(TimerTriggerNotification{
			Receiver: grainAddr,
			Name:     name,
		})
		if err != nil {
			s.log.V(1).Error(err, "failed to trigger timer notification", "address", grainAddr, "triggerName", name)
		}
	})
	s.timerService.Start()
	return s
}

func (s *Silo) Client() SiloClient {
	return s.client
}

func (s *Silo) TimerService() TimerService {
	return s.timerService
}

func (s *Silo) Register(desc *GrainDescription, activator interface{}) {
}

func (s *Silo) CreateGrain(activator GenericGrainActivator) (Address, error) {
	address := Address{
		Location:  "local",
		GrainType: "Grain",
		ID:        ksuid.New().String(),
	}
	err := s.localGrainManager.ActivateGrain(ActivateGrainRequest{
		Address:   address,
		Activator: activator,
	})
	return address, err
}

type SiloClient interface {
	InvokeMethod(ctx context.Context, toAddress Address, grainType string, method string,
		in proto.Message) InvokeMethodFuture

	RegisterObserver(ctx context.Context, observer Address, observable Address, name string, in proto.Message) RegisterObserverFuture
	AckRegisterObserver(ctx context.Context, receiver Address, uuid string, errOut error) error
	SendResponse(ctx context.Context, receiver Address, uuid string, out proto.Message) error
	NotifyObservers(ctx context.Context, observableType string, observableName string, receiver []Address, out proto.Message) error
}

type siloClientImpl struct {
	mutex                   sync.Mutex
	futures                 map[string]InvokeMethodFuture
	registerObserverFutures map[string]RegisterObserverFuture
	grainManager            GrainActivationManager
	log                     logr.Logger
}

func (s *siloClientImpl) SendResponse(ctx context.Context, receiver Address, uuid string, out proto.Message) error {
	log := s.log.WithValues("receiver", receiver, "uuid", uuid)
	log.V(4).Info("SendResponse")

	s.mutex.Lock()
	f, ok := s.futures[uuid]
	delete(s.futures, uuid)
	s.mutex.Unlock()
	if !ok {
		log.Info("future not found")
		panic("future not found")
	}
	bytes, err := proto.Marshal(out)
	if err != nil {
		log.Error(err, "failed to marshal")
		return err
	}
	err = f.Resolve(InvokeMethodResp{
		data: bytes,
	})
	if err != nil {
		log.Error(err, "failed to resolve future")
		return err
	}

	return nil
}

func (s *siloClientImpl) InvokeMethod(ctx context.Context, receiver Address, grainType string, method string,
	in proto.Message) InvokeMethodFuture {
	id := ksuid.New().String()
	log := s.log.WithValues("uuid", id, "receiver", receiver, "grainType", grainType, "method", method)

	log.V(4).Info("InvokeMethod")

	f := newInvokeMethodFuture(id, 60*time.Second)
	sender := AddressFromContext(ctx)
	if sender == nil {
		log.Info("no sender in context")
		// TODO: generate anonymous address
		panic("no sender")
	}
	bytes, err := proto.Marshal(in)
	if err != nil {
		log.Error(err, "failed to marshal")
		// TODO: error handling
		panic(err)
	}
	s.mutex.Lock()
	if _, ok := s.futures[id]; ok {
		log.Error(err, "duplicate futures found")
		panic("duplicate future ids")
	}
	s.futures[id] = f
	s.mutex.Unlock()

	err = s.grainManager.EnqueueInvokeMethodRequest(InvokeMethodRequest{
		Sender:   *sender,
		Receiver: receiver,
		Method:   method,
		UUID:     id,
		in:       bytes,
	})
	if err != nil {
		log.Error(err, "failed to enqueue method invokation")

		s.mutex.Lock()
		delete(s.futures, id)
		s.mutex.Unlock()
		if rErr := f.Resolve(InvokeMethodResp{err: err}); rErr != nil {
			log.Error(err, "failed to resolve future")
		}
	}
	return f
}

func (s *siloClientImpl) RegisterObserver(ctx context.Context, observer Address, observable Address,
	name string, in proto.Message) RegisterObserverFuture {

	id := ksuid.New().String()

	log := s.log.WithValues("uuid", id, "observer", observer, "observable", observable, "observableName", name)
	log.V(4).Info("RegisterObserver")

	f := newRegisterObserverFuture(id, 60*time.Second)
	bytes, err := proto.Marshal(in)
	if err != nil {
		log.Error(err, "failed to marshal")
		// TODO: error handling
		panic(err)
	}
	s.mutex.Lock()
	if _, ok := s.registerObserverFutures[id]; ok {
		s.mutex.Unlock()
		log.Error(err, "duplicate futures found")
		panic("duplicate future ids")
	}
	s.registerObserverFutures[id] = f
	s.mutex.Unlock()

	err = s.grainManager.EnqueueRegisterObserverRequest(RegisterObserverRequest{
		Observer:   observer,
		Observable: observable,
		Name:       name,
		UUID:       id,
		In:         bytes,
	})
	if err != nil {
		log.Error(err, "failed to enqueue register observer request")

		s.mutex.Lock()
		delete(s.registerObserverFutures, id)
		s.mutex.Unlock()
		if rErr := f.Resolve(RegisterObserverResp{Err: err}); rErr != nil {
			log.Error(err, "failed to resolve future")
		}
	}
	return f
}

func (s *siloClientImpl) AckRegisterObserver(ctx context.Context, receiver Address, uuid string, errOut error) error {
	log := s.log.WithValues("uuid", uuid, "reciever", receiver)
	log.V(4).Info("AckRegisterObserver")

	s.mutex.Lock()
	f, ok := s.registerObserverFutures[uuid]
	s.mutex.Unlock()
	if !ok {
		panic("future not found")
	}

	err := f.Resolve(RegisterObserverResp{
		Err: errOut,
	})
	s.mutex.Lock()
	delete(s.futures, uuid)
	s.mutex.Unlock()

	return err
}

func (s *siloClientImpl) NotifyObservers(ctx context.Context, observableType string, observableName string,
	receivers []Address, out proto.Message) error {

	log := s.log.WithValues("recievers", receivers)
	log.V(4).Info("NotifyObservers")

	if len(receivers) == 0 {
		return nil
	}

	sender := AddressFromContext(ctx)
	if sender == nil {
		log.Info("no sender in context")
		panic("no sender")
	}

	data, err := proto.Marshal(out)
	if err != nil {
		log.Error(err, "failed to marshal")
		return err
	}

	return s.grainManager.EnqueueObserverNotification(ObserverNotification{
		Sender:         *sender,
		Receivers:      receivers,
		ObservableType: observableType,
		Name:           observableName,
		In:             data,
	})
}
