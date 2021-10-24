package silo

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-logr/logr"

	gcontext "github.com/jaym/go-orleans/context"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/proto"
)

type Silo struct {
	localGrainManager GrainActivationManager
	client            *siloClientImpl
	timerService      timer.TimerService
	observerStore     observer.Store
	log               logr.Logger
}

func NewSilo(log logr.Logger, observerStore observer.Store, registrar descriptor.Registrar) *Silo {
	s := &Silo{
		log:           log.WithName("silo"),
		observerStore: observerStore,
	}
	// TODO: do something a little more sensible. With the generic
	// grain, it is expected to pass in an activator for each
	// activation. Setting it to nil is will cause a panic with
	// no information on what went wrong
	registrar.Register(&grain_GrainDesc, nil)
	s.client = &siloClientImpl{
		futures:                 make(map[string]grain.InvokeMethodFuture),
		registerObserverFutures: make(map[string]grain.RegisterObserverFuture),
		log:                     s.log.WithName("siloClient"),
		codec:                   protobuf.NewCodec(),
	}
	s.timerService = newTimerServiceImpl(s.log.WithName("timerService"), func(grainAddr grain.Identity, name string) {
		err := s.localGrainManager.EnqueueTimerTrigger(TimerTriggerNotification{
			Receiver: grainAddr,
			Name:     name,
		})
		if err != nil {
			s.log.V(1).Error(err, "failed to trigger timer notification", "identity", grainAddr, "triggerName", name)
		}
	})
	s.localGrainManager = NewGrainActivationManager(registrar, s.client, s.timerService, s.observerStore)
	s.client.grainManager = s.localGrainManager
	s.timerService.Start()
	return s
}

func (s *Silo) Client() grain.SiloClient {
	return s.client
}

func (s *Silo) Register(desc *descriptor.GrainDescription, activator interface{}) {
}

func (s *Silo) CreateGrain(activator GenericGrainActivator) (grain.Identity, error) {
	identity := grain.Identity{
		GrainType: "Grain",
		ID:        ksuid.New().String(),
	}
	err := s.localGrainManager.ActivateGrain(ActivateGrainRequest{
		Identity:  identity,
		Activator: activator,
	})
	return identity, err
}

type siloClientImpl struct {
	mutex                   sync.Mutex
	futures                 map[string]grain.InvokeMethodFuture
	registerObserverFutures map[string]grain.RegisterObserverFuture
	grainManager            GrainActivationManager
	log                     logr.Logger
	codec                   codec.Codec
}

func (s *siloClientImpl) SendResponse(ctx context.Context, receiver grain.Identity, uuid string, out proto.Message) error {
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

	if err := f.ResolveValue(out); err != nil {
		log.Error(err, "failed to resolve future")
		return err
	}

	return nil
}

func (s *siloClientImpl) InvokeMethod(ctx context.Context, receiver grain.Identity, grainType string, method string,
	in proto.Message) grain.InvokeMethodFuture {
	id := ksuid.New().String()
	log := s.log.WithValues("uuid", id, "receiver", receiver, "grainType", grainType, "method", method)

	log.V(4).Info("InvokeMethod")

	f := newInvokeMethodFuture(s.codec, id, 60*time.Second)
	sender := gcontext.IdentityFromContext(ctx)
	if sender == nil {
		log.Info("no sender in context")
		// TODO: generate anonymous identity
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
		if rErr := f.ResolveError(err); rErr != nil {
			log.Error(err, "failed to resolve future")
		}
	}
	return f
}

func (s *siloClientImpl) RegisterObserver(ctx context.Context, observer grain.Identity, observable grain.Identity,
	name string, in proto.Message) grain.RegisterObserverFuture {

	id := ksuid.New().String()

	log := s.log.WithValues("uuid", id, "observer", observer, "observable", observable, "observableName", name)
	log.V(4).Info("RegisterObserver")

	f := newRegisterObserverFuture(id, 60*time.Second)
	s.mutex.Lock()
	if _, ok := s.registerObserverFutures[id]; ok {
		s.mutex.Unlock()
		log.Error(errors.New("duplicate future ids"), "duplicate futures found")
		panic("duplicate future ids")
	}
	s.registerObserverFutures[id] = f
	s.mutex.Unlock()

	data, err := proto.Marshal(in)
	if err != nil {
		log.Error(err, "failed to marshal")

		s.mutex.Lock()
		delete(s.registerObserverFutures, id)
		s.mutex.Unlock()
		if rErr := f.ResolveError(err); rErr != nil {
			log.Error(err, "failed to resolve future")
		}
	}

	err = s.grainManager.EnqueueRegisterObserverRequest(RegisterObserverRequest{
		Observer:   observer,
		Observable: observable,
		Name:       name,
		UUID:       id,
		In:         data,
	})
	if err != nil {
		log.Error(err, "failed to enqueue register observer request")

		s.mutex.Lock()
		delete(s.registerObserverFutures, id)
		s.mutex.Unlock()
		if rErr := f.ResolveError(err); rErr != nil {
			log.Error(err, "failed to resolve future")
		}
	}
	return f
}

func (s *siloClientImpl) AckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut error) error {
	log := s.log.WithValues("uuid", uuid, "reciever", receiver)
	log.V(4).Info("AckRegisterObserver")

	s.mutex.Lock()
	f, ok := s.registerObserverFutures[uuid]
	s.mutex.Unlock()
	if !ok {
		panic("future not found")
	}

	var err error

	if errOut != nil {
		err = f.ResolveError(errOut)
	} else {
		err = f.Resolve()
	}

	s.mutex.Lock()
	delete(s.futures, uuid)
	s.mutex.Unlock()

	return err
}

func (s *siloClientImpl) NotifyObservers(ctx context.Context, observableType string, observableName string,
	receivers []grain.Identity, out proto.Message) error {

	log := s.log.WithValues("recievers", receivers)
	log.V(4).Info("NotifyObservers")

	if len(receivers) == 0 {
		return nil
	}

	sender := gcontext.IdentityFromContext(ctx)
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
