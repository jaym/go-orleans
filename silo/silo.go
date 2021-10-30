package silo

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"

	gcontext "github.com/jaym/go-orleans/context"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/silo/internal/graindir"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/jaym/go-orleans/silo/services/observer"
	"github.com/jaym/go-orleans/silo/services/timer"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/proto"
)

type Silo struct {
	localGrainManager *GrainActivationManagerImpl
	client            *siloClientImpl
	timerService      timer.TimerService
	observerStore     observer.Store
	log               logr.Logger
	nodeName          cluster.Location
	grainDirectory    cluster.GrainDirectory
	transportManager  *transport.Manager
}

func NewSilo(log logr.Logger, observerStore observer.Store, registrar descriptor.Registrar) *Silo {
	s := &Silo{
		log:           log.WithName("silo"),
		observerStore: observerStore,
	}
	s.nodeName = cluster.Location("mynodename")
	s.grainDirectory = graindir.NewInmemoryGrainDirectory(s.nodeName)
	// TODO: do something a little more sensible. With the generic
	// grain, it is expected to pass in an activator for each
	// activation. Setting it to nil is will cause a panic with
	// no information on what went wrong
	registrar.Register(&grain_GrainDesc, nil)
	s.client = &siloClientImpl{
		log:            s.log.WithName("siloClient"),
		codec:          protobuf.NewCodec(),
		nodeName:       s.nodeName,
		grainDirectory: s.grainDirectory,
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
	s.localGrainManager = NewGrainActivationManager(registrar, s.client, s.timerService, s.observerStore, s.grainDirectory)

	handler := siloTransportHandler{
		log:               s.log.WithName("transport-handler"),
		codec:             s.client.codec,
		localGrainManager: s.localGrainManager,
	}
	s.transportManager = transport.NewManager(s.log.WithName("transport-manager"), handler)
	s.transportManager.AddTransport(s.nodeName, &localTransport{
		log:               s.log.WithName("local-transport"),
		codec:             s.client.codec,
		localGrainManager: s.localGrainManager,
	})
	s.client.transportManager = s.transportManager
	s.timerService.Start()

	return s
}

type siloTransportHandler struct {
	log               logr.Logger
	codec             codec.Codec
	localGrainManager *GrainActivationManagerImpl
}

func (s siloTransportHandler) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, payload []byte, promise transport.InvokeMethodPromise) {
	err := s.localGrainManager.EnqueueInvokeMethodRequest(InvokeMethodRequest{
		Sender:   sender,
		Receiver: receiver,
		Method:   method,
		in:       payload,
		ResolveFunc: func(i interface{}, e error) {
			if e != nil {
				promise.Resolve(nil, e)
			} else {
				data, err := s.codec.Encode(i)
				promise.Resolve(data, err)
			}
		},
	})
	if err != nil {
		promise.Resolve(nil, err)
		s.log.Error(err, "failed to enqueue invoke method", "sender", sender, "receiver", receiver, "method", method)
	}
}

func (s siloTransportHandler) ReceiveRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, payload []byte, promise transport.RegisterObserverPromise) {
	err := s.localGrainManager.EnqueueRegisterObserverRequest(RegisterObserverRequest{
		Observer:   observer,
		Observable: observable,
		Name:       name,
		In:         payload,
		ResolveFunc: func(e error) {
			if e != nil {
				promise.Resolve(e)
			} else {
				promise.Resolve(nil)
			}
		},
	})
	if err != nil {
		promise.Resolve(err)
		s.log.Error(err, "failed to enqueue register observer", "observer", observer, "observable", observable, "name", name)
	}
}

func (s siloTransportHandler) ReceiveObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte) {
	err := s.localGrainManager.EnqueueObserverNotification(ObserverNotification{
		Sender:         sender,
		Receivers:      receivers,
		ObservableType: observableType,
		Name:           name,
		In:             payload,
	})
	if err != nil {
		s.log.Error(err, "failed to enqueue observer notification", "sender", sender, "receivers", receivers, "name", name)
	}
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
	log              logr.Logger
	codec            codec.Codec
	transportManager *transport.Manager
	nodeName         cluster.Location
	grainDirectory   cluster.GrainDirectory
}

func (s *siloClientImpl) getGrainAddress(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	grainAddress, err := s.grainDirectory.Lookup(ctx, ident)
	if err != nil {
		if err == cluster.ErrGrainActivationNotFound {
			return s.placeGrain(ctx, ident)
		}
		return cluster.GrainAddress{}, err
	}
	return grainAddress, nil
}

func (s *siloClientImpl) placeGrain(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	return cluster.GrainAddress{
		Location: cluster.Location(s.nodeName),
		Identity: ident,
	}, nil
}

func (s *siloClientImpl) InvokeMethod(ctx context.Context, receiver grain.Identity, grainType string, method string,
	in proto.Message) grain.InvokeMethodFuture {
	id := ksuid.New().String()
	log := s.log.WithValues("uuid", id, "receiver", receiver, "grainType", grainType, "method", method)

	log.V(4).Info("InvokeMethod")

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

	addr, err := s.getGrainAddress(ctx, receiver)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	f, err := s.transportManager.InvokeMethod(ctx, *sender, addr, method, id, bytes)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	return newInvokeMethodFuture(s.codec, f)
}

func (s *siloClientImpl) RegisterObserver(ctx context.Context, observer grain.Identity, observable grain.Identity,
	name string, in proto.Message) grain.RegisterObserverFuture {

	id := ksuid.New().String()

	log := s.log.WithValues("uuid", id, "observer", observer, "observable", observable, "observableName", name)
	log.V(4).Info("RegisterObserver")

	data, err := proto.Marshal(in)
	if err != nil {
		log.Error(err, "failed to marshal")
		panic(err)
	}

	addr, err := s.getGrainAddress(ctx, observable)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	f, err := s.transportManager.RegisterObserver(ctx, observer, addr, name, id, data)
	if err != nil {
		panic(err)
	}

	return newRegisterObserverFuture(s.codec, f)
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

	receiverAddrs := make([]cluster.GrainAddress, 0, len(receivers))
	var grainAddrErrs error
	for _, r := range receivers {
		addr, err := s.getGrainAddress(ctx, r)
		if err != nil {
			s.log.V(0).Error(err, "failed to find grain address", "grain", r)
			grainAddrErrs = errors.CombineErrors(grainAddrErrs, err)
			continue
		}
		receiverAddrs = append(receiverAddrs, addr)
	}

	err2 := s.transportManager.ObserverNotificationAsync(ctx, *sender, receiverAddrs, observableType, observableName, data)
	if err2 != nil {
		s.log.V(0).Error(err, "failed to notify grains")
	}
	return errors.CombineErrors(err2, grainAddrErrs)
}

type localTransport struct {
	log               logr.Logger
	codec             codec.Codec
	localGrainManager *GrainActivationManagerImpl
	h                 cluster.TransportHandler
}

func (t *localTransport) Listen(handler cluster.TransportHandler) error {
	t.h = handler
	return nil
}

func (*localTransport) Stop() error { return nil }

func (t *localTransport) EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error {
	t.h.ReceiveInvokeMethodRequest(ctx, sender, receiver, method, uuid, payload)
	return nil
}

func (t *localTransport) EnqueueRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte) error {
	t.h.ReceiveRegisterObserverRequest(ctx, observer, observable, name, uuid, payload)
	return nil
}

func (t *localTransport) EnqueueObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte) error {
	t.h.ReceiveObserverNotification(ctx, sender, receivers, observableType, name, payload)
	return nil
}

func (t *localTransport) EnqueueAckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte) error {
	t.h.ReceiveAckRegisterObserver(ctx, receiver, uuid, errOut)
	return nil
}

func (t *localTransport) EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error {
	t.h.ReceiveInvokeMethodResponse(ctx, receiver, uuid, payload, err)
	return nil
}
