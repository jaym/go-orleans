// Code generated by protoc-gen-goor. DO NOT EDIT.
package examples

import (
	context "context"
	grain "github.com/jaym/go-orleans/grain"
	descriptor "github.com/jaym/go-orleans/grain/descriptor"
	generic "github.com/jaym/go-orleans/grain/generic"
	services "github.com/jaym/go-orleans/grain/services"
)

type ChirperGrainServices interface {
	CoreGrainServices() services.CoreGrainServices
	NotifyMessageObservers(ctx context.Context, observers []grain.RegisteredObserver, val *ChirpMessage) error
	ListMessageObservers(ctx context.Context) ([]grain.RegisteredObserver, error)
	AddMessageObserver(ctx context.Context, observer grain.Identity, req *SubscribeRequest) error
	RemoveMessageObserver(ctx context.Context, observer grain.Identity) error
}

type impl_ChirperGrainServices struct {
	observerManager services.GrainObserverManager
	coreServices    services.CoreGrainServices
}

func (m *impl_ChirperGrainServices) CoreGrainServices() services.CoreGrainServices {
	return m.coreServices
}

func (m *impl_ChirperGrainServices) NotifyMessageObservers(ctx context.Context, observers []grain.RegisteredObserver, val *ChirpMessage) error {
	return m.observerManager.Notify(ctx, ChirperGrain_GrainDesc.Observables[0].Name, observers, val)
}

func (m *impl_ChirperGrainServices) ListMessageObservers(ctx context.Context) ([]grain.RegisteredObserver, error) {
	return m.observerManager.List(ctx, ChirperGrain_GrainDesc.Observables[0].Name)
}

func (m *impl_ChirperGrainServices) AddMessageObserver(ctx context.Context, observer grain.Identity, req *SubscribeRequest) error {
	_, err := m.observerManager.Add(ctx, ChirperGrain_GrainDesc.Observables[0].Name, observer, req)
	return err
}

func (m *impl_ChirperGrainServices) RemoveMessageObserver(ctx context.Context, observer grain.Identity) error {
	return m.observerManager.Remove(ctx, ChirperGrain_GrainDesc.Observables[0].Name, observer)
}

type ChirperGrainActivator interface {
	Activate(ctx context.Context, identity grain.Identity, services ChirperGrainServices) (ChirperGrain, error)
}

func RegisterChirperGrainActivator(registrar descriptor.Registrar, activator ChirperGrainActivator) {
	registrar.Register(&ChirperGrain_GrainDesc, activator)
}

type ChirperGrain interface {
	grain.GrainReference
	PublishMessage(ctx context.Context, req *PublishMessageRequest) (*PublishMessageResponse, error)
	RegisterMessageObserver(ctx context.Context, observer grain.Identity, req *SubscribeRequest) error
	UnsubscribeMessageObserver(ctx context.Context, observer grain.Identity) error
}

type ChirperGrainMessageObserver interface {
	grain.GrainReference
	OnNotifyMessage(ctx context.Context, req *ChirpMessage) error
}

func CreateChirperGrainMessageStream(g *generic.Grain) (*ChirperGrainMessageStream, error) {
	desc := ChirperGrain_GrainDesc.Observables[0]
	genericStream, err := g.CreateStream(ChirperGrain_GrainDesc.GrainType, desc.Name)
	if err != nil {
		return nil, err
	}

	stream := &ChirperGrainMessageStream{
		Stream: genericStream,
		c:      make(chan ChirperGrainMessageStreamMessage),
	}

	go func() {
		c := stream.Stream.C()
		for {
			select {
			case <-stream.Stream.Done():
				return
			case msg := <-c:
				m := ChirperGrainMessageStreamMessage{
					Sender: msg.Sender,
				}
				val := new(ChirpMessage)
				if err := msg.Decode(val); err != nil {
					m.Err = err
				} else {
					m.Value = val
				}
				select {
				case stream.c <- m:
				default:
				}
			}
		}
	}()

	return stream, nil
}

type ChirperGrainMessageStreamMessage struct {
	Sender grain.Identity
	Value  *ChirpMessage
	Err    error
}

type ChirperGrainMessageStream struct {
	generic.Stream
	c chan ChirperGrainMessageStreamMessage
}

func (s *ChirperGrainMessageStream) C() <-chan ChirperGrainMessageStreamMessage {
	return s.c
}

type ChirperGrainRef interface {
	grain.GrainReference
	PublishMessage(ctx context.Context, req *PublishMessageRequest) (*PublishMessageResponse, error)
	ObserveMessage(ctx context.Context, observer grain.GrainReference, req *SubscribeRequest) error
	UnsubscribeMessage(ctx context.Context, observer grain.GrainReference) error
}

var ChirperGrain_GrainDesc = descriptor.GrainDescription{
	GrainType: "ChirperGrain",
	Activation: descriptor.ActivationDesc{
		Handler: _ChirperGrain_Activate,
	},
	Methods: []descriptor.MethodDesc{
		{
			Name:    "PublishMessage",
			Handler: _ChirperGrain_PublishMessage_MethodHandler,
		},
	},
	Observables: []descriptor.ObservableDesc{
		{
			Name:               "Message",
			Handler:            _ChirperGrain_Message_ObserverHandler,
			RegisterHandler:    _ChirperGrain_Message_RegisterObserverHandler,
			UnsubscribeHandler: _ChirperGrain_Message_UnsubscribeObserverHandler,
		},
	},
}

func _ChirperGrain_Activate(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, observerManager services.GrainObserverManager, identity grain.Identity) (grain.GrainReference, error) {
	grainServices := &impl_ChirperGrainServices{
		observerManager: observerManager,
		coreServices:    coreServices,
	}
	return activator.(ChirperGrainActivator).Activate(ctx, identity, grainServices)
}

func _ChirperGrain_PublishMessage_MethodHandler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(PublishMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}

	return srv.(ChirperGrain).PublishMessage(ctx, in)
}

func _ChirperGrain_Message_ObserverHandler(srv interface{}, ctx context.Context, dec func(interface{}) error) error {
	in := new(ChirpMessage)
	if err := dec(in); err != nil {
		return err
	}

	return srv.(ChirperGrainMessageObserver).OnNotifyMessage(ctx, in)
}
func _ChirperGrain_Message_RegisterObserverHandler(srv interface{}, ctx context.Context, observer grain.Identity, dec func(interface{}) error) error {
	in := new(SubscribeRequest)
	if err := dec(in); err != nil {
		return err
	}

	return srv.(ChirperGrain).RegisterMessageObserver(ctx, observer, in)
}
func _ChirperGrain_Message_UnsubscribeObserverHandler(srv interface{}, ctx context.Context, observer grain.Identity) error {
	return srv.(ChirperGrain).UnsubscribeMessageObserver(ctx, observer)
}

type _grainClient_ChirperGrain struct {
	grain.Identity
	siloClient grain.SiloClient
}

func GetChirperGrain(siloClient grain.SiloClient, identity grain.Identity) ChirperGrainRef {
	return &_grainClient_ChirperGrain{
		Identity:   identity,
		siloClient: siloClient,
	}
}

func (c *_grainClient_ChirperGrain) PublishMessage(ctx context.Context, req *PublishMessageRequest) (*PublishMessageResponse, error) {
	f := c.siloClient.InvokeMethod(ctx, c.Identity, ChirperGrain_GrainDesc.GrainType, ChirperGrain_GrainDesc.Methods[0].Name, req)
	resp, err := f.Await(ctx)
	if err != nil {
		return nil, err
	}
	out := new(PublishMessageResponse)
	if err := resp.Get(out); err != nil {
		return nil, err
	}
	return out, nil
}
func (c *_grainClient_ChirperGrain) ObserveMessage(ctx context.Context, observer grain.GrainReference, req *SubscribeRequest) error {
	f := c.siloClient.RegisterObserver(ctx, observer.GetIdentity(), c.GetIdentity(), ChirperGrain_GrainDesc.Observables[0].Name, req)
	err := f.Await(ctx)
	if err != nil {
		return err
	}
	return nil
}
func (c *_grainClient_ChirperGrain) UnsubscribeMessage(ctx context.Context, observer grain.GrainReference) error {
	f := c.siloClient.UnsubscribeObserver(ctx, observer.GetIdentity(), c.GetIdentity(), ChirperGrain_GrainDesc.Observables[0].Name)
	err := f.Await(ctx)
	if err != nil {
		return err
	}
	return nil
}
