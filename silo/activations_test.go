package silo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	examples "github.com/jaym/go-orleans/examples/proto"
	"github.com/stretchr/testify/require"
)

type ChirperGrainMessageObserverRef interface {
	Addressable
	OnNotifyMessage(context.Context, *examples.ChirpMessage) error
}

type ChirperGrain interface {
	Addressable
	PublishMessage(context.Context, *examples.PublishMessageRequest) (*examples.PublishMessageResponse, error)
}

type ChirperGrainRef interface {
	ChirperGrain
	ObserveMessage(ctx context.Context, observer ChirperGrainMessageObserverRef, req *examples.SubscribeRequest) error
}

type ChirperGrainActivator interface {
	Activate(ctx context.Context, address Address, services ChirperGrainServices) (ChirperGrain, error)
}

type ChirperGrainServices interface {
	SiloClient() SiloClient
	NotifyAllMessageObservers(ctx context.Context, o []RegisteredObserver, val *examples.ChirpMessage) error
	ListMessageObservers(ctx context.Context) ([]RegisteredObserver, error)
}

type chirperGrainObserverManager struct {
	observerManager ObserverManager
	siloClient      SiloClient
}

func (m *chirperGrainObserverManager) SiloClient() SiloClient {
	return m.siloClient
}

func (m *chirperGrainObserverManager) NotifyAllMessageObservers(ctx context.Context, o []RegisteredObserver, val *examples.ChirpMessage) error {
	return m.observerManager.Notify(ctx, ChirperGrain_GrainDesc.Observables[0].Name, o, val)
}

func (m *chirperGrainObserverManager) ListMessageObservers(ctx context.Context) ([]RegisteredObserver, error) {
	return m.observerManager.List(ctx, ChirperGrain_GrainDesc.Observables[0].Name)

}

var ChirperGrain_GrainDesc = GrainDescription{
	GrainType: "ChirperGrain",
	Activation: ActivationDesc{
		Handler: _ChirperGrain_Activate,
	},
	Methods: []MethodDesc{
		{
			Name:    "PublishMessage",
			Handler: _ChirperGrain_PublishMessage_GrainHandler,
		},
	},
	Observables: []ObservableDesc{
		{
			Name: "Message",
			Handler: func(srv interface{}, ctx context.Context, observerAddress Address, dec func(interface{}) error) error {
				in := new(examples.ChirpMessage)
				if err := dec(in); err != nil {
					return err
				}
				obsv, ok := srv.(ChirperGrainMessageObserverRef)
				if !ok {
					// TODO: don't panic
					panic("Invalid type")
				}
				return obsv.OnNotifyMessage(ctx, in)
			},
		},
	},
}

func _ChirperGrain_Activate(activator interface{}, ctx context.Context, siloClient SiloClient, o ObserverManager, address Address) (Addressable, error) {
	grainServices := &chirperGrainObserverManager{
		observerManager: o,
		siloClient:      siloClient,
	}
	return activator.(ChirperGrainActivator).Activate(ctx, address, grainServices)
}

func _ChirperGrain_PublishMessage_GrainHandler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(examples.PublishMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}

	return srv.(ChirperGrain).PublishMessage(ctx, in)
}

type chirperGrainClient struct {
	Address
	siloClient SiloClient
}

func (c *chirperGrainClient) PublishMessage(ctx context.Context, req *examples.PublishMessageRequest) (*examples.PublishMessageResponse, error) {
	f := c.siloClient.InvokeMethod(ctx, c.Address, ChirperGrain_GrainDesc.GrainType, ChirperGrain_GrainDesc.Methods[0].Name, req)
	resp, err := f.Await(ctx)
	if err != nil {
		return nil, err
	}
	out := new(examples.PublishMessageResponse)
	if err := resp.Get(out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chirperGrainClient) ObserveMessage(ctx context.Context, observer ChirperGrainMessageObserverRef,
	req *examples.SubscribeRequest) error {

	f := c.siloClient.RegisterObserver(ctx, observer.GetAddress(), c.GetAddress(), ChirperGrain_GrainDesc.Observables[0].Name, req)
	resp, err := f.Await(ctx)
	if err != nil {
		return err
	}

	if len(resp.err) > 0 {
		return errors.New(string(resp.err))
	}
	return nil
}

func GetChirperGrain(siloClient SiloClient, address Address) ChirperGrainRef {
	return &chirperGrainClient{
		Address:    address,
		siloClient: siloClient,
	}
}

type ChirperGrainActivatorTestImpl struct {
}

func (*ChirperGrainActivatorTestImpl) Activate(ctx context.Context, address Address, services ChirperGrainServices) (ChirperGrain, error) {
	g := &ChirperGrainImpl{
		Address:  address,
		services: services,
	}

	if address.ID == "u2" {
		g1Address := address
		g1Address.ID = "u1"
		g1 := GetChirperGrain(services.SiloClient(), g1Address)
		g1.ObserveMessage(ctx, g, &examples.SubscribeRequest{})
	}
	return g, nil
}

type ChirperGrainImpl struct {
	Address
	services ChirperGrainServices
}

func (g *ChirperGrainImpl) PublishMessage(ctx context.Context, req *examples.PublishMessageRequest) (*examples.PublishMessageResponse, error) {
	fmt.Printf("%v got message %q\n", g.Address, req.Msg)
	observers, err := g.services.ListMessageObservers(ctx)
	if err != nil {
		return nil, err
	}
	err = g.services.NotifyAllMessageObservers(ctx, observers, &examples.ChirpMessage{
		Msg: "You've been notified",
	})
	if err != nil {
		return nil, err
	}
	return &examples.PublishMessageResponse{
		Foobar: "hello " + req.Msg,
	}, nil
}
func (g *ChirperGrainImpl) OnNotifyMessage(ctx context.Context, req *examples.ChirpMessage) error {
	fmt.Printf("%v got notification %q\n", g.Address, req.Msg)
	return nil
}

func RegisterChirperGrainActivator(registrar Registrar, activator ChirperGrainActivator) {
	registrar.Register(&ChirperGrain_GrainDesc, activator)
}

type registrarEntry struct {
	Description *GrainDescription
	Impl        interface{}
}
type TestRegistrar struct {
	entries map[string]registrarEntry
}

func (r *TestRegistrar) Register(desc *GrainDescription, impl interface{}) {
	r.entries[desc.GrainType] = registrarEntry{
		Description: desc,
		Impl:        impl,
	}
}

func (r *TestRegistrar) Lookup(grainType string) (*GrainDescription, interface{}, error) {
	e, ok := r.entries[grainType]
	if !ok {
		return nil, nil, errors.New("no impl")
	}
	return e.Description, e.Impl, nil
}

func TestItAll(t *testing.T) {
	registrar := &TestRegistrar{
		entries: make(map[string]registrarEntry),
	}
	impl := &ChirperGrainActivatorTestImpl{}
	RegisterChirperGrainActivator(registrar, impl)

	silo := NewSilo(registrar)
	in := &examples.PublishMessageRequest{
		Msg: "world",
	}

	g1Address := Address{
		Location:  "local",
		GrainType: "ChirperGrain",
		ID:        "u1",
	}

	g2Address := Address{
		Location:  "local",
		GrainType: "ChirperGrain",
		ID:        "u2",
	}

	chirperGrain1Ref := GetChirperGrain(silo.Client(), g1Address)
	chirperGrain2Ref := GetChirperGrain(silo.Client(), g2Address)

	resp, err := chirperGrain2Ref.PublishMessage(WithAddressContext(context.Background(), Address{
		Location: "local",
	}), in)
	require.NoError(t, err)

	resp, err = chirperGrain1Ref.PublishMessage(WithAddressContext(context.Background(), Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	require.Equal(t, "hello world", resp.Foobar)

	resp, err = chirperGrain1Ref.PublishMessage(WithAddressContext(context.Background(), Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(WithAddressContext(context.Background(), Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	resp, err = chirperGrain2Ref.PublishMessage(WithAddressContext(context.Background(), Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(WithAddressContext(context.Background(), Address{
		Location: "local",
	}), in)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	require.Fail(t, "testing")
}
