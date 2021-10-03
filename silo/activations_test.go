package silo_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	examples "github.com/jaym/go-orleans/examples/proto"
	"github.com/jaym/go-orleans/silo"
	"github.com/stretchr/testify/require"
)

type ChirperGrainActivatorTestImpl struct {
}

func (*ChirperGrainActivatorTestImpl) Activate(ctx context.Context, address silo.Address, services examples.ChirperGrainServices) (examples.ChirperGrain, error) {
	g := &ChirperGrainImpl{
		Address:  address,
		services: services,
	}

	if address.ID == "u2" {
		g1Address := address
		g1Address.ID = "u1"
		g1 := examples.GetChirperGrain(services.SiloClient(), g1Address)
		g1.ObserveMessage(ctx, g, &examples.SubscribeRequest{})
	}
	return g, nil
}

type ChirperGrainImpl struct {
	silo.Address
	services examples.ChirperGrainServices
}

func (g *ChirperGrainImpl) PublishMessage(ctx context.Context, req *examples.PublishMessageRequest) (*examples.PublishMessageResponse, error) {
	fmt.Printf("%v got message %q\n", g.Address, req.Msg)
	observers, err := g.services.ListMessageObservers(ctx)
	if err != nil {
		return nil, err
	}
	err = g.services.NotifyMessageObservers(ctx, observers, &examples.ChirpMessage{
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

type registrarEntry struct {
	Description *silo.GrainDescription
	Impl        interface{}
}
type TestRegistrar struct {
	entries map[string]registrarEntry
}

func (r *TestRegistrar) Register(desc *silo.GrainDescription, impl interface{}) {
	r.entries[desc.GrainType] = registrarEntry{
		Description: desc,
		Impl:        impl,
	}
}

func (r *TestRegistrar) Lookup(grainType string) (*silo.GrainDescription, interface{}, error) {
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
	examples.RegisterChirperGrainActivator(registrar, impl)

	s := silo.NewSilo(registrar)
	in := &examples.PublishMessageRequest{
		Msg: "world",
	}

	g1Address := silo.Address{
		Location:  "local",
		GrainType: "ChirperGrain",
		ID:        "u1",
	}

	g2Address := silo.Address{
		Location:  "local",
		GrainType: "ChirperGrain",
		ID:        "u2",
	}

	chirperGrain1Ref := examples.GetChirperGrain(s.Client(), g1Address)
	chirperGrain2Ref := examples.GetChirperGrain(s.Client(), g2Address)

	resp, err := chirperGrain2Ref.PublishMessage(silo.WithAddressContext(context.Background(), silo.Address{
		Location: "local",
	}), in)
	require.NoError(t, err)

	resp, err = chirperGrain1Ref.PublishMessage(silo.WithAddressContext(context.Background(), silo.Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	require.Equal(t, "hello world", resp.Foobar)

	resp, err = chirperGrain1Ref.PublishMessage(silo.WithAddressContext(context.Background(), silo.Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(silo.WithAddressContext(context.Background(), silo.Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(silo.WithAddressContext(context.Background(), silo.Address{
		Location: "local",
	}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(silo.WithAddressContext(context.Background(), silo.Address{
		Location: "local",
	}), in)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	require.Fail(t, "testing")
}
