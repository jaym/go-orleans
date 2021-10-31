package silo_test

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/go-logr/stdr"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"

	gcontext "github.com/jaym/go-orleans/context"
	examples "github.com/jaym/go-orleans/examples/proto"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/plugins/observers/psql"
	"github.com/jaym/go-orleans/silo"
)

type ChirperGrainActivatorTestImpl struct {
}

func (*ChirperGrainActivatorTestImpl) Activate(ctx context.Context, identity grain.Identity, services examples.ChirperGrainServices) (examples.ChirperGrain, error) {
	g := &ChirperGrainImpl{
		Identity: identity,
		services: services,
	}

	coreServices := services.CoreGrainServices()

	coreServices.TimerService().RegisterTimer("hello", time.Second, func() {
		fmt.Printf("Got timer: %v\n", g.Identity)
	})

	if identity.ID == "u2" {
		g1Identity := identity
		g1Identity.ID = "u1"
		g1 := examples.GetChirperGrain(services.CoreGrainServices().SiloClient(), g1Identity)
		g1.ObserveMessage(ctx, g, &examples.SubscribeRequest{})
	}
	return g, nil
}

type ChirperGrainImpl struct {
	grain.Identity
	services examples.ChirperGrainServices
}

var ErrTestIt = errors.New("Testing an error")

func (g *ChirperGrainImpl) PublishMessage(ctx context.Context, req *examples.PublishMessageRequest) (*examples.PublishMessageResponse, error) {
	fmt.Printf("%v got message %q\n", g.Identity, req.Msg)
	if g.ID == "err" {
		return nil, errors.Wrap(ErrTestIt, "testing out an error")
	}
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

func (g *ChirperGrainImpl) RegisterMessageObserver(ctx context.Context, observer grain.Identity, req *examples.SubscribeRequest) error {
	return g.services.AddMessageObserver(ctx, observer, req)
}

func (g *ChirperGrainImpl) OnNotifyMessage(ctx context.Context, req *examples.ChirpMessage) error {
	fmt.Printf("%v got notification %q\n", g.Identity, req.Msg)
	return nil
}

func (g *ChirperGrainImpl) Deactivate(ctx context.Context) {
	fmt.Printf("Deactivating %v\n", g.Identity)
}

func TestItAll(t *testing.T) {
	database := embeddedpostgres.NewDatabase()
	if err := database.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Stop(); err != nil {
			t.Fatal(err)
		}
	}()

	pool, err := pgxpool.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		t.Fatal(err)
	}

	stdDb, err := sql.Open("pgx", pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}

	if err := psql.SetupDatabase(stdDb); err != nil {
		t.Fatal(err)
	}

	if err := stdDb.Close(); err != nil {
		t.Fatal(err)
	}

	stdr.SetVerbosity(4)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})

	observerStore := psql.NewObserverStore(log.WithName("observerstore"), pool, psql.WithCodec(protobuf.NewCodec()))
	s := silo.NewSilo(log, observerStore)
	examples.RegisterChirperGrainActivator(s, &ChirperGrainActivatorTestImpl{})
	s.Start()

	in := &examples.PublishMessageRequest{
		Msg: "world",
	}

	for i := 0; i < 10; i++ {
		a := grain.Identity{
			GrainType: "ChirperGrain",
			ID:        fmt.Sprintf("g%d", i),
		}
		ref := examples.GetChirperGrain(s.Client(), a)
		fmt.Printf("Calling g%d\n", i)
		_, err := ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}

	g1Identity := grain.Identity{
		GrainType: "ChirperGrain",
		ID:        "u1",
	}

	g2Identity := grain.Identity{
		GrainType: "ChirperGrain",
		ID:        "u2",
	}

	gErrIdentity := grain.Identity{
		GrainType: "ChirperGrain",
		ID:        "err",
	}

	chirperGrain1Ref := examples.GetChirperGrain(s.Client(), g1Identity)
	chirperGrain2Ref := examples.GetChirperGrain(s.Client(), g2Identity)
	identity, err := examples.CreateChirperGrainMessageObserver(context.Background(), s,
		func(ctx context.Context, req *examples.ChirpMessage) error {
			fmt.Printf("anonymous grain got notification: %q\n", req.Msg)
			return nil
		})
	require.NoError(t, err)
	err = chirperGrain1Ref.ObserveMessage(context.Background(), identity, &examples.SubscribeRequest{})
	require.NoError(t, err)

	resp, err := chirperGrain2Ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.NoError(t, err)

	resp, err = chirperGrain1Ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.NoError(t, err)
	require.Equal(t, "hello world", resp.Foobar)

	resp, err = chirperGrain1Ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.NoError(t, err)
	resp, err = chirperGrain1Ref.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.NoError(t, err)

	chirperGrainErrRef := examples.GetChirperGrain(s.Client(), gErrIdentity)
	_, err = chirperGrainErrRef.PublishMessage(gcontext.WithIdentityContext(context.Background(), grain.Identity{}), in)
	require.True(t, errors.Is(err, ErrTestIt))
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	require.Fail(t, "testing")
}
