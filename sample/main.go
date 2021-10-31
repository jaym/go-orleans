package main

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/go-logr/stdr"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	examples "github.com/jaym/go-orleans/examples/proto"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/plugins/discovery/static"
	"github.com/jaym/go-orleans/plugins/membership/memberlist"
	"github.com/jaym/go-orleans/plugins/observers/psql"
	"github.com/jaym/go-orleans/silo"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/pkg/errors"
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

func main() {
	var port int
	switch os.Args[1] {
	case "node1":
		port = 9991
	case "node2":
		port = 9992
	default:
		panic("wrong")
	}

	if os.Getenv("START_PG") == "true" {
		database := embeddedpostgres.NewDatabase()
		if err := database.Start(); err != nil {
			panic(err)
		}
		defer func() {
			if err := database.Stop(); err != nil {
			}
		}()
	}

	pool, err := pgxpool.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		panic(err)
	}

	stdDb, err := sql.Open("pgx", pool.Config().ConnString())
	if err != nil {
		panic(err)
	}

	if err := psql.SetupDatabase(stdDb); err != nil {
		// panic(err)
	}

	if err := stdDb.Close(); err != nil {
		panic(err)
	}

	stdr.SetVerbosity(4)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})

	observerStore := psql.NewObserverStore(log.WithName("observerstore"), pool, psql.WithCodec(protobuf.NewCodec()))
	d := static.New([]string{"127.0.0.1:9991", "127.0.0.1:9992"})

	mp := memberlist.New(log, cluster.Location(os.Args[1]), port)
	s := silo.NewSilo(log, observerStore, silo.WithNodeName(os.Args[1]), silo.WithDiscovery(d), silo.WithMembershipProtocol(mp))
	examples.RegisterChirperGrainActivator(s, &ChirperGrainActivatorTestImpl{})
	if err := s.Start(); err != nil {
		panic(err)
	}

	stop := make(chan os.Signal, 1)

	// Register the signals we want to be notified, these 3 indicate exit
	// signals, similar to CTRL+C
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	<-stop
}
