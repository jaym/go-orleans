package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/go-logr/logr"
	"github.com/go-logr/zerologr"
	_ "github.com/jackc/pgx/v4/stdlib"
	examples "github.com/jaym/go-orleans/examples/proto"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/plugins/discovery/static"
	graindir_psql "github.com/jaym/go-orleans/plugins/graindir/psql"
	"github.com/jaym/go-orleans/plugins/membership/memberlist"
	"github.com/jaym/go-orleans/plugins/observers/psql"
	observer_psql "github.com/jaym/go-orleans/plugins/observers/psql"
	"github.com/jaym/go-orleans/plugins/transport/grpc"
	"github.com/jaym/go-orleans/silo"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
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
	fmt.Printf(">>>> %v got message %q\n", g.Identity, req.Msg)
	if g.ID == "err" {
		return nil, errors.Wrap(ErrTestIt, "testing out an error")
	}
	observers, err := g.services.ListMessageObservers(ctx)
	if err != nil {
		return nil, err
	}
	err = g.services.NotifyMessageObservers(ctx, observers, &examples.ChirpMessage{
		Msg: fmt.Sprintf("You've been notified: %s", req.Msg),
	})
	if err != nil {
		return nil, err
	}
	return &examples.PublishMessageResponse{
		Foobar: "hello " + req.Msg,
	}, nil
}

func (g *ChirperGrainImpl) RegisterMessageObserver(ctx context.Context, observer grain.Identity, registrationTimeout time.Duration, req *examples.SubscribeRequest) error {
	return g.services.AddMessageObserver(ctx, observer, registrationTimeout, req)
}

func (g *ChirperGrainImpl) UnsubscribeMessageObserver(ctx context.Context, observer grain.Identity) error {
	return g.services.RemoveMessageObserver(ctx, observer)
}

func (g *ChirperGrainImpl) OnNotifyMessage(ctx context.Context, req *examples.ChirpMessage) error {
	fmt.Printf("%v got notification %q\n", g.Identity, req.Msg)
	return nil
}

func (g *ChirperGrainImpl) Deactivate(ctx context.Context) {
	fmt.Printf("Deactivating %v\n", g.Identity)
}

func main() {
	var membershipPort int
	var rpcPort int
	switch os.Args[1] {
	case "node1":
		membershipPort = 9991
		rpcPort = 8991
	case "node2":
		membershipPort = 9992
		rpcPort = 8992
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

	poolObservers, err := setupDatabase(context.Background(), "observers", observer_psql.SetupDatabase)
	if err != nil {
		panic(err)
	}

	poolGrainDir, err := setupDatabase(context.Background(), "graindir", graindir_psql.SetupDatabase)
	if err != nil {
		panic(err)
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerologr.NameFieldName = "logger"
	zerologr.NameSeparator = "/"

	zl := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})
	zl = zl.With().Timestamp().Logger()
	var log logr.Logger = zerologr.New(&zl)

	observerStore := observer_psql.NewObserverStore(log.WithName("observerstore"), poolObservers, psql.WithCodec(protobuf.NewCodec()))
	d := static.New([]string{"127.0.0.1:9991", "127.0.0.1:9992"})

	grainDir := graindir_psql.NewGrainDirectory(log.WithName("graindir"), poolGrainDir)

	orlServer, err := grpc.New(log.WithName("grpc"), os.Args[1], fmt.Sprintf("127.0.0.1:%d", rpcPort))
	if err != nil {
		panic(err)
	}
	if err := orlServer.Start(); err != nil {
		panic(err)
	}

	mp := memberlist.New(log, cluster.Location(os.Args[1]), membershipPort, rpcPort)
	s := silo.NewSilo(log, observerStore, silo.WithNodeName(os.Args[1]),
		silo.WithNodeName(os.Args[1]),
		silo.WithDiscovery(d),
		silo.WithMembership(mp, orlServer),
		silo.WithGrainDirectory(grainDir))
	examples.RegisterChirperGrainActivator(s, &ChirperGrainActivatorTestImpl{})
	if err := s.Start(context.Background()); err != nil {
		panic(err)
	}

	stop := make(chan os.Signal, 1)

	// Register the signals we want to be notified, these 3 indicate exit
	// signals, similar to CTRL+C
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	subscriber, err := s.CreateGrain()
	if err != nil {
		panic(err)
	}

	stream, err := examples.CreateChirperGrainMessageStream(subscriber)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			c := stream.C()

			select {
			case <-stream.Done():
				return
			case msg := <-c:
				if msg.Err != nil {
					fmt.Printf(">>>>>>>>>>>>>>> SUBSCRIBER GOT ERROR: %v\n", msg.Err)
				} else {
					fmt.Printf(">>>>>>>>>>>>>>> SUBSCRIBER GOT MSG FROM %q: %v\n", msg.Sender, msg.Value)
				}
			}
		}
	}()

	err = stream.Observe(context.Background(), grain.Identity{"ChirperGrain", "g2"}, &examples.SubscribeRequest{})
	if err != nil {
		panic(err)
	}

	closeChan := make(chan struct{})
	if os.Args[1] == "node1" {
		for i := 0; i < 1; i++ {
			go func(log logr.Logger, client grain.SiloClient) {
				time.Sleep(2 * time.Second)
				ctx := context.Background()
				for {
					select {
					case <-closeChan:
						return
					default:
					}
					//i := rand.Intn(64)
					i := 2
					gident := grain.Identity{
						GrainType: "ChirperGrain",
						ID:        fmt.Sprintf("g%d", i),
					}
					log.Info("Calling grain", "grain", gident)

					grainRef := examples.GetChirperGrain(client, gident)
					resp, err := grainRef.PublishMessage(ctx, &examples.PublishMessageRequest{
						Msg: fmt.Sprintf("calling for %d", i),
					})
					if err != nil {
						log.Error(err, "failed to call chirper grain", "grain", gident)
					} else {
						log.Info("got response", "msg", resp.Foobar)
					}
					time.Sleep(1 * time.Second)
				}

			}(log.WithName("client"), s.Client())
		}
	}
	<-stop
	close(closeChan)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		panic(err)
	}
}
