package psql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	graindir_psql "github.com/jaym/go-orleans/plugins/graindir/psql"
	"github.com/jaym/go-orleans/plugins/membership/memberlist"
	"github.com/jaym/go-orleans/plugins/observers/psql"
	observer_psql "github.com/jaym/go-orleans/plugins/observers/psql"
	"github.com/jaym/go-orleans/plugins/transport/grpc"
	"github.com/jaym/go-orleans/silo"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type setupOptions struct {
	logr logr.Logger

	pgUser string
	pgHost string
	pgPort int
	pgPass string
	pgDB   string

	nodeName       string
	rpcAddr        string
	membershipAddr string

	siloOpts []silo.SiloOption
}

type SetupOpt func(*setupOptions)

func WithDatasource(pgHost string, pgPort int, pgUser string, pgPass string, pgDB string) SetupOpt {
	return func(so *setupOptions) {
		so.pgHost = pgHost
		so.pgPort = pgPort
		so.pgUser = pgUser
		so.pgPass = pgPass
		so.pgDB = pgDB
	}
}

func WithPGEnvironment() SetupOpt {
	return func(so *setupOptions) {
		pgUser := os.Getenv("PG_USER")
		pgHost := os.Getenv("PG_HOST")
		pgPort := os.Getenv("PG_PORT")
		pgPass := os.Getenv("PG_PASSWORD")
		pgDB := os.Getenv("PG_DATABASE")

		if pgUser != "" {
			so.pgUser = pgUser
		}

		if pgPass != "" {
			so.pgPass = pgPass
		}

		if pgPort != "" {
			port, _ := strconv.ParseInt(pgPort, 10, 16)
			so.pgPort = int(port)
		}

		if pgHost != "" {
			so.pgHost = pgHost
		}

		if pgDB != "" {
			so.pgDB = pgDB
		}

	}
}

func WithLogr(l logr.Logger) SetupOpt {
	return func(so *setupOptions) {
		so.logr = l
	}
}

func WithSiloOptions(opts ...silo.SiloOption) SetupOpt {
	return func(so *setupOptions) {
		so.siloOpts = opts
	}
}

func WithRPCAddr(addr string) SetupOpt {
	return func(so *setupOptions) {
		so.rpcAddr = addr
	}
}

func WithMembershipAddr(addr string) SetupOpt {
	return func(so *setupOptions) {
		so.membershipAddr = addr
	}
}

func WithNodeName(nodeName string) SetupOpt {
	return func(so *setupOptions) {
		so.nodeName = nodeName
	}
}

func Setup(ctx context.Context, opts ...SetupOpt) (*silo.Silo, error) {
	sOpts, err := parseOpts(opts...)
	if err != nil {
		return nil, err
	}

	poolObservers, err := setupDatabase(context.Background(), sOpts, "observers", observer_psql.SetupDatabase)
	if err != nil {
		return nil, err
	}

	poolGrainDir, err := setupDatabase(context.Background(), sOpts, "graindir", graindir_psql.SetupDatabase)
	if err != nil {
		return nil, err
	}

	observerStore := observer_psql.NewObserverStore(sOpts.logr.WithName("observerstore"),
		poolObservers, psql.WithCodec(protobuf.NewCodec()))

	grainDir := graindir_psql.NewGrainDirectory(sOpts.logr.WithName("graindir"), poolGrainDir)

	orlServer, err := grpc.New(sOpts.logr.WithName("grpc"), sOpts.nodeName, sOpts.rpcAddr)
	if err != nil {
		return nil, err
	}
	if err := orlServer.Start(); err != nil {
		return nil, err
	}

	membershipAddr, err := net.ResolveTCPAddr("", sOpts.membershipAddr)
	if err != nil {
		return nil, err
	}

	rpcAddr, err := net.ResolveTCPAddr("", sOpts.rpcAddr)
	if err != nil {
		return nil, err
	}

	mp := memberlist.New(sOpts.logr, cluster.Location(sOpts.nodeName), membershipAddr.Port, rpcAddr.Port)
	siloOpts := append([]silo.SiloOption{
		silo.WithNodeName(sOpts.nodeName),
		silo.WithMembership(mp, orlServer),
		silo.WithGrainDirectory(grainDir),
	}, sOpts.siloOpts...)
	s := silo.NewSilo(sOpts.logr, observerStore, siloOpts...)

	return s, nil
}

func parseOpts(opts ...SetupOpt) (*setupOptions, error) {
	hostname, _ := os.Hostname()

	sOpts := setupOptions{
		pgUser:         "postgres",
		pgPass:         "postgres",
		pgHost:         "localhost",
		pgPort:         5432,
		pgDB:           "orleans",
		rpcAddr:        ":8990",
		membershipAddr: ":9990",
		nodeName:       hostname,
		logr:           stdr.New(log.Default()),
	}
	for _, o := range opts {
		o(&sOpts)
	}

	return &sOpts, nil
}

type migrateFunc func(*sql.DB) error

func setupDatabase(ctx context.Context, sOpts *setupOptions, name string, f migrateFunc) (*pgxpool.Pool, error) {
	source := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?search_path=%s",
		sOpts.pgUser, sOpts.pgPass, sOpts.pgHost, sOpts.pgPort, sOpts.pgDB, name)

	err := func() error {
		stdDb, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:%d/%s", sOpts.pgUser, sOpts.pgPass, sOpts.pgHost, sOpts.pgPort, sOpts.pgDB))
		if err != nil {
			return err
		}
		defer stdDb.Close()
		if _, err := stdDb.Exec("CREATE SCHEMA IF NOT EXISTS " + name); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}
	stdDb, err := sql.Open("pgx", source)
	if err != nil {
		return nil, err
	}
	defer stdDb.Close()

	if err := f(stdDb); err != nil {
		return nil, err
	}

	return pgxpool.Connect(ctx, source)
}
