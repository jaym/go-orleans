package psql_test

import (
	"context"
	"database/sql"
	stdlog "log"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/go-logr/stdr"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/graindir/psql"
	"github.com/jaym/go-orleans/plugins/graindir/psql/internal"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

func TestPsqlStore(t *testing.T) {
	stdr.SetVerbosity(4)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.All})

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

	if err := internal.Migrate(stdDb); err != nil {
		t.Fatal(err)
	}

	if err := stdDb.Close(); err != nil {
		t.Fatal(err)
	}

	store := psql.NewGrainDirectory(log, pool)
	ctx := context.Background()

	t.Run("activate/lookup/delete", func(t *testing.T) {
		node1 := "node1"
		node2 := "node2"
		grain1 := grainIdent("tester1", "grain1")
		grain2 := grainIdent("tester2", "grain2")

		_, err := store.Lookup(ctx, grain1)
		require.Equal(t, err, cluster.ErrGrainActivationNotFound)

		err = store.Activate(ctx, grainAddr(node1, grain1))
		require.NoError(t, err)

		err = store.Activate(ctx, grainAddr(node1, grain2))
		require.NoError(t, err)

		g1, err := store.Lookup(ctx, grain1)
		require.NoError(t, err)
		require.Equal(t, grainAddr(node1, grain1), g1)

		g2, err := store.Lookup(ctx, grain2)
		require.NoError(t, err)
		require.Equal(t, grainAddr(node1, grain2), g2)

		err = store.Deactivate(ctx, grainAddr(node1, grain2))
		require.NoError(t, err)
		_, err = store.Lookup(ctx, grain2)
		require.Equal(t, err, cluster.ErrGrainActivationNotFound)

		// shouldn't do anything because location does not match
		err = store.Deactivate(ctx, grainAddr(node2, grain1))
		require.NoError(t, err)
		g1, err = store.Lookup(ctx, grain1)
		require.NoError(t, err)
		require.Equal(t, grainAddr(node1, grain1), g1)
	})

}

func grainIdent(grainType string, grainID string) grain.Identity {
	return grain.Identity{
		GrainType: grainType,
		ID:        grainID,
	}
}
func grainAddr(nodeName string, ident grain.Identity) cluster.GrainAddress {
	return cluster.GrainAddress{
		Location: cluster.Location(nodeName),
		Identity: ident,
	}
}
