package psql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	stdlog "log"
	"os"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/go-logr/stdr"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/observers/psql"
	"github.com/jaym/go-orleans/plugins/observers/psql/internal"
	"github.com/jaym/go-orleans/silo/services/observer"
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

	store := psql.NewObserverStore(log, pool, psql.WithCodec(jsonCodec{}))
	defer store.Stop()

	ctx := context.Background()

	idents := []grain.Identity{
		{
			GrainType: "type1",
			ID:        "id1",
		},
		{
			GrainType: "type2",
			ID:        "id2",
		},
		{
			GrainType: "type3",
			ID:        "id3",
		},
		{
			GrainType: "type4",
			ID:        "id4",
		},
		{
			GrainType: "type5",
			ID:        "id5",
		},
	}

	err = store.Add(ctx, idents[0], "observable1", idents[1])
	require.NoError(t, err)
	err = store.Add(ctx, idents[0], "observable1", idents[2])
	require.NoError(t, err)
	err = store.Add(ctx, idents[0], "observable1", idents[3])
	require.NoError(t, err)
	err = store.Add(ctx, idents[0], "observable2", idents[3])
	require.NoError(t, err)
	err = store.Add(ctx, idents[0], "observable2", idents[4])
	require.NoError(t, err)

	observables, err := store.List(ctx, idents[0], "observable1")
	require.NoError(t, err)
	require.Len(t, observables, 3)

	err = store.Add(ctx, idents[0], "observable1", idents[1], observer.AddWithExpiration(time.Now().Add(-1*time.Minute)))
	require.NoError(t, err)
	observables, err = store.List(ctx, idents[0], "observable1")
	require.NoError(t, err)
	require.Len(t, observables, 2)

	observables, err = store.List(ctx, idents[0], "observable2")
	require.NoError(t, err)
	require.Len(t, observables, 2)

	observables, err = store.List(ctx, idents[1], "observable1")
	require.NoError(t, err)
	require.Len(t, observables, 0)

	err = store.Add(ctx, idents[0], "observable3", idents[4], observer.AddWithVal("teststring"))
	require.NoError(t, err)
	observables, err = store.List(ctx, idents[0], "observable3")
	require.NoError(t, err)
	require.Len(t, observables, 1)
	var s string
	err = observables[0].Get(&s)
	require.NoError(t, err)
	require.Equal(t, "teststring", s)

	err = store.Remove(ctx, idents[0], observer.RemoveByObserverGrain(idents[1]))
	require.NoError(t, err)
	observables, err = store.List(ctx, idents[0], "observable1")
	require.NoError(t, err)
	require.Len(t, observables, 2)

	err = store.Remove(ctx, idents[0], observer.RemoveByObservableName("observable1"))
	require.NoError(t, err)
	observables, err = store.List(ctx, idents[0], "observable1")
	require.NoError(t, err)
	require.Len(t, observables, 0)

	observables, err = store.List(ctx, idents[0], "observable2")
	require.NoError(t, err)
	require.Len(t, observables, 2)
}

type jsonCodec struct{}

func (jsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Decode(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}
