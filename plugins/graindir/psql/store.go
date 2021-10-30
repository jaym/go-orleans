package psql

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/go-logr/logr"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/graindir/psql/internal"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type PSQLStore struct {
	log logr.Logger
	db  *pgxpool.Pool
	q   *internal.Queries
}

func SetupDatabase(db *sql.DB) error {
	return internal.Migrate(db)
}

func NewGrainDirectory(log logr.Logger, db *pgxpool.Pool) *PSQLStore {
	return &PSQLStore{
		log: log,
		db:  db,
		q:   internal.New(db),
	}
}

func (m *PSQLStore) Lookup(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	g, err := m.q.LookupGrain(ctx, internal.LookupGrainParams{
		GrainType: ident.GrainType,
		GrainID:   ident.ID,
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			return cluster.GrainAddress{}, cluster.ErrGrainActivationNotFound
		}
		return cluster.GrainAddress{}, err
	}
	return cluster.GrainAddress{
		Location: cluster.Location(g.NodeName),
		Identity: grain.Identity{
			GrainType: g.GrainType,
			ID:        g.GrainID,
		},
	}, nil
}

func (m *PSQLStore) Activate(ctx context.Context, addr cluster.GrainAddress) error {
	m.log.V(5).Info("storing grain activation", "addr", addr)
	err := m.q.InsertGrain(ctx, internal.InsertGrainParams{
		NodeName:  string(addr.Location),
		GrainType: addr.GrainType,
		GrainID:   addr.ID,
	})
	if err != nil {
		m.log.V(0).Error(err, "failed to store grain activation", "addr", addr)
		if isConstraintError(err) {
			return cluster.ErrGrainAlreadyActivated
		}
		return err
	}
	return nil
}

func (m *PSQLStore) Deactivate(ctx context.Context, addr cluster.GrainAddress) error {
	m.log.V(5).Info("deleting grain activation", "addr", addr)
	err := m.q.DeleteGrainActivation(ctx, internal.DeleteGrainActivationParams{
		NodeName:  string(addr.Location),
		GrainType: addr.GrainType,
		GrainID:   addr.ID,
	})
	if err != nil {
		m.log.V(0).Error(err, "failed to delete grain activation", "addr", addr)
		return err
	}
	return nil
}

func isConstraintError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "23")
	}
	return false
}
