package psql

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/graindir/psql/internal"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type PSQLStore struct {
	log             logr.Logger
	db              *pgxpool.Pool
	q               *internal.Queries
	checkinInterval time.Duration
}

type psqlWriteLock struct {
	s            *PSQLStore
	address      cluster.Location
	onExpiration cluster.GrainDirectoryLockOnExpirationFunc
	id           int64
	stopChan     chan struct{}
	wg           sync.WaitGroup

	lock sync.RWMutex
	err  error
}

func SetupDatabase(db *sql.DB) error {
	return internal.Migrate(db)
}

func NewGrainDirectory(log logr.Logger, db *pgxpool.Pool) *PSQLStore {
	return &PSQLStore{
		log:             log,
		db:              db,
		q:               internal.New(db),
		checkinInterval: time.Minute,
	}
}

func (s *PSQLStore) withTx(ctx context.Context, f func(*internal.Queries) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	q := s.q.WithTx(tx)
	if err := f(q); err != nil {
		if errRollback := tx.Rollback(ctx); errRollback != nil {
			s.log.Error(err, "failed to roll back transaction")
		}
		return err
	}
	return tx.Commit(ctx)
}

func (s *PSQLStore) Lock(ctx context.Context, addr cluster.Location,
	onExpFunc cluster.GrainDirectoryLockOnExpirationFunc) (cluster.GrainDirectoryLock, error) {

	var id int64
	err := s.withTx(ctx, func(q *internal.Queries) error {
		var err error
		err = q.DeleteNodeByName(ctx, string(addr))
		if err != nil {
			return err
		}
		id, err = q.InsertNode(ctx, string(addr))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	l := &psqlWriteLock{
		s:            s,
		address:      addr,
		id:           id,
		onExpiration: onExpFunc,
		stopChan:     make(chan struct{}),
	}

	l.startBackgroundWorker()

	return l, nil
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

func (m *PSQLStore) activate(ctx context.Context, nodeID int64, addr cluster.GrainAddress) error {
	m.log.V(5).Info("storing grain activation", "addr", addr)
	if addr.Location == "" {
		panic("no location specified")
	}
	err := m.q.InsertGrain(ctx, internal.InsertGrainParams{
		NodeID:    nodeID,
		NodeName:  string(addr.Location),
		GrainType: addr.GrainType,
		GrainID:   addr.ID,
	})
	if err != nil {
		m.log.V(0).Error(err, "failed to store grain activation", "addr", addr)
		if isDuplicateGrain(err) {
			return cluster.ErrGrainAlreadyActivated
		}
		return err
	}
	return nil
}

func (m *PSQLStore) deactivate(ctx context.Context, nodeID int64, ident grain.Identity) error {
	m.log.V(5).Info("deleting grain activation", "ident", ident)
	err := m.q.DeleteGrainActivation(ctx, internal.DeleteGrainActivationParams{
		NodeID:    nodeID,
		GrainType: ident.GrainType,
		GrainID:   ident.ID,
	})
	if err != nil {
		m.log.V(0).Error(err, "failed to delete grain activation", "ident", ident)
		return err
	}
	return nil
}

func (m *PSQLStore) releaseLock(ctx context.Context, id int64) error {
	return m.q.DeleteNodeByID(ctx, id)
}

func (m *PSQLStore) refreshLock(ctx context.Context, id int64) error {
	affected, err := m.q.UpdateNodeCheckin(ctx, id)
	if err != nil {
		return err
	}
	if affected != 1 {
		return cluster.ErrGrainDirectoryLockLost
	}
	return nil
}

func (m *PSQLStore) cleanupExpiredNodes(ctx context.Context) error {
	return m.q.DeleteNodeByCheckin(ctx, sql.NullTime{
		Valid: true,
		Time:  time.Now().Add(-2 * m.checkinInterval),
	})
}

func isDuplicateGrain(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "23") && pgErr.ConstraintName == "grains_grain_type_grain_id_key"
	}
	return false
}

func (l *psqlWriteLock) Activate(ctx context.Context, ident grain.Identity) error {
	return l.s.activate(ctx, l.id, cluster.GrainAddress{
		Location: l.address,
		Identity: ident,
	})
}

func (l *psqlWriteLock) Deactivate(ctx context.Context, ident grain.Identity) error {
	return l.s.deactivate(ctx, l.id, ident)
}

func (l *psqlWriteLock) Lookup(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	return l.s.Lookup(ctx, ident)
}

func (l *psqlWriteLock) Unlock(ctx context.Context) error {
	close(l.stopChan)
	err := l.s.releaseLock(ctx, l.id)
	l.wg.Wait()
	return err
}

func (l *psqlWriteLock) Err() error {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.err
}

func (l *psqlWriteLock) startBackgroundWorker() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		// TODO: add some jitter
		refreshTicker := time.NewTicker(l.s.checkinInterval)
		cleanupTicker := time.NewTicker(2 * l.s.checkinInterval)

		ctx := context.Background()
		defer refreshTicker.Stop()
		defer cleanupTicker.Stop()

		for {
			select {
			case <-refreshTicker.C:
				if err := l.s.refreshLock(ctx, l.id); err != nil {
					if errors.Is(err, cluster.ErrGrainDirectoryLockLost) {
						l.lock.Lock()
						l.err = err
						l.lock.Unlock()
						l.onExpiration()
						return
					}
				}
			case <-cleanupTicker.C:
				if err := l.s.cleanupExpiredNodes(ctx); err != nil {
					l.s.log.Error(err, "failed to cleanup expired nodes")
				}
			case <-l.stopChan:
				return
			}
		}
	}()
}
