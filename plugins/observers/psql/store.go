package psql

import (
	"context"
	"database/sql"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/plugins/codec/protobuf"
	"github.com/jaym/go-orleans/plugins/observers/psql/internal"
	"github.com/jaym/go-orleans/silo/services/observer"
)

type PSQLStore struct {
	log         logr.Logger
	codec       codec.Codec
	nowProvider func() time.Time

	db          *pgxpool.Pool
	q           *internal.Queries
	cleanupChan chan int64
	stopChan    chan struct{}
}

type NewObserverStoreOption func(*PSQLStore)

func WithCodec(c codec.Codec) NewObserverStoreOption {
	return func(p *PSQLStore) {
		p.codec = c
	}
}

func SetupDatabase(db *sql.DB) error {
	return internal.Migrate(db)
}

func NewObserverStore(log logr.Logger, db *pgxpool.Pool, opts ...NewObserverStoreOption) *PSQLStore {
	s := &PSQLStore{
		log:         log,
		nowProvider: time.Now,
		codec:       protobuf.NewCodec(),
		db:          db,
		q:           internal.New(db),
		cleanupChan: make(chan int64, 64),
		stopChan:    make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	s.startBackgroundCleaner()

	return s
}

func (s *PSQLStore) Stop() {
	close(s.stopChan)
}

func (s *PSQLStore) startBackgroundCleaner() {
	go func() {
		for {
			select {
			case <-s.stopChan:
				return
			default:
			}

			select {
			case <-s.stopChan:
				return
			case <-s.cleanupChan:
			default:
			}
		}
	}()
}

func (s *PSQLStore) List(ctx context.Context, owner grain.Identity, observableName string) ([]grain.RegisteredObserver, error) {
	log := s.log.WithValues("owner", owner.String(), "observableName", observableName)
	rows, err := s.q.ListObservers(ctx, internal.ListObserversParams{
		OwnerGrain:     owner.String(),
		ObservableName: observableName,
	})
	if err != nil {
		return nil, err
	}

	observers := make([]grain.RegisteredObserver, 0, len(rows))
	now := s.nowProvider()
	for _, r := range rows {
		if r.Expires.Valid && now.After(r.Expires.Time) {
			select {
			case s.cleanupChan <- r.ID:
				log.V(4).Info("cleaning up expired observable", "id", r.ID, "observer", r.ObserverGrain)
			default:
				log.V(2).Info("could not enqueue expired observer for cleanup", "id", r.ID, "observer", r.ObserverGrain)
			}
			continue
		}
		ident := grain.Identity{}
		if err := ident.UnmarshalText([]byte(r.ObserverGrain)); err != nil {
			log.V(0).Error(err, "invald observer identity", "id", r.ID, "observer", r.ObserverGrain)
			return nil, err
		}
		observer := newRegisteredObserver(s.codec, ident, r.ObservableName, r.Val)
		observers = append(observers, observer)
	}

	return observers, nil
}

func (s *PSQLStore) Add(ctx context.Context, owner grain.Identity, observableName string, observerIdentity grain.Identity, opts ...observer.AddOption) error {
	options := observer.AddOptions{}
	observer.ReadAddOptions(&options, opts)

	var val []byte
	if options.Val != nil {
		var err error
		val, err = s.codec.Encode(options.Val)
		if err != nil {
			return err
		}
	}

	expires := sql.NullTime{}
	if !options.Expires.IsZero() {
		expires.Time = options.Expires
		expires.Valid = true
	}

	err := s.db.BeginFunc(ctx, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		id, err := q.UpsertObserver(ctx, internal.UpsertObserverParams{
			OwnerGrain:     owner.String(),
			ObserverGrain:  observerIdentity.String(),
			ObservableName: observableName,
			Expires:        expires,
		})
		if err != nil {
			return err
		}

		err = q.UpsertObserverValue(ctx, internal.UpsertObserverValueParams{
			ObserversID: id,
			Val:         val,
		})

		if err != nil {
			return err
		}

		return nil
	})
	return err
}

func (s *PSQLStore) Remove(ctx context.Context, owner grain.Identity, opts ...observer.RemoveOption) error {
	options := observer.RemoveOptions{}
	observer.ReadRemoveOptions(&options, opts)

	var observerGrain *string
	if options.ObserverGrain != nil {
		og := options.ObserverGrain.String()
		observerGrain = &og
	}

	err := s.q.RemoveObserversFixed(ctx, owner.String(), observerGrain, options.ObservableName)
	if err != nil {
		return err
	}

	return nil
}
