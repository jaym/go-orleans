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

func (s *PSQLStore) List(ctx context.Context, owner grain.Address, observableName string) ([]grain.RegisteredObserver, error) {
	owner.Location = ""
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
		addr := grain.Address{}
		if err := addr.UnmarshalText([]byte(r.ObserverGrain)); err != nil {
			log.V(0).Error(err, "invald observer address", "id", r.ID, "observer", r.ObserverGrain)
			return nil, err
		}
		observer := newRegisteredObserver(s.codec, addr, r.ObservableName, r.Val)
		observers = append(observers, observer)
	}

	return observers, nil
}

func (s *PSQLStore) Add(ctx context.Context, owner grain.Address, observableName string, observerAddress grain.Address, opts ...observer.AddOption) error {
	owner.Location = ""
	observerAddress.Location = ""

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
			ObserverGrain:  observerAddress.String(),
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

func (s *PSQLStore) Remove(ctx context.Context, owner grain.Address, opts ...observer.RemoveOption) error {
	owner.Location = ""

	options := observer.RemoveOptions{}
	observer.ReadRemoveOptions(&options, opts)

	var observerGrain *string
	if options.ObserverGrain != nil {
		options.ObserverGrain.Location = ""
		og := options.ObserverGrain.String()
		observerGrain = &og
	}

	err := s.q.RemoveObserversFixed(ctx, owner.String(), observerGrain, options.ObservableName)
	if err != nil {
		return err
	}

	return nil
}
