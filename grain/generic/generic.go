package generic

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/proto"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/services"
)

var ErrStreamInboxFull = errors.New("stream inbox full")
var ErrObservableAlreadyRegistered = errors.New("observable already registered")
var ErrNoHandler = errors.New("no handler for notification")

var GenericGrainType = "GenericGrain"

type decoderFunc = func(in interface{}) error

const (
	registrationTimeout         = 3 * time.Minute
	registrationRefreshInterval = time.Second
)

var Descriptor = descriptor.GrainDescription{
	GrainType: GenericGrainType,
	Activation: descriptor.ActivationDesc{
		Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, o services.GrainObserverManager, identity grain.Identity) (grain.GrainReference, error) {
			g := activator.(*Grain)
			err := coreServices.TimerService().RegisterTicker("obsv", registrationRefreshInterval, func() {
				g.refreshObservables(ctx)
			})
			if err != nil {
				return nil, err
			}
			return g, nil
		},
	},
}

type observable struct {
	ident grain.Identity
	req   proto.Message
}

type Grain struct {
	grain.Identity

	client  grain.SiloClient
	lock    sync.RWMutex
	streams map[string]*stream
}

func NewGrain(location string, client grain.SiloClient) *Grain {
	return &Grain{
		Identity: Identity(location),
		client:   client,
		streams:  map[string]*stream{},
	}
}

func Identity(location string) grain.Identity {
	return grain.Identity{
		GrainType: GenericGrainType,
		ID:        fmt.Sprintf("%s!%s", location, ksuid.New().String()),
	}
}

func IsGenericGrain(ident grain.Identity) bool {
	return ident.GrainType == GenericGrainType
}

var ErrInvalidID = errors.New("generic grain has invalid id")

func ParseIdentity(ident grain.Identity) (location string, err error) {
	if !IsGenericGrain(ident) {
		return "", errors.New("not a generic grain")
	}
	parts := strings.Split(ident.ID, "!")
	if len(parts) != 2 {
		return "", ErrInvalidID
	}
	return parts[0], nil
}

type Message struct {
	Sender  grain.Identity
	decoder decoderFunc
}

func (m *Message) Decode(in interface{}) error {
	return m.decoder(in)
}

type Stream interface {
	Done() <-chan struct{}
	Close(ctx context.Context) error
	C() <-chan Message
	GenericObserve(context.Context, grain.Identity, proto.Message) error
}

func (g *Grain) CreateStream(observableType string, observableName string) (Stream, error) {
	g.lock.Lock()
	defer g.lock.Unlock()

	k := key(observableType, observableName)
	if _, ok := g.streams[k]; ok {
		return nil, ErrObservableAlreadyRegistered
	}
	s := &stream{
		g:              g,
		c:              make(chan Message, 8),
		done:           make(chan struct{}),
		observableType: observableType,
		observableName: observableName,
	}
	g.streams[k] = s
	return s, nil
}

func (g *Grain) HandleNotification(observableType string, observableName string, sender grain.Identity, decoder decoderFunc) error {
	g.lock.RLock()
	defer g.lock.RUnlock()

	k := key(observableType, observableName)
	s, ok := g.streams[k]
	if !ok {
		return ErrNoHandler
	}
	select {
	case s.c <- Message{
		Sender:  sender,
		decoder: decoder,
	}:
	default:
		return ErrStreamInboxFull
	}
	return nil
}

func (g *Grain) Deactivate(ctx context.Context) {
	g.lock.Lock()
	defer g.lock.Unlock()

	// TODO: asser that this method is only called by the runtime
	// users should use silo.DestroyGrain

	for _, s := range g.streams {
		close(s.done)
		// TODO: what should happen with this error
		s.destroy(ctx)
	}

	g.streams = map[string]*stream{}
}

func (g *Grain) deleteStream(k string) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if s, ok := g.streams[k]; ok {
		close(s.done)
		delete(g.streams, k)
	}
}

func (g *Grain) refreshObservables(ctx context.Context) {
	g.lock.RLock()
	defer g.lock.RUnlock()

	for _, s := range g.streams {
		s.refreshObservables(ctx)
	}
}

func key(observableType string, observableName string) string {
	return fmt.Sprintf("%s\x00%s", observableType, observableName)
}

type stream struct {
	l              sync.Mutex
	destroyed      bool
	g              *Grain
	c              chan Message
	done           chan struct{}
	observableType string
	observableName string
	observables    []observable
}

func (s *stream) Done() <-chan struct{} {
	return s.done
}

func (s *stream) Close(ctx context.Context) error {
	// TODO: the lock should probably be held for this entire operation
	s.g.deleteStream(key(s.observableType, s.observableName))

	return s.destroy(ctx)
}

func (s *stream) destroy(ctx context.Context) error {
	s.l.Lock()
	defer s.l.Unlock()
	if s.destroyed {
		return nil
	}
	s.destroyed = true

	futures := make([]grain.UnsubscribeObserverFuture, len(s.observables))
	for i, o := range s.observables {
		f := s.g.client.UnsubscribeObserver(ctx, s.g.Identity, o.ident, s.observableName)
		futures[i] = f
	}

	for _, f := range futures {
		if err := f.Await(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (s *stream) C() <-chan Message {
	return s.c
}

func (s *stream) GenericObserve(ctx context.Context, observableIdent grain.Identity, req proto.Message) error {
	s.l.Lock()
	defer s.l.Unlock()

	f := s.g.client.RegisterObserver(ctx, s.g.Identity, observableIdent, s.observableName, req, grain.WithRegisterObserverRegistrationTimeout(registrationTimeout))
	if err := f.Await(ctx); err != nil {
		return err
	}

	observableIdx := s.findObservable(observableIdent)
	if observableIdx != -1 {
		s.observables[observableIdx].req = req
	} else {
		s.observables = append(s.observables, observable{
			ident: observableIdent,
			req:   req,
		})
	}

	return nil
}

func (s *stream) Unsubscribe(ctx context.Context, observableIdent grain.Identity) error {
	s.l.Lock()
	defer s.l.Unlock()

	observableIdx := s.findObservable(observableIdent)
	if observableIdx != -1 {
		return nil
	}

	f := s.g.client.UnsubscribeObserver(ctx, s.g.Identity, observableIdent, s.observableName)
	if err := f.Await(ctx); err != nil {
		return err
	}

	newLen := len(s.observables) - 1
	if newLen > 0 {
		s.observables[observableIdx] = s.observables[newLen]
		s.observables = s.observables[:newLen]
	}

	return nil
}

func (s *stream) refreshObservables(ctx context.Context) {
	s.l.Lock()
	defer s.l.Unlock()

	futures := make([]grain.RegisterObserverFuture, len(s.observables))
	for i, o := range s.observables {
		futures[i] = s.g.client.RegisterObserver(ctx, s.g.Identity, o.ident, s.observableName, o.req, grain.WithRegisterObserverRegistrationTimeout(registrationTimeout))
	}
	for _, f := range futures {
		err := f.Await(ctx)
		if err != nil {
			// TODO: what should happen with the errors
		}
	}
}

func (s *stream) findObservable(o grain.Identity) int {
	for i, eo := range s.observables {
		if eo.ident == o {
			return i
		}
	}
	return -1
}
