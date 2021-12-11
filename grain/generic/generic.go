package generic

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/services"
	"github.com/segmentio/ksuid"
)

var ErrStreamInboxFull = errors.New("stream inbox full")
var ErrObservableAlreadyRegistered = errors.New("observable already registered")
var ErrNoHandler = errors.New("no handler for notification")

var GenericGrainType = "GenericGrain"

type decoderFunc = func(in interface{}) error

var Descriptor = descriptor.GrainDescription{
	GrainType: GenericGrainType,
	Activation: descriptor.ActivationDesc{
		Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, o services.GrainObserverManager, identity grain.Identity) (grain.GrainReference, error) {
			return activator.(*Grain), nil
		},
	},
}

type Grain struct {
	grain.Identity

	lock    sync.RWMutex
	streams map[string]*stream
}

func NewGrain(location string) *Grain {
	return &Grain{
		Identity: Identity(location),
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
	Cancel()
	C() <-chan Message
}

func (g *Grain) CreateStream(observableType string, observableName string) (Stream, error) {
	g.lock.Lock()
	defer g.lock.Unlock()

	k := key(observableType, observableName)
	if _, ok := g.streams[k]; ok {
		return nil, ErrObservableAlreadyRegistered
	}
	s := &stream{
		c:    make(chan Message, 8),
		done: make(chan struct{}),
		onCancel: func() {
			g.cancel(k)
		},
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

func (g *Grain) cancel(k string) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if s, ok := g.streams[k]; ok {
		close(s.done)
		delete(g.streams, k)
	}
}

func key(observableType string, observableName string) string {
	return fmt.Sprintf("%s\x00%s", observableType, observableName)
}

type stream struct {
	c        chan Message
	done     chan struct{}
	onCancel func()
}

func (s *stream) Done() <-chan struct{} {
	return s.done
}

func (s *stream) Cancel() {
	s.onCancel()
}

func (s *stream) C() <-chan Message {
	return s.c
}
