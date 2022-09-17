package generic

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/segmentio/ksuid"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/services"
)

type decoderFunc = func(in interface{}) error

var (
	ErrStreamInboxFull             = errors.New("stream inbox full")
	ErrObservableAlreadyRegistered = errors.New("observable already registered")
	ErrNoHandler                   = errors.New("no handler for notification")
	ErrInvalidID                   = errors.New("generic grain has invalid id")
)

const (
	GenericGrainType = "GenericGrain"

	registrationRefreshInterval = time.Minute
	registrationTimeout         = 3 * registrationRefreshInterval
)

type observable struct {
	ident grain.Identity
	token grain.ObserverRegistrationToken
}

type InvokeMethodFunc func(ctx context.Context, method string, sender grain.Identity,
	d grain.Deserializer, respSerializer grain.Serializer) error

type Grain struct {
	grain.Identity

	client grain.SiloClient
	lock   sync.RWMutex

	methods map[string]InvokeMethodFunc
}

func NewGrain(location string, client grain.SiloClient) *Grain {
	return &Grain{
		Identity: Identity(location),
		client:   client,
		methods:  map[string]InvokeMethodFunc{},
	}
}

func (s *Grain) Activate(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
	return s, nil
}

func (s *Grain) InvokeMethod(ctx context.Context, methodName string, sender grain.Identity,
	d grain.Deserializer, respSerializer grain.Serializer) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if m, ok := s.methods[methodName]; ok {
		return m(ctx, methodName, sender, d, respSerializer)
	}

	return errors.New("unknown method")
}

func (s *Grain) OnMethod(methodName string, f InvokeMethodFunc) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.methods[methodName]; ok {
		return errors.New("method already registered")
	}
	s.methods[methodName] = f

	return nil
}

func Identity(location string) grain.Identity {
	return grain.Identity{
		GrainType: GenericGrainType,
		ID:        fmt.Sprintf("%s!%s", location, ksuid.New().String()),
	}
}

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

func IsGenericGrain(ident grain.Identity) bool {
	return ident.GrainType == GenericGrainType
}
