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

	refreshObservablesTickerName = "__refresh-observables"
)

type ObservableGrain interface {
	grain.GrainReference
	UnregisterObserver(context.Context, grain.ObserverRegistrationToken)
	RefreshObserver(context.Context, grain.ObserverRegistrationToken) (grain.ObserverRegistrationToken, error)
}

type observable struct {
	id    string
	ref   ObservableGrain
	token grain.ObserverRegistrationToken
}

type InvokeMethodFunc func(ctx context.Context, client grain.SiloClient, method string, sender grain.Identity,
	d grain.Deserializer, respSerializer grain.Serializer) error

type Grain struct {
	grain.Identity

	client grain.SiloClient
	lock   sync.RWMutex

	methods     map[string]InvokeMethodFunc
	observables map[string]observable
}

func NewGrain(location string, client grain.SiloClient) *Grain {
	return &Grain{
		Identity:    Identity(location),
		client:      client,
		methods:     map[string]InvokeMethodFunc{},
		observables: map[string]observable{},
	}
}

func (s *Grain) Activate(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
	err := services.TimerService().RegisterTicker(refreshObservablesTickerName, 10*time.Second, func(ctx context.Context) {
		s.lock.Lock()
		defer s.lock.Unlock()
		fmt.Println("Refreshing observables")
		for k, o := range s.observables {
			t, err := o.ref.RefreshObserver(ctx, o.token)
			if err != nil {
				fmt.Printf("an error happened in refreshing observables: %v\n", err)
				continue
			}
			s.observables[k] = observable{
				id:    k,
				ref:   o.ref,
				token: t,
			}
		}

	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Grain) InvokeMethod(ctx context.Context, methodName string, sender grain.Identity,
	d grain.Deserializer, respSerializer grain.Serializer) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if m, ok := s.methods[methodName]; ok {
		return m(ctx, s.client, methodName, sender, d, respSerializer)
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

func (s *Grain) RegisterObservable(ref ObservableGrain, token grain.ObserverRegistrationToken) {
	s.lock.Lock()
	defer s.lock.Unlock()
	id := ksuid.New().String()
	s.observables[id] = observable{
		id:    id,
		ref:   ref,
		token: token,
	}
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
