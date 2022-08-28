package activation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/services"
	"github.com/jaym/go-orleans/mocks"
	"github.com/jaym/go-orleans/silo/services/resourcemanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRegistrar struct {
	entries map[string]descriptor.ActivatorFunc
}

func (*mockRegistrar) RegisterV2(grainType string, activatorFunc descriptor.ActivatorFunc) {
	panic("unimplemented")
}
func (m *mockRegistrar) LookupV2(grainType string) (descriptor.ActivatorFunc, error) {
	e, ok := m.entries[grainType]
	if !ok {
		return nil, descriptor.ErrGrainTypeNotFound
	}
	return e, nil
}

type noopResourceManager struct{}

func (noopResourceManager) Touch(grainAddr grain.Identity) error  { return nil }
func (noopResourceManager) Remove(grainAddr grain.Identity) error { return nil }

func TestActivationFailsOfUnknownGrainType(t *testing.T) {
	registrar := &mockRegistrar{entries: map[string]descriptor.ActivatorFunc{}}
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := mocks.NewResourceManager(t)
	log := logr.Discard()
	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 8, func(i grain.Identity) {})
	_, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.Equal(t, descriptor.ErrGrainTypeNotFound, err)
}

func TestActivationGrainActivationNoResources(t *testing.T) {
	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				assert.Equal(t, grain.Identity{
					GrainType: "Mytype",
					ID:        "Myid",
				}, identity)
				return nil, errors.New("failed")
			},
		},
	}
	deactivated := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := mocks.NewResourceManager(t)
	resourceManager.On("Touch", mock.Anything).Return(resourcemanager.ErrNoCapacity)
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 8,
		func(i grain.Identity) {
			defer wg.Done()
			assert.Equal(t, grain.Identity{
				GrainType: "Mytype",
				ID:        "Myid",
			}, i)
			deactivated = true
		})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)
	wg.Wait()
	require.Equal(t, grainStateDeactivated, a.grainState)
	require.True(t, deactivated)

	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.InvokeOneWayMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil)
	require.Equal(t, ErrGrainDeactivating, err)
}

func TestActivationGrainActivatorFailure(t *testing.T) {
	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				assert.Equal(t, grain.Identity{
					GrainType: "Mytype",
					ID:        "Myid",
				}, identity)
				return nil, errors.New("failed")
			},
		},
	}
	deactivated := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := noopResourceManager{}
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 8,
		func(i grain.Identity) {
			defer wg.Done()
			assert.Equal(t, grain.Identity{
				GrainType: "Mytype",
				ID:        "Myid",
			}, i)
			deactivated = true
		})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)
	wg.Wait()
	require.Equal(t, grainStateDeactivated, a.grainState)
	require.True(t, deactivated)

	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.InvokeOneWayMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil)
	require.Equal(t, ErrGrainDeactivating, err)
}

func TestActivationGrainActivatorFailureWithCalls(t *testing.T) {
	// activationWait will make the activator wait until we have queued up
	// A response is expected to the InvokeMethod call even when the activation
	// fails
	activationWait := make(chan struct{})
	expectedErr := errors.New("failed")
	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				<-activationWait
				return nil, expectedErr
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := noopResourceManager{}
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 8, func(i grain.Identity) {})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)
	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		defer wg.Done()
		assert.Equal(t, expectedErr, err)
	})
	require.NoError(t, err)

	require.NoError(t, err)
	close(activationWait)
	wg.Wait()
}

type mockActivation struct {
	InvokeMethodFunc func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error
}

func (m mockActivation) InvokeMethod(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
	return m.InvokeMethodFunc(ctx, method, sender, d, respSerializer)
}

func TestActivationGrainMailboxFull(t *testing.T) {
	waitChan := make(chan struct{})
	pingEntered := sync.WaitGroup{}
	activation := mockActivation{
		InvokeMethodFunc: func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
			pingEntered.Done()
			<-waitChan
			return nil
		},
	}

	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				return activation, nil
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := noopResourceManager{}
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 1, func(i grain.Identity) {})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)
	pingEntered.Add(1)
	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		defer wg.Done()
	})
	require.NoError(t, err)
	pingEntered.Wait()

	pingEntered.Add(1)
	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		defer wg.Done()
	})
	require.NoError(t, err)

	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		assert.Fail(t, "unexpected call")
	})
	require.Equal(t, ErrInboxFull, err)

	close(waitChan)

	wg.Wait()

	wg.Add(1)
	a.StopAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	})
	wg.Wait()
}

type notEvictable struct {
	InvokeMethodFunc func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error
}

func (*notEvictable) CanEvict(ctx context.Context) bool {
	return false
}

func (m *notEvictable) InvokeMethod(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
	return m.InvokeMethodFunc(ctx, method, sender, d, respSerializer)
}

func TestActivationGrainEvictionBlockedTest(t *testing.T) {
	waitChan := make(chan struct{})
	pingEntered := sync.WaitGroup{}
	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				return &notEvictable{
					InvokeMethodFunc: func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
						pingEntered.Done()
						<-waitChan
						return nil
					},
				}, nil
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := noopResourceManager{}
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 2, func(i grain.Identity) {})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)
	pingEntered.Add(1)
	err = a.InvokeMethod(grain.Anonymous(), "Ping", time.Now().Add(time.Minute), nil, nil, func(err error) {
		defer wg.Done()
	})
	require.NoError(t, err)
	pingEntered.Wait()

	wg.Add(1)
	a.EvictAsync(func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainDeactivationRefused, err)
	})

	a.EvictAsync(func(err error) {
		require.Equal(t, ErrInboxFull, err)
	})

	close(waitChan)

	wg.Wait()

	wg.Add(1)
	a.StopAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	})
	wg.Wait()
}

type evictable struct {
	InvokeMethodFunc func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error
	DeactivateFunc   func(ctx context.Context)
}

func (*evictable) CanEvict(ctx context.Context) bool {
	return true
}

func (m *evictable) InvokeMethod(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
	return m.InvokeMethodFunc(ctx, method, sender, d, respSerializer)
}

func (m *evictable) Deactivate(ctx context.Context) {
	m.DeactivateFunc(ctx)
}

func TestActivationGrainDeactivateOnEvict(t *testing.T) {
	waitChan := make(chan struct{})
	deactivateEntered := make(chan struct{})
	var e *evictable
	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				e = &evictable{
					InvokeMethodFunc: func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
						return nil
					},
					DeactivateFunc: func(ctx context.Context) {
						close(deactivateEntered)
						<-waitChan
					},
				}
				return e, nil
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := noopResourceManager{}
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 2, func(i grain.Identity) {})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)

	a.EvictAsync(func(err error) {
		defer wg.Done()
		assert.NoError(t, err)
	})

	<-deactivateEntered
	require.Equal(t, grainStateDeactivating, a.grainState)

	close(waitChan)
	wg.Wait()
	require.Equal(t, grainStateDeactivated, a.grainState)
}

func TestActivationGrainDeactivateOnStop(t *testing.T) {
	waitChan := make(chan struct{})
	deactivateEntered := make(chan struct{})
	var e *evictable
	registrar := &mockRegistrar{
		entries: map[string]descriptor.ActivatorFunc{
			"Mytype": func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error) {
				e = &evictable{
					InvokeMethodFunc: func(ctx context.Context, method string, sender grain.Identity, d grain.Deserializer, respSerializer grain.Serializer) error {
						return nil
					},
					DeactivateFunc: func(ctx context.Context) {
						close(deactivateEntered)
						<-waitChan
					},
				}
				return e, nil
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	timerService := mocks.NewTimerService(t)
	siloClient := mocks.NewSiloClient(t)
	resourceManager := noopResourceManager{}
	log := logr.Discard()

	grainActivator := NewLocalGrainActivator(log, registrar, siloClient, timerService, resourceManager, 2, func(i grain.Identity) {})
	a, err := grainActivator.ActivateGrainWithDefaultActivator(grain.Identity{
		GrainType: "Mytype",
		ID:        "Myid",
	})
	require.NoError(t, err)

	a.StopAsync(func(err error) {
		defer wg.Done()
		assert.NoError(t, err)
	})

	<-deactivateEntered
	require.Equal(t, grainStateDeactivating, a.grainState)

	close(waitChan)
	wg.Wait()
	require.Equal(t, grainStateDeactivated, a.grainState)
}
