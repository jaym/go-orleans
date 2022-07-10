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

type mockRegistrarEntry struct {
	desc *descriptor.GrainDescription
	impl interface{}
}
type mockRegistrar struct {
	entries map[string]mockRegistrarEntry
}

func (*mockRegistrar) Register(desc *descriptor.GrainDescription, impl interface{}) {
	panic("unimplemented")
}
func (m *mockRegistrar) Lookup(grainType string) (*descriptor.GrainDescription, interface{}, error) {
	e, ok := m.entries[grainType]
	if !ok {
		return nil, nil, descriptor.ErrGrainTypeNotFound
	}
	return e.desc, e.impl, nil
}

type noopResourceManager struct{}

func (noopResourceManager) Touch(grainAddr grain.Identity) error  { return nil }
func (noopResourceManager) Remove(grainAddr grain.Identity) error { return nil }

func TestActivationFailsOfUnknownGrainType(t *testing.T) {
	registrar := &mockRegistrar{entries: map[string]mockRegistrarEntry{}}
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
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							assert.Equal(t, grain.Identity{
								GrainType: "Mytype",
								ID:        "Myid",
							}, identity)
							return nil, errors.New("failed")
						},
					},
				},
				impl: nil,
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

	err = a.NotifyObservable(grain.Anonymous(), "Fake", "Fake", []byte{})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.UnsubscribeObserver(grain.Anonymous(), "Fake", func(err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)
}

func TestActivationGrainActivatorFailure(t *testing.T) {
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							assert.Equal(t, grain.Identity{
								GrainType: "Mytype",
								ID:        "Myid",
							}, identity)
							return nil, errors.New("failed")
						},
					},
				},
				impl: nil,
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

	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)

	err = a.UnsubscribeObserver(grain.Anonymous(), "Fake", func(err error) {
		assert.Fail(t, "unexpected resolve")
	})
	require.Equal(t, ErrGrainDeactivating, err)
}

func TestActivationGrainActivatorFailureWithCalls(t *testing.T) {
	// activationWait will make the activator wait until we have queued up
	// an Calls (InvokeMethod, RegisterObserver, UnsubscribeObserver).
	// A response is expected to the InvokeMethod call even when the activation
	// fails
	activationWait := make(chan struct{})
	expectedErr := errors.New("failed")
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							<-activationWait
							return nil, expectedErr
						},
					},
				},
				impl: nil,
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
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
	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
		defer wg.Done()
		assert.Equal(t, expectedErr, err)
	})
	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.Equal(t, expectedErr, err)
	})
	require.NoError(t, err)

	err = a.UnsubscribeObserver(grain.Anonymous(), "Fake", func(err error) {
		defer wg.Done()
		assert.Equal(t, expectedErr, err)
	})
	require.NoError(t, err)
	close(activationWait)
	wg.Wait()
}

func TestActivationGrainUnknownCalls(t *testing.T) {
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							return nil, nil
						},
					},
				},
				impl: nil,
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
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

	err = a.NotifyObservable(grain.Anonymous(), "FakeType", "Fake", []byte{})
	require.NoError(t, err)

	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainMethodNotFound, err)
	})
	require.NoError(t, err)

	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainObservableNotFound, err)
	})
	require.NoError(t, err)

	err = a.UnsubscribeObserver(grain.Anonymous(), "Fake", func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainObservableNotFound, err)
	})
	require.NoError(t, err)
	wg.Wait()

	wg.Add(1)
	a.StopAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	})
	wg.Wait()
}

func TestActivationGrainMailboxFull(t *testing.T) {
	waitChan := make(chan struct{})
	pingEntered := sync.WaitGroup{}
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							return nil, nil
						},
					},
					Methods: []descriptor.MethodDesc{
						{
							Name: "Ping",
							Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
								pingEntered.Done()
								<-waitChan
								return nil, nil
							},
						},
					},
				},
				impl: nil,
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
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
	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
		defer wg.Done()
	})
	require.NoError(t, err)
	pingEntered.Wait()

	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainObservableNotFound, err)
	})
	require.NoError(t, err)

	err = a.UnsubscribeObserver(grain.Anonymous(), "Fake", func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainObservableNotFound, err)
	})
	require.NoError(t, err)

	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
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
	grain.Identity
}

func (*notEvictable) CanEvict(ctx context.Context) bool {
	return false
}

func TestActivationGrainEvictionBlockedTest(t *testing.T) {
	waitChan := make(chan struct{})
	pingEntered := sync.WaitGroup{}
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							return &notEvictable{
								Identity: identity,
							}, nil
						},
					},
					Methods: []descriptor.MethodDesc{
						{
							Name: "Ping",
							Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
								pingEntered.Done()
								<-waitChan
								return nil, nil
							},
						},
					},
				},
				impl: nil,
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
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
	err = a.InvokeMethod(grain.Anonymous(), "Ping", []byte{}, time.Now().Add(time.Minute), func(out interface{}, err error) {
		defer wg.Done()
	})
	require.NoError(t, err)
	pingEntered.Wait()

	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainObservableNotFound, err)
	})
	require.NoError(t, err)

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
	err = a.RegisterObserver(grain.Anonymous(), "Fake", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.Equal(t, ErrGrainObservableNotFound, err)
	})
	require.NoError(t, err)
	wg.Wait()

	wg.Add(1)
	a.StopAsync(func(err error) {
		assert.NoError(t, err)
		wg.Done()
	})
	wg.Wait()
}

type evictable struct {
	grain.Identity
	waitFor chan struct{}
	entered chan struct{}
}

func (e *evictable) Deactivate(ctx context.Context) {
	close(e.entered)
	<-e.waitFor
	return
}

func TestActivationGrainDeactivateOnEvict(t *testing.T) {
	waitChan := make(chan struct{})
	deactivateEntered := make(chan struct{})
	var e *evictable
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							e = &evictable{
								entered:  deactivateEntered,
								Identity: identity,
								waitFor:  waitChan,
							}
							return e, nil
						},
					},
				},
				impl: nil,
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
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							e = &evictable{
								entered:  deactivateEntered,
								Identity: identity,
								waitFor:  waitChan,
							}
							return e, nil
						},
					},
				},
				impl: nil,
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

type testActivation struct {
	grain.Identity
}

func TestActivationGrainObserverSameType(t *testing.T) {
	observerIdentity := grain.Identity{
		GrainType: "Mytype",
		ID:        "Observer",
	}
	activation := &testActivation{
		grain.Identity{
			GrainType: "Mytype",
			ID:        "Myid",
		},
	}
	var expectedError error
	registerCalls := 0
	unsubscribeCalls := 0
	handleCalls := 0
	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							return activation, nil
						},
					},
					Observables: []descriptor.ObservableDesc{
						{
							Name: "MyObservable",
							Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error) error {
								assert.Equal(t, activation, srv)
								handleCalls++
								return nil
							},
							RegisterHandler: func(srv interface{}, ctx context.Context, observer grain.Identity, registrationTimeout time.Duration, dec func(interface{}) error) error {
								assert.Equal(t, activation, srv)
								assert.Equal(t, observerIdentity, observer)
								registerCalls += 1
								return expectedError
							},
							UnsubscribeHandler: func(srv interface{}, ctx context.Context, observer grain.Identity) error {
								assert.Equal(t, activation, srv)
								assert.Equal(t, observerIdentity, observer)
								unsubscribeCalls += 1
								return expectedError
							},
						},
					},
				},
				impl: nil,
			},
		},
	}
	wg := sync.WaitGroup{}
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

	wg.Add(1)
	err = a.RegisterObserver(observerIdentity, "MyObservable", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.NoError(t, err)
	})
	require.NoError(t, err)
	wg.Wait()

	wg.Add(1)
	err = a.UnsubscribeObserver(observerIdentity, "MyObservable", func(err error) {
		defer wg.Done()
		assert.NoError(t, err)
	})
	require.NoError(t, err)
	wg.Wait()

	err = a.NotifyObservable(grain.Identity{
		GrainType: "Mytype",
		ID:        "Observable",
	}, "Mytype", "MyObservable", []byte{})
	require.NoError(t, err)

	wg.Add(1)
	expectedError = errors.New("expected error")
	err = a.RegisterObserver(observerIdentity, "MyObservable", []byte{}, time.Minute, func(err error) {
		defer wg.Done()
		assert.Equal(t, expectedError, err)
	})
	require.NoError(t, err)
	wg.Wait()

	wg.Add(1)
	err = a.UnsubscribeObserver(observerIdentity, "MyObservable", func(err error) {
		defer wg.Done()
		assert.Equal(t, expectedError, err)
	})
	require.NoError(t, err)
	wg.Wait()

	wg.Add(1)
	a.StopAsync(func(err error) {
		defer wg.Done()
		assert.NoError(t, err)
	})

	wg.Wait()

	require.Equal(t, 1, handleCalls)
	require.Equal(t, 2, registerCalls)
	require.Equal(t, 2, unsubscribeCalls)
}

func TestActivationGrainNotifyObserverDifferentTypes(t *testing.T) {
	observableIdentity := grain.Identity{
		GrainType: "MyObservableType",
		ID:        "Observer",
	}
	activation := &testActivation{
		grain.Identity{
			GrainType: "Mytype",
			ID:        "Myid",
		},
	}
	handleCalls := 0
	wg := sync.WaitGroup{}

	registrar := &mockRegistrar{
		entries: map[string]mockRegistrarEntry{
			"Mytype": {
				desc: &descriptor.GrainDescription{
					GrainType: "Mytype",
					Activation: descriptor.ActivationDesc{
						Handler: func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error) {
							return activation, nil
						},
					},
				},
			},
			"MyObservableType": {
				desc: &descriptor.GrainDescription{
					GrainType: "MyObservableType",
					Observables: []descriptor.ObservableDesc{
						{
							Name: "MyObservable",
							Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error) error {
								defer wg.Done()
								assert.Equal(t, activation, srv)
								handleCalls++
								return nil
							},
						},
					},
				},
			},
		},
	}
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

	wg.Add(1)
	err = a.NotifyObservable(observableIdentity, "MyObservableType", "MyObservable", []byte{})
	require.NoError(t, err)
	wg.Wait()

	wg.Add(1)
	a.StopAsync(func(err error) {
		defer wg.Done()
		assert.NoError(t, err)
	})
	wg.Wait()

	require.Equal(t, 1, handleCalls)
}
