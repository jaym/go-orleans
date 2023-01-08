package goor

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

type Grain interface {
	grain.GrainReference
}

type ObservableGrain interface {
	Grain
	UnregisterObserver(context.Context, grain.ObserverRegistrationToken)
	RefreshObserver(context.Context, grain.ObserverRegistrationToken) (grain.ObserverRegistrationToken, error)
}

type Observer interface {
	grain.GrainReference
}

// StatelessGrain is a grain that holds no state. It is always dispatched locally.
type StatelessGrain interface {
	grain.GrainReference
}
