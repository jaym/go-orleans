package observer

import (
	"time"

	"github.com/jaym/go-orleans/grain"
)

type AddOptions struct {
	Val     interface{}
	Expires time.Time
}

type AddOption func(*AddOptions)

func AddWithVal(v interface{}) AddOption {
	return func(o *AddOptions) {
		o.Val = v
	}
}

func AddWithExpiration(t time.Time) AddOption {
	return func(o *AddOptions) {
		o.Expires = t
	}
}

func ReadAddOptions(options *AddOptions, opts []AddOption) {
	for _, o := range opts {
		o(options)
	}
}

type RemoveOptions struct {
	ObserverGrain  *grain.Address
	ObservableName *string
}

type RemoveOption func(*RemoveOptions)

func RemoveByObserverGrain(observerAddress grain.Address) RemoveOption {
	return func(o *RemoveOptions) {
		o.ObserverGrain = &observerAddress
	}
}

func RemoveByObservableName(observableName string) RemoveOption {
	return func(o *RemoveOptions) {
		o.ObservableName = &observableName
	}
}

func ReadRemoveOptions(options *RemoveOptions, opts []RemoveOption) {
	for _, o := range opts {
		o(options)
	}
}
