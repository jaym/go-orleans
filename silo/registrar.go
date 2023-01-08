package silo

import (
	"github.com/cockroachdb/errors"
	"github.com/jaym/go-orleans/grain/descriptor"
)

var ErrImplementationNotFound = errors.New("no implementation for grainType")

type registrarEntryV2 struct {
	ActivatorConfig descriptor.ActivatorConfig
}

type registrarImpl struct {
	entriesV2 map[string]registrarEntryV2
}

func (r *registrarImpl) Register(grainType string, activatorFunc descriptor.ActivatorFunc, opts ...descriptor.ActivatorOpt) {
	c := descriptor.ActivatorConfig{
		ActivatorFunc: activatorFunc,
	}
	for _, o := range opts {
		o(&c)
	}
	r.entriesV2[grainType] = registrarEntryV2{
		ActivatorConfig: c,
	}
}

func (r *registrarImpl) Lookup(grainType string) (descriptor.ActivatorConfig, error) {
	e, ok := r.entriesV2[grainType]
	if !ok {
		return descriptor.ActivatorConfig{}, errors.WithDetailf(ErrImplementationNotFound, "an implementation must be registerd for %s", grainType)
	}
	return e.ActivatorConfig, nil
}
