package silo

import (
	"github.com/cockroachdb/errors"
	"github.com/jaym/go-orleans/grain/descriptor"
)

var ErrImplementationNotFound = errors.New("no implementation for grainType")

type registrarEntryV2 struct {
	ActivatorFunc descriptor.ActivatorFunc
}

type registrarImpl struct {
	entriesV2 map[string]registrarEntryV2
}

func (r *registrarImpl) RegisterV2(grainType string, activatorFunc descriptor.ActivatorFunc) {
	r.entriesV2[grainType] = registrarEntryV2{
		ActivatorFunc: activatorFunc,
	}
}

func (r *registrarImpl) LookupV2(grainType string) (descriptor.ActivatorFunc, error) {
	e, ok := r.entriesV2[grainType]
	if !ok {
		return nil, errors.WithDetailf(ErrImplementationNotFound, "an implementation must be registerd for %s", grainType)
	}
	return e.ActivatorFunc, nil
}
