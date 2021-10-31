package silo

import (
	"github.com/cockroachdb/errors"
	"github.com/jaym/go-orleans/grain/descriptor"
)

var ErrImplementationNotFound = errors.New("no implementation for grainType")

type registrarEntry struct {
	Description *descriptor.GrainDescription
	Impl        interface{}
}

type registrarImpl struct {
	entries map[string]registrarEntry
}

func (r *registrarImpl) Register(desc *descriptor.GrainDescription, impl interface{}) {
	r.entries[desc.GrainType] = registrarEntry{
		Description: desc,
		Impl:        impl,
	}
}

func (r *registrarImpl) Lookup(grainType string) (*descriptor.GrainDescription, interface{}, error) {
	e, ok := r.entries[grainType]
	if !ok {
		return nil, nil, errors.WithDetailf(ErrImplementationNotFound, "an implementation must be registerd for %s", grainType)
	}
	return e.Description, e.Impl, nil
}
