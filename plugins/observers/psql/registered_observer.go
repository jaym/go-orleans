package psql

import (
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
)

type registeredObserver struct {
	codec codec.Codec
	grain.Identity
	observableName string
	val            []byte
}

func newRegisteredObserver(codec codec.Codec, address grain.Identity, observableName string, val []byte) *registeredObserver {
	return &registeredObserver{
		Identity:       address,
		observableName: observableName,
		codec:          codec,
		val:            val,
	}
}

func (o *registeredObserver) Get(v interface{}) error {
	return o.codec.Decode(o.val, v)
}

func (o *registeredObserver) ObservableName() string {
	return o.observableName
}
