package psql

import (
	"time"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
)

type registeredObserver struct {
	codec codec.Codec
	grain.Identity
	observableName string
	expiresAt      time.Time
	val            []byte
}

func newRegisteredObserver(codec codec.Codec, identity grain.Identity, observableName string, expiresAt time.Time, val []byte) *registeredObserver {
	return &registeredObserver{
		Identity:       identity,
		observableName: observableName,
		codec:          codec,
		expiresAt:      expiresAt,
		val:            val,
	}
}

func (o *registeredObserver) Get(v interface{}) error {
	return o.codec.Decode(o.val, v)
}

func (o *registeredObserver) ObservableName() string {
	return o.observableName
}

func (o *registeredObserver) ExpiresAt() time.Time {
	return o.expiresAt
}
