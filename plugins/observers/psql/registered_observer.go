package psql

import (
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
)

type registeredObserver struct {
	codec codec.Codec
	grain.Address
	val []byte
}

func newRegisteredObserver(codec codec.Codec, address grain.Address, val []byte) (*registeredObserver, error) {
	return &registeredObserver{
		Address: address,
		codec:   codec,
		val:     val,
	}, nil
}

func (o *registeredObserver) Get(v interface{}) error {
	return o.codec.Decode(o.val, v)
}
