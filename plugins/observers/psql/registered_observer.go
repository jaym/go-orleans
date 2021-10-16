package psql

import (
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/segmentio/ksuid"
)

type registeredObserver struct {
	codec codec.Codec
	grain.Address
	uuid ksuid.KSUID
	val  []byte
}

func newRegisteredObserver(codec codec.Codec, address grain.Address, val []byte) (*registeredObserver, error) {
	return &registeredObserver{
		Address: address,
		codec:   codec,
		uuid:    ksuid.New(),
		val:     val,
	}, nil
}

func (o *registeredObserver) UUID() string {
	return o.uuid.String()
}

func (o *registeredObserver) Get(v interface{}) error {
	return o.codec.Decode(o.val, v)
}
