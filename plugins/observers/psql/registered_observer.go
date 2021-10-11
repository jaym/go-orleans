package psql

import (
	"github.com/jaym/go-orleans/grain"
	"github.com/segmentio/ksuid"
)

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type registeredObserver struct {
	codec Codec
	grain.Address
	uuid ksuid.KSUID
	val  []byte
}

func newRegisteredObserver(codec Codec, address grain.Address, val []byte) (*registeredObserver, error) {
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
