package protobuf

import (
	"github.com/jaym/go-orleans/plugins/codec"
	"google.golang.org/protobuf/proto"
)

type protobufCodec struct{}

func NewCodec() codec.Codec {
	return protobufCodec{}
}

func (protobufCodec) Encode(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

func (protobufCodec) Decode(b []byte, v interface{}) error {
	return proto.Unmarshal(b, v.(proto.Message))
}
