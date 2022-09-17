package codec

import (
	"encoding/json"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec/frame"
)

var ErrUnexpectedType = errors.New("unexpected type")
var ErrValuesConsumed = errors.New("no more values to consume")

type CodecV2 interface {
	Pack() FrameSerializer
	Unpack([]byte) (FrameDeserializer, error)
}

type FrameSerializer interface {
	grain.Serializer
	ToBytes() ([]byte, error)
}

type FrameDeserializer interface {
	grain.Deserializer
}

func NewBasicCodec() CodecV2 {
	return &codec{}
}

type codec struct{}

func (c *codec) Pack() FrameSerializer {
	return &frameSerializer{
		values: make([]*frame.Value, 0, 4),
	}
}

func (c *codec) Unpack(b []byte) (FrameDeserializer, error) {
	v := frame.Frame{}
	err := proto.Unmarshal(b, &v)
	if err != nil {
		return nil, err
	}
	return &frameDeserializer{
		values: v.Values,
	}, nil
}

type frameDeserializer struct {
	values     []*frame.Value
	currentIdx int
}

func (d *frameDeserializer) inc() {
	d.currentIdx++
}

func (d *frameDeserializer) Int64() (int64, error) {
	if d.currentIdx >= len(d.values) {
		return 0, ErrValuesConsumed
	}
	defer d.inc()
	v := d.values[d.currentIdx]
	if v.ValueType != frame.Value_INT64 {
		return 0, ErrUnexpectedType
	}
	return v.IntValue, nil
}

func (d *frameDeserializer) Bool() (bool, error) {
	if d.currentIdx >= len(d.values) {
		return false, ErrValuesConsumed
	}
	defer d.inc()
	v := d.values[d.currentIdx]
	if v.ValueType != frame.Value_BOOL {
		return false, ErrUnexpectedType
	}
	i := v.IntValue
	if i == 0 {
		return false, nil
	} else if i == 1 {
		return true, nil
	} else {
		return false, ErrUnexpectedType
	}
}

func (d *frameDeserializer) String() (string, error) {
	if d.currentIdx >= len(d.values) {
		return "", ErrValuesConsumed
	}
	defer d.inc()
	v := d.values[d.currentIdx]
	if v.ValueType != frame.Value_STRING {
		return "", ErrUnexpectedType
	}
	if v.BytesValue == nil {
		return "", nil
	}
	return string(v.BytesValue), nil
}

func (d *frameDeserializer) Bytes() ([]byte, error) {
	if d.currentIdx >= len(d.values) {
		return nil, ErrValuesConsumed
	}
	defer d.inc()
	v := d.values[d.currentIdx]
	if v.ValueType != frame.Value_STRING {
		return nil, ErrUnexpectedType
	}
	return v.BytesValue, nil
}

func (d *frameDeserializer) Interface(iface interface{}) error {
	if d.currentIdx >= len(d.values) {
		return ErrValuesConsumed
	}
	defer d.inc()
	v := d.values[d.currentIdx]
	switch v.ValueType {
	case frame.Value_PROTOBUF:
		m, ok := iface.(proto.Message)
		if !ok {
			return ErrUnexpectedType
		}
		return proto.Unmarshal(v.BytesValue, m)
	case frame.Value_JSON:
		return json.Unmarshal(v.BytesValue, iface)
	default:
		return ErrUnexpectedType
	}
}

type frameSerializer struct {
	err    error
	values []*frame.Value
}

func (s *frameSerializer) Int64(i int64) {
	if s.err != nil {
		return
	}
	s.values = append(s.values, &frame.Value{
		ValueType: frame.Value_INT64,
		IntValue:  i,
	})
}

func (s *frameSerializer) Bool(b bool) {
	if s.err != nil {
		return
	}
	var i int64
	if b {
		i = 1
	}
	s.values = append(s.values, &frame.Value{
		ValueType: frame.Value_BOOL,
		IntValue:  i,
	})
}

func (s *frameSerializer) String(b string) {
	if s.err != nil {
		return
	}
	if s.err != nil {
		return
	}
	s.values = append(s.values, &frame.Value{
		ValueType:  frame.Value_STRING,
		BytesValue: []byte(b),
	})
}

func (s *frameSerializer) Bytes(b []byte) {
	if s.err != nil {
		return
	}
	s.values = append(s.values, &frame.Value{
		ValueType:  frame.Value_BYTES,
		BytesValue: b,
	})
}

func (s *frameSerializer) Interface(iface interface{}) error {
	if s.err != nil {
		return nil
	}
	switch v := iface.(type) {
	case proto.Message:
		b, err := proto.Marshal(v)
		if err != nil {
			s.err = err
			return err
		}
		s.values = append(s.values, &frame.Value{
			ValueType:  frame.Value_PROTOBUF,
			BytesValue: b,
		})
	default:
		b, err := json.Marshal(v)
		if err != nil {
			s.err = err
			return err
		}
		s.values = append(s.values, &frame.Value{
			ValueType:  frame.Value_JSON,
			BytesValue: b,
		})
	}
	return nil
}

func (s *frameSerializer) ToBytes() ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := frame.Frame{
		Values: s.values,
	}
	return proto.Marshal(&out)
}
