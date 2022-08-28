package codec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type Foo struct {
	Bar string
}

func TestCodec(t *testing.T) {
	c := NewBasicCodec()

	ser := c.Pack()
	ser.Bool(true)
	ser.Bool(false)
	ser.Bytes([]byte("bytes1"))
	ser.String("string1")
	ser.Int64(0)
	ser.Int64(73)
	ser.Int64(-654)
	ser.UInt64(0)
	ser.UInt64(99)
	err := ser.Interface(Foo{Bar: "Baz"})
	require.NoError(t, err)
	ser.StringList([]string{"foo", "bar", "baz"})
	ser.BytesList([][]byte{[]byte("foo1"), []byte("bar1"), []byte("baz1")})
	ser.BoolList([]bool{true, false, true, false, false})
	ser.Int64List([]int64{-1, 0, 1})
	ser.UInt64List([]uint64{2, 3, 4})

	data, err := ser.ToBytes()
	require.NoError(t, err)

	dec, err := c.Unpack(data)
	require.NoError(t, err)

	bool1, err := dec.Bool()
	require.NoError(t, err)
	require.Equal(t, true, bool1)

	bool2, err := dec.Bool()
	require.NoError(t, err)
	require.Equal(t, false, bool2)

	bytes1, err := dec.Bytes()
	require.NoError(t, err)
	require.Equal(t, []byte("bytes1"), bytes1)

	string1, err := dec.String()
	require.NoError(t, err)
	require.Equal(t, "string1", string1)

	int1, err := dec.Int64()
	require.NoError(t, err)
	require.EqualValues(t, 0, int1)

	int2, err := dec.Int64()
	require.NoError(t, err)
	require.EqualValues(t, 73, int2)

	int3, err := dec.Int64()
	require.NoError(t, err)
	require.EqualValues(t, -654, int3)

	uint1, err := dec.UInt64()
	require.NoError(t, err)
	require.EqualValues(t, 0, uint1)

	uint2, err := dec.UInt64()
	require.NoError(t, err)
	require.EqualValues(t, 99, uint2)

	foo := Foo{}
	err = dec.Interface(&foo)
	require.NoError(t, err)
	require.Equal(t, Foo{Bar: "Baz"}, foo)

	stringList1, err := dec.StringList()
	require.NoError(t, err)
	require.Equal(t, []string{"foo", "bar", "baz"}, stringList1)

	bytesList1, err := dec.BytesList()
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("foo1"), []byte("bar1"), []byte("baz1")}, bytesList1)

	boolList1, err := dec.BoolList()
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true, false, false}, boolList1)

	int64List1, err := dec.Int64List()
	require.NoError(t, err)
	require.Equal(t, []int64{-1, 0, 1}, int64List1)

	uint64List1, err := dec.UInt64List()
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3, 4}, uint64List1)

	_, err = dec.Bool()
	require.Equal(t, ErrValuesConsumed, err)
	_, err = dec.Bytes()
	require.Equal(t, ErrValuesConsumed, err)
	_, err = dec.String()
	require.Equal(t, ErrValuesConsumed, err)
	_, err = dec.Int64()
	require.Equal(t, ErrValuesConsumed, err)
	_, err = dec.UInt64()
	require.Equal(t, ErrValuesConsumed, err)
	err = dec.Interface(nil)
	require.Equal(t, ErrValuesConsumed, err)
	_, err = dec.StringList()
	require.Equal(t, ErrValuesConsumed, err)
}
