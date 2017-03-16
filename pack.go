package msgpack

import (
	"io"
	"os"
	"reflect"
	"unsafe"
)

const (
	NIL = 0xc0

	FALSE = 0xc2
	TRUE  = 0xc3

	FLOAT  = 0xca
	DOUBLE = 0xcb

	UINT8  = 0xcc
	UINT16 = 0xcd
	UINT32 = 0xce
	UINT64 = 0xcf
	INT8   = 0xd0
	INT16  = 0xd1
	INT32  = 0xd2
	INT64  = 0xd3

	RAW16   = 0xda
	RAW32   = 0xdb
	ARRAY16 = 0xdc
	ARRAY32 = 0xdd
	MAP16   = 0xde
	MAP32   = 0xdf

	FIXMAP   = 0x80
	FIXARRAY = 0x90
	FIXRAW   = 0xa0

	MAXFIXMAP   = 16
	MAXFIXARRAY = 16
	MAXFIXRAW   = 32

	LEN_INT32 = 4
	LEN_INT64 = 8

	MAX16BIT = 2 << (16 - 1)

	REGULAR_UINT7_MAX  = 2 << (7 - 1)
	REGULAR_UINT8_MAX  = 2 << (8 - 1)
	REGULAR_UINT16_MAX = 2 << (16 - 1)
	REGULAR_UINT32_MAX = 2 << (32 - 1)

	SPECIAL_INT8  = 32
	SPECIAL_INT16 = 2 << (8 - 2)
	SPECIAL_INT32 = 2 << (16 - 2)
	SPECIAL_INT64 = 2 << (32 - 2)
)

type Bytes []byte

// Packs a given value and writes it into the specified writer.
func PackUint8(writer io.Writer, value uint8) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value >= REGULAR_UINT7_MAX {
		return writer.Write(Bytes{UINT8, value})
	}
	return writer.Write(Bytes{value})
}

// Packs a given value and writes it into the specified writer.
func PackUint16(writer io.Writer, value uint16) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value >= REGULAR_UINT8_MAX {
		return writer.Write(Bytes{UINT16, byte(value >> 8), byte(value)})
	}
	return PackUint8(writer, uint8(value))
}

// Packs a given value and writes it into the specified writer.
func PackUint32(writer io.Writer, value uint32) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value >= REGULAR_UINT16_MAX {
		return writer.Write(Bytes{UINT32, byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
	}
	return PackUint16(writer, uint16(value))
}

// Packs a given value and writes it into the specified writer.
func PackUint64(writer io.Writer, value uint64) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value >= REGULAR_UINT32_MAX {
		return writer.Write(Bytes{UINT64, byte(value >> 56), byte(value >> 48), byte(value >> 40), byte(value >> 32), byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
	}
	return PackUint32(writer, uint32(value))
}

// Packs a given value and writes it into the specified writer.
func PackUint(writer io.Writer, value uint) (n int, err error) {
	switch unsafe.Sizeof(value) {
	case LEN_INT32:
		return PackUint32(writer, *(*uint32)(unsafe.Pointer(&value)))
	case LEN_INT64:
		return PackUint64(writer, *(*uint64)(unsafe.Pointer(&value)))
	}
	return 0, os.ErrNotExist // never get here
}

// Packs a given value and writes it into the specified writer.
func PackInt8(writer io.Writer, value int8) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value < -SPECIAL_INT8 {
		return writer.Write(Bytes{INT8, byte(value)})
	}
	return writer.Write(Bytes{byte(value)})
}

// Packs a given value and writes it into the specified writer.
func PackInt16(writer io.Writer, value int16) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value < -SPECIAL_INT16 || value >= SPECIAL_INT16 {
		return writer.Write(Bytes{INT16, byte(uint16(value) >> 8), byte(value)})
	}
	return PackInt8(writer, int8(value))
}

// Packs a given value and writes it into the specified writer.
func PackInt32(writer io.Writer, value int32) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value < -SPECIAL_INT32 || value >= SPECIAL_INT32 {
		return writer.Write(Bytes{INT32, byte(uint32(value) >> 24), byte(uint32(value) >> 16), byte(uint32(value) >> 8), byte(value)})
	}
	return PackInt16(writer, int16(value))
}

// Packs a given value and writes it into the specified writer.
func PackInt64(writer io.Writer, value int64) (n int, err error) {
	// Assume the numbers outside of range is the least common case
	if value < -SPECIAL_INT64 || value >= SPECIAL_INT64 {
		return writer.Write(Bytes{INT64, byte(uint64(value) >> 56), byte(uint64(value) >> 48), byte(uint64(value) >> 40), byte(uint64(value) >> 32), byte(uint64(value) >> 24), byte(uint64(value) >> 16), byte(uint64(value) >> 8), byte(value)})
	}
	return PackInt32(writer, int32(value))
}

// Packs a given value and writes it into the specified writer.
func PackInt(writer io.Writer, value int) (n int, err error) {
	switch unsafe.Sizeof(value) {
	case LEN_INT32:
		return PackInt32(writer, *(*int32)(unsafe.Pointer(&value)))
	case LEN_INT64:
		return PackInt64(writer, *(*int64)(unsafe.Pointer(&value)))
	}
	return 0, os.ErrNotExist // never get here
}

// Packs a given value and writes it into the specified writer.
func PackNil(writer io.Writer) (n int, err error) {
	return writer.Write(Bytes{NIL})
}

// Packs a given value and writes it into the specified writer.
func PackBool(writer io.Writer, value bool) (n int, err error) {
	if value {
		return writer.Write(Bytes{TRUE})
	}
	return writer.Write(Bytes{FALSE})
}

// Packs a given value and writes it into the specified writer.
func PackFloat32(writer io.Writer, value float32) (n int, err error) {
	return PackUint32(writer, *(*uint32)(unsafe.Pointer(&value)))
}

// Packs a given value and writes it into the specified writer.
func PackFloat64(writer io.Writer, value float64) (n int, err error) {
	return PackUint64(writer, *(*uint64)(unsafe.Pointer(&value)))
}

// Packs a given value and writes it into the specified writer.
func PackBytes(writer io.Writer, value []byte) (n int, err error) {
	length := len(value)
	if length < MAXFIXRAW {
		n1, err := writer.Write(Bytes{FIXRAW | uint8(length)})
		if err != nil {
			return n1, err
		}
		n2, err := writer.Write(value)
		return n1 + n2, err
	} else if length < MAX16BIT {
		n1, err := writer.Write(Bytes{RAW16, byte(length >> 8), byte(length)})
		if err != nil {
			return n1, err
		}
		n2, err := writer.Write(value)
		return n1 + n2, err
	}
	n1, err := writer.Write(Bytes{RAW32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
	if err != nil {
		return n1, err
	}
	n2, err := writer.Write(value)
	return n1 + n2, err
}

// Packs a given value and writes it into the specified writer.
func PackUint16Array(writer io.Writer, value []uint16) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint16(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint16(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint16(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackUint32Array(writer io.Writer, value []uint32) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackUint64Array(writer io.Writer, value []uint64) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackUint64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackUintArray(writer io.Writer, value []uint) (n int, err error) {
	switch unsafe.Sizeof(0) {
	case 4:
		return PackUint32Array(writer, *(*[]uint32)(unsafe.Pointer(&value)))
	case 8:
		return PackUint64Array(writer, *(*[]uint64)(unsafe.Pointer(&value)))
	}
	return 0, os.ErrNotExist // never get here
}

// Packs a given value and writes it into the specified writer.
func PackInt8Array(writer io.Writer, value []int8) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt8(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt8(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt8(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackInt16Array(writer io.Writer, value []int16) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt16(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt16(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt16(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackInt32Array(writer io.Writer, value []int32) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackInt64Array(writer io.Writer, value []int64) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackInt64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackIntArray(writer io.Writer, value []int) (n int, err error) {
	switch unsafe.Sizeof(0) {
	case LEN_INT32:
		return PackInt32Array(writer, *(*[]int32)(unsafe.Pointer(&value)))
	case LEN_INT64:
		return PackInt64Array(writer, *(*[]int64)(unsafe.Pointer(&value)))
	}
	return 0, os.ErrNotExist // never get here
}

// Packs a given value and writes it into the specified writer.
func PackFloat32Array(writer io.Writer, value []float32) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackFloat32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackFloat32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackFloat32(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackFloat64Array(writer io.Writer, value []float64) (n int, err error) {
	length := len(value)
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackFloat64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackFloat64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, i := range value {
			_n, err := PackFloat64(writer, i)
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackArray(writer io.Writer, value reflect.Value) (n int, err error) {
	{
		elemType := value.Type().Elem()
		if (elemType.Kind() == reflect.Uint || elemType.Kind() == reflect.Uint8 || elemType.Kind() == reflect.Uint16 || elemType.Kind() == reflect.Uint32 || elemType.Kind() == reflect.Uint64 || elemType.Kind() == reflect.Uintptr) &&
			elemType.Kind() == reflect.Uint8 {
			return PackBytes(writer, value.Interface().([]byte))
		}
	}

	length := value.Len()
	if length < MAXFIXARRAY {
		n, err := writer.Write(Bytes{FIXARRAY | byte(length)})
		if err != nil {
			return n, err
		}
		for i := 0; i < length; i++ {
			_n, err := PackValue(writer, value.Index(i))
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{ARRAY16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for i := 0; i < length; i++ {
			_n, err := PackValue(writer, value.Index(i))
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{ARRAY32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for i := 0; i < length; i++ {
			_n, err := PackValue(writer, value.Index(i))
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackMap(writer io.Writer, value reflect.Value) (n int, err error) {
	keys := value.MapKeys()
	length := len(keys)
	if length < MAXFIXMAP {
		n, err := writer.Write(Bytes{FIXMAP | byte(length)})
		if err != nil {
			return n, err
		}
		for _, k := range keys {
			_n, err := PackValue(writer, k)
			if err != nil {
				return n, err
			}
			n += _n
			_n, err = PackValue(writer, value.MapIndex(k))
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else if length < MAX16BIT {
		n, err := writer.Write(Bytes{MAP16, byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, k := range keys {
			_n, err := PackValue(writer, k)
			if err != nil {
				return n, err
			}
			n += _n
			_n, err = PackValue(writer, value.MapIndex(k))
			if err != nil {
				return n, err
			}
			n += _n
		}
	} else {
		n, err := writer.Write(Bytes{MAP32, byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)})
		if err != nil {
			return n, err
		}
		for _, k := range keys {
			_n, err := PackValue(writer, k)
			if err != nil {
				return n, err
			}
			n += _n
			_n, err = PackValue(writer, value.MapIndex(k))
			if err != nil {
				return n, err
			}
			n += _n
		}
	}
	return n, nil
}

// Packs a given value and writes it into the specified writer.
func PackValue(writer io.Writer, value reflect.Value) (n int, err error) {
	if !value.IsValid() || value.Type() == nil {
		return PackNil(writer)
	}
	switch _value := value; _value.Kind() {
	case reflect.Bool:
		return PackBool(writer, _value.Bool())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return PackUint64(writer, _value.Uint())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return PackInt64(writer, _value.Int())
	case reflect.Float32, reflect.Float64:
		return PackFloat64(writer, _value.Float())
	case reflect.Array:
		return PackArray(writer, _value)
	case reflect.Slice:
		return PackArray(writer, _value)
	case reflect.Map:
		return PackMap(writer, _value)
	case reflect.String:
		return PackBytes(writer, []byte(_value.String()))
	case reflect.Interface:
		__value := reflect.ValueOf(_value.Interface())

		if __value.Kind() != reflect.Interface {
			return PackValue(writer, __value)
		}
	}
	panic("unsupported type: " + value.Type().String())
}

// Packs a given value and writes it into the specified writer.
func Pack(writer io.Writer, value interface{}) (n int, err error) {
	if value == nil {
		return PackNil(writer)
	}
	switch _value := value.(type) {
	case bool:
		return PackBool(writer, _value)
	case uint8:
		return PackUint8(writer, _value)
	case uint16:
		return PackUint16(writer, _value)
	case uint32:
		return PackUint32(writer, _value)
	case uint64:
		return PackUint64(writer, _value)
	case uint:
		return PackUint(writer, _value)
	case int8:
		return PackInt8(writer, _value)
	case int16:
		return PackInt16(writer, _value)
	case int32:
		return PackInt32(writer, _value)
	case int64:
		return PackInt64(writer, _value)
	case int:
		return PackInt(writer, _value)
	case float32:
		return PackFloat32(writer, _value)
	case float64:
		return PackFloat64(writer, _value)
	case []byte:
		return PackBytes(writer, _value)
	case []uint16:
		return PackUint16Array(writer, _value)
	case []uint32:
		return PackUint32Array(writer, _value)
	case []uint64:
		return PackUint64Array(writer, _value)
	case []uint:
		return PackUintArray(writer, _value)
	case []int8:
		return PackInt8Array(writer, _value)
	case []int16:
		return PackInt16Array(writer, _value)
	case []int32:
		return PackInt32Array(writer, _value)
	case []int64:
		return PackInt64Array(writer, _value)
	case []int:
		return PackIntArray(writer, _value)
	case []float32:
		return PackFloat32Array(writer, _value)
	case []float64:
		return PackFloat64Array(writer, _value)
	case string:
		return PackBytes(writer, Bytes(_value))
	default:
		return PackValue(writer, reflect.ValueOf(value))
	}
	return 0, nil // never get here
}
