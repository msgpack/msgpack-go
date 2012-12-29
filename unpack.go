package msgpack

import (
	"io"
	"reflect"
	"strconv"
	"unsafe"
)

type (
	Bytes1 [1]byte
	Bytes2 [2]byte
	Bytes4 [4]byte
	Bytes8 [8]byte
)

const (
	NEGFIXNUM     = 0xe0
	FIXMAPMAX     = 0x8f
	FIXARRAYMAX   = 0x9f
	FIXRAWMAX     = 0xbf
	FIRSTBYTEMASK = 0xf
)

func readByte(reader io.Reader) (v uint8, err error) {
	var data Bytes1
	_, e := reader.Read(data[0:])
	if e != nil {
		return 0, e
	}
	return data[0], nil
}

func readUint16(reader io.Reader) (v uint16, n int, err error) {
	var data Bytes2
	n, e := reader.Read(data[0:])
	if e != nil {
		return 0, n, e
	}
	return (uint16(data[0]) << 8) | uint16(data[1]), n, nil
}

func readUint32(reader io.Reader) (v uint32, n int, err error) {
	var data Bytes4
	n, e := reader.Read(data[0:])
	if e != nil {
		return 0, n, e
	}
	return (uint32(data[0]) << 24) | (uint32(data[1]) << 16) | (uint32(data[2]) << 8) | uint32(data[3]), n, nil
}

func readUint64(reader io.Reader) (v uint64, n int, err error) {
	var data Bytes8
	n, e := reader.Read(data[0:])
	if e != nil {
		return 0, n, e
	}
	return (uint64(data[0]) << 56) | (uint64(data[1]) << 48) | (uint64(data[2]) << 40) | (uint64(data[3]) << 32) | (uint64(data[4]) << 24) | (uint64(data[5]) << 16) | (uint64(data[6]) << 8) | uint64(data[7]), n, nil
}

func readInt16(reader io.Reader) (v int16, n int, err error) {
	var data Bytes2
	n, e := reader.Read(data[0:])
	if e != nil {
		return 0, n, e
	}
	return (int16(data[0]) << 8) | int16(data[1]), n, nil
}

func readInt32(reader io.Reader) (v int32, n int, err error) {
	var data Bytes4
	n, e := reader.Read(data[0:])
	if e != nil {
		return 0, n, e
	}
	return (int32(data[0]) << 24) | (int32(data[1]) << 16) | (int32(data[2]) << 8) | int32(data[3]), n, nil
}

func readInt64(reader io.Reader) (v int64, n int, err error) {
	var data Bytes8
	n, e := reader.Read(data[0:])
	if e != nil {
		return 0, n, e
	}
	return (int64(data[0]) << 56) | (int64(data[1]) << 48) | (int64(data[2]) << 40) | (int64(data[3]) << 32) | (int64(data[4]) << 24) | (int64(data[5]) << 16) | (int64(data[6]) << 8) | int64(data[7]), n, nil
}

func unpackArray(reader io.Reader, nelems uint) (v reflect.Value, n int, err error) {
	var i uint
	var nbytesread int
	retval := make([]interface{}, nelems)

	for i = 0; i < nelems; i++ {
		v, n, err = Unpack(reader)
		nbytesread += n
		if err != nil {
			return reflect.Value{}, nbytesread, err
		}
		retval[i] = v.Interface()
	}
	return reflect.ValueOf(retval), nbytesread, nil
}

func unpackArrayReflected(reader io.Reader, nelems uint) (v reflect.Value, n int, err error) {
	var i uint
	var nbytesread int
	retval := make([]reflect.Value, nelems)

	for i = 0; i < nelems; i++ {
		v, n, err = UnpackReflected(reader)
		nbytesread += n
		if err != nil {
			return reflect.Value{}, nbytesread, err
		}
		retval[i] = v
	}
	return reflect.ValueOf(retval), nbytesread, nil
}

func unpackMap(reader io.Reader, nelems uint) (v reflect.Value, n int, err error) {
	var i uint
	var nbytesread int
	var k reflect.Value
	retval := make(map[interface{}]interface{})

	for i = 0; i < nelems; i++ {
		k, n, err = Unpack(reader)
		nbytesread += n
		if err != nil {
			return reflect.Value{}, nbytesread, err
		}
		v, n, err = Unpack(reader)
		nbytesread += n
		if err != nil {
			return reflect.Value{}, nbytesread, err
		}
		ktyp := k.Type()
		if ktyp.Kind() == reflect.Slice && ktyp.Elem().Kind() == reflect.Uint8 {
			retval[string(k.Interface().([]uint8))] = v.Interface()
		} else {
			retval[k.Interface()] = v.Interface()
		}
	}
	return reflect.ValueOf(retval), nbytesread, nil
}

func unpackMapReflected(reader io.Reader, nelems uint) (v reflect.Value, n int, err error) {
	var i uint
	var nbytesread int
	var k reflect.Value
	retval := make(map[interface{}]reflect.Value)

	for i = 0; i < nelems; i++ {
		k, n, err = UnpackReflected(reader)
		nbytesread += n
		if err != nil {
			return reflect.Value{}, nbytesread, err
		}
		v, n, err = UnpackReflected(reader)
		nbytesread += n
		if err != nil {
			return reflect.Value{}, nbytesread, err
		}
		retval[k] = v
	}
	return reflect.ValueOf(retval), nbytesread, nil
}

// Get the four lowest bits
func lownibble(u8 uint8) uint {
	return uint(u8 & 0xf)
}

// Get the five lowest bits
func lowfive(u8 uint8) uint {
	return uint(u8 & 0x1f)
}

func unpack(reader io.Reader, reflected bool) (v reflect.Value, n int, err error) {
	var retval reflect.Value
	var nbytesread int

	c, e := readByte(reader)
	if e != nil {
		return reflect.Value{}, 0, e
	}
	nbytesread++
	if c < FIXMAP || c >= NEGFIXNUM {
		retval = reflect.ValueOf(int8(c))
	} else if c >= FIXMAP && c <= FIXMAPMAX {
		if reflected {
			retval, n, e = unpackMapReflected(reader, lownibble(c))
		} else {
			retval, n, e = unpackMap(reader, lownibble(c))
		}
		nbytesread += n
		if e != nil {
			return reflect.Value{}, nbytesread, e
		}
		nbytesread += n
	} else if c >= FIXARRAY && c <= FIXARRAYMAX {
		if reflected {
			retval, n, e = unpackArrayReflected(reader, lownibble(c))
		} else {
			retval, n, e = unpackArray(reader, lownibble(c))
		}
		nbytesread += n
		if e != nil {
			return reflect.Value{}, nbytesread, e
		}
		nbytesread += n
	} else if c >= FIXRAW && c <= FIXRAWMAX {
		data := make([]byte, lowfive(c))
		n, e := reader.Read(data)
		nbytesread += n
		if e != nil {
			return reflect.Value{}, nbytesread, e
		}
		retval = reflect.ValueOf(data)
	} else {
		switch c {
		case NIL:
			retval = reflect.ValueOf(nil)
		case FALSE:
			retval = reflect.ValueOf(false)
		case TRUE:
			retval = reflect.ValueOf(true)
		case FLOAT:
			data, n, e := readUint32(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(*(*float32)(unsafe.Pointer(&data)))
		case DOUBLE:
			data, n, e := readUint64(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(*(*float64)(unsafe.Pointer(&data)))
		case UINT8:
			data, e := readByte(reader)
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(uint8(data))
			nbytesread++
		case UINT16:
			data, n, e := readUint16(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case UINT32:
			data, n, e := readUint32(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case UINT64:
			data, n, e := readUint64(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case INT8:
			data, e := readByte(reader)
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(int8(data))
			nbytesread++
		case INT16:
			data, n, e := readInt16(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case INT32:
			data, n, e := readInt32(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case INT64:
			data, n, e := readInt64(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case RAW16:
			nbytestoread, n, e := readUint16(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			data := make([]byte, nbytestoread)
			n, e = reader.Read(data)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case RAW32:
			nbytestoread, n, e := readUint32(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			data := make(Bytes, nbytestoread)
			n, e = reader.Read(data)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			retval = reflect.ValueOf(data)
		case ARRAY16:
			nelemstoread, n, e := readUint16(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			if reflected {
				retval, n, e = unpackArrayReflected(reader, uint(nelemstoread))
			} else {
				retval, n, e = unpackArray(reader, uint(nelemstoread))
			}
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
		case ARRAY32:
			nelemstoread, n, e := readUint32(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			if reflected {
				retval, n, e = unpackArrayReflected(reader, uint(nelemstoread))
			} else {
				retval, n, e = unpackArray(reader, uint(nelemstoread))
			}
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
		case MAP16:
			nelemstoread, n, e := readUint16(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			if reflected {
				retval, n, e = unpackMapReflected(reader, uint(nelemstoread))
			} else {
				retval, n, e = unpackMap(reader, uint(nelemstoread))
			}
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
		case MAP32:
			nelemstoread, n, e := readUint32(reader)
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
			if reflected {
				retval, n, e = unpackMapReflected(reader, uint(nelemstoread))
			} else {
				retval, n, e = unpackMap(reader, uint(nelemstoread))
			}
			nbytesread += n
			if e != nil {
				return reflect.Value{}, nbytesread, e
			}
		default:
			panic("unsupported code: " + strconv.Itoa(int(c)))
		}
	}
	return retval, nbytesread, nil
}

// Reads a value from the reader, unpack and returns it.
func Unpack(reader io.Reader) (v reflect.Value, n int, err error) {
	return unpack(reader, false)
}

// Reads unpack a value from the reader, unpack and returns it.  When the
// value is an array or map, leaves the elements wrapped by corresponding
// wrapper objects defined in reflect package.
func UnpackReflected(reader io.Reader) (v reflect.Value, n int, err error) {
	return unpack(reader, true)
}
