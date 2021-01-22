package lethe

import (
	"bytes"
	"encoding/binary"
)

const (
	opBase uint64 = 0                  // 0x0000000000000000
	opPut  uint64 = opBase | (1 << 56) // 0x0100000000000000
	opDel  uint64 = opBase | (2 << 56) // 0x0200000000000000, tombstone
)

type keyMeta struct {
	seqNum uint64 // sequence number of operation
	opType uint64 // now, opType only uses the highest 8 bits
}

// -------------------------------------------------------------------------------------------------

type entry struct {
	key       []byte
	value     []byte
	deleteKey []byte
	meta      keyMeta
}

func entryEqual(e, e2 *entry) bool {

	if !bytes.Equal(e.key, e2.key) {
		return false
	}

	if !bytes.Equal(e.value, e2.value) {
		return false

	}
	if !bytes.Equal(e.deleteKey, e2.deleteKey) {
		return false

	}
	if (e.meta.seqNum != e2.meta.seqNum) || (e.meta.opType != e2.meta.opType) {
		return false
	}

	return true
}

// ------------------------------------------------------------------------------------
// persist format
// ------------------------------------------------------------------------------------
// [ lenMeta(10) | seqNum(10) | opType(10) | key | value | deleteKey ]
// ------------------------------------------------------------------------------------

const (
	maxSortKeyBytesLen   int = (1 << 16) - 1
	maxDeleteKeyBytesLen int = (1 << 16) - 1
	maxValueBytesLen     int = (1 << 32) - 1

	sortKeyLenOff   uint64 = 48
	deleteKeyLenOff uint64 = 32
	valueLenOff     uint64 = 0

	sortKeyLenMask   uint64 = 0xffff << sortKeyLenOff
	deleteKeyLenMask uint64 = 0xffff << deleteKeyLenOff
	valueLenMask     uint64 = 0xffffffff << valueLenOff
)

const (
	uint64EncodeLen = binary.MaxVarintLen64 // const 10
)

// persistFormatLen returns the length of persistent format.
// pure function
func persistFormatLen(e *entry) int {
	// 3*uint64EncodeLen means lenMeta(10), seqNum(10), opType(10)
	return (3*uint64EncodeLen + len(e.key) + len(e.deleteKey) + len(e.value))
}

// encodeEntry allocates a new byte buffer to save persistent format and encodes entry to the buffer.
func encodeEntry(e *entry) (buf []byte, err error) {

	key := e.key
	value := e.value
	deleteKey := e.deleteKey
	meta := e.meta

	var lenMeta uint64 = 0
	lenMeta |= uint64(len(key)) << sortKeyLenOff
	lenMeta |= uint64(len(deleteKey)) << deleteKeyLenOff
	lenMeta |= uint64(len(value)) << valueLenOff

	buf = make([]byte, persistFormatLen(e))

	binary.PutUvarint(buf[0*uint64EncodeLen:], lenMeta)
	binary.PutUvarint(buf[1*uint64EncodeLen:], meta.seqNum)
	binary.PutUvarint(buf[2*uint64EncodeLen:], meta.opType)
	copy(buf[3*uint64EncodeLen:], key)
	copy(buf[3*uint64EncodeLen+len(key):], value)
	copy(buf[3*uint64EncodeLen+len(key)+len(value):], deleteKey)

	return buf, nil
}

// decodeEntry decodes entry from persistent format.
// Note that decodeEntrys will NOT allocate new buffers but occupy the input buf.
func decodeEntry(buf []byte) (entry, error) {

	e := entry{}

	lenMeta, _ := binary.Uvarint(buf[0*uint64EncodeLen : 1*uint64EncodeLen])
	seqNum, _ := binary.Uvarint(buf[1*uint64EncodeLen : 2*uint64EncodeLen])
	opType, _ := binary.Uvarint(buf[2*uint64EncodeLen : 3*uint64EncodeLen])

	sortKeyLen := int((lenMeta & sortKeyLenMask) >> sortKeyLenOff)
	valueLen := int((lenMeta & valueLenMask) >> valueLenOff)
	deleteKeyLen := int((lenMeta & deleteKeyLenMask) >> deleteKeyLenOff)

	e.key = buf[3*uint64EncodeLen : 3*uint64EncodeLen+sortKeyLen]
	e.value = buf[3*uint64EncodeLen+sortKeyLen : 3*uint64EncodeLen+sortKeyLen+valueLen]
	e.deleteKey = buf[3*uint64EncodeLen+sortKeyLen+valueLen : 3*uint64EncodeLen+sortKeyLen+valueLen+deleteKeyLen]

	e.meta = keyMeta{
		seqNum: seqNum,
		opType: opType,
	}

	return e, nil
}

func encodeEntries(es []entry) (buf []byte, err error) {

	buf = []byte{}

	for i := 0; i < len(es); i++ {

		b, err := encodeEntry(&es[i])
		if err != nil {
			return nil, err
		}

		buf = append(buf, b...)
	}

	return buf, nil
}

func decodeEntries(buf []byte) ([]entry, error) {

	es := []entry{}

	start := 0
	for start < len(buf) {

		e, err := decodeEntry(buf[start:])
		if err != nil {
			return nil, err
		}

		es = append(es, e)
		start += persistFormatLen(&e)
	}

	return es, nil
}

// -------------------------------------------------------------------------------------------------
