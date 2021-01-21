package lethe

import "encoding/binary"

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

// ------------------------------------------------------------------------------------
// persist format
// ------------------------------------------------------------------------------------
// [ lenMeta(10) | seqNum(10) | opType(10) | key | value | deleteKey ]
// ------------------------------------------------------------------------------------

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
func decodeEntry(buf []byte, e *entry) (err error) {

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

	return nil
}

// -------------------------------------------------------------------------------------------------
