package lethe

import "encoding/binary"

const (
	opBase uint64 = 0 // 0x0000000000000000

	opPut uint64 = opBase | (1 << 56) // 0x0100000000000000
	opDel uint64 = opBase | (2 << 56) // 0x0200000000000000, tombstone
)

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
	constKeyMetaLen int = 16
)

type keyMeta struct {
	seqNum uint64 // sequence number of operation
	opType uint64 // now, opType only uses the highest 8 bits
}

type sstFileMeta struct {
	// TODO
}

type deleteTileMeta struct {
	// TODO
}

type pageMeta struct {
	// TODO
}

// -------------------------------------------------

const (
	uint64EncodeLen = binary.MaxVarintLen64
)

// encodeEntry encodes entry to persistent format.
func encodeEntry(key, value, deleteKey []byte, meta keyMeta) (buf []byte, err error) {

	var lenMeta uint64 = 0
	lenMeta |= uint64(len(key)) << sortKeyLenOff
	lenMeta |= uint64(len(deleteKey)) << deleteKeyLenOff
	lenMeta |= uint64(len(value)) << valueLenOff

	// [ lenMeta(10) | seqNum(10) | opType(10) | key | deleteKey | value ]
	buf = make([]byte, 3*uint64EncodeLen+len(key)+len(deleteKey)+len(value))

	binary.PutUvarint(buf[0*uint64EncodeLen:], lenMeta)
	binary.PutUvarint(buf[1*uint64EncodeLen:], meta.seqNum)
	binary.PutUvarint(buf[2*uint64EncodeLen:], meta.opType)
	copy(buf[3*uint64EncodeLen:], key)
	copy(buf[3*uint64EncodeLen+len(key):], deleteKey)
	copy(buf[3*uint64EncodeLen+len(key)+len(deleteKey):], value)

	return buf, nil
}

// decodeEntry decodes entry from persistent format.
func decodeEntry(buf []byte) (key, value, deleteKey []byte, meta keyMeta, err error) {

	lenMeta, _ := binary.Uvarint(buf[0*uint64EncodeLen : 1*uint64EncodeLen])
	seqNum, _ := binary.Uvarint(buf[1*uint64EncodeLen : 2*uint64EncodeLen])
	opType, _ := binary.Uvarint(buf[2*uint64EncodeLen : 3*uint64EncodeLen])

	sortKeyLen := int((lenMeta & sortKeyLenMask) >> sortKeyLenOff)
	deleteKeyLen := int((lenMeta & deleteKeyLenMask) >> deleteKeyLenOff)
	valueLen := int((lenMeta & valueLenMask) >> valueLenOff)

	key = buf[3*uint64EncodeLen : 3*uint64EncodeLen+sortKeyLen]
	deleteKey = buf[3*uint64EncodeLen+sortKeyLen : 3*uint64EncodeLen+sortKeyLen+deleteKeyLen]
	value = buf[3*uint64EncodeLen+sortKeyLen+deleteKeyLen : 3*uint64EncodeLen+sortKeyLen+deleteKeyLen+valueLen]
	meta = keyMeta{
		seqNum: seqNum,
		opType: opType,
	}

	return key, value, deleteKey, meta, nil
}
