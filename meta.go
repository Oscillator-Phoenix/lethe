package lethe

import "encoding/binary"

// -------------------------------------------------------------------------------------------------

type keyMeta struct {
	seqNum uint64 // sequence number of operation
	opType uint64 // now, opType only uses the highest 8 bits
}

const (
	opBase uint64 = 0                  // 0x0000000000000000
	opPut  uint64 = opBase | (1 << 56) // 0x0100000000000000
	opDel  uint64 = opBase | (2 << 56) // 0x0200000000000000, tombstone
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
	uint64EncodeLen = binary.MaxVarintLen64 // const 10
)

// entryPersistFormatLen returns the length of persistent format.
func entryPersistFormatLen(key, value, deleteKey []byte) int {
	return (3*uint64EncodeLen + len(key) + len(deleteKey) + len(value))
}

// encodeEntry encodes entry to persistent format.
func encodeEntry(key, value, deleteKey []byte, meta keyMeta) (buf []byte, err error) {

	var lenMeta uint64 = 0
	lenMeta |= uint64(len(key)) << sortKeyLenOff
	lenMeta |= uint64(len(deleteKey)) << deleteKeyLenOff
	lenMeta |= uint64(len(value)) << valueLenOff

	// persist format [ lenMeta(10) | seqNum(10) | opType(10) | key | value | deleteKey ]
	buf = make([]byte, entryPersistFormatLen(key, value, deleteKey))

	binary.PutUvarint(buf[0*uint64EncodeLen:], lenMeta)
	binary.PutUvarint(buf[1*uint64EncodeLen:], meta.seqNum)
	binary.PutUvarint(buf[2*uint64EncodeLen:], meta.opType)
	copy(buf[3*uint64EncodeLen:], key)
	copy(buf[3*uint64EncodeLen+len(key):], value)
	copy(buf[3*uint64EncodeLen+len(key)+len(value):], deleteKey)

	return buf, nil
}

// decodeEntry decodes entry from persistent format.
func decodeEntry(buf []byte) (key, value, deleteKey []byte, meta keyMeta, err error) {

	lenMeta, _ := binary.Uvarint(buf[0*uint64EncodeLen : 1*uint64EncodeLen])
	seqNum, _ := binary.Uvarint(buf[1*uint64EncodeLen : 2*uint64EncodeLen])
	opType, _ := binary.Uvarint(buf[2*uint64EncodeLen : 3*uint64EncodeLen])

	sortKeyLen := int((lenMeta & sortKeyLenMask) >> sortKeyLenOff)
	valueLen := int((lenMeta & valueLenMask) >> valueLenOff)
	deleteKeyLen := int((lenMeta & deleteKeyLenMask) >> deleteKeyLenOff)

	key = buf[3*uint64EncodeLen : 3*uint64EncodeLen+sortKeyLen]
	value = buf[3*uint64EncodeLen+sortKeyLen : 3*uint64EncodeLen+sortKeyLen+valueLen]
	deleteKey = buf[3*uint64EncodeLen+sortKeyLen+valueLen : 3*uint64EncodeLen+sortKeyLen+valueLen+deleteKeyLen]

	meta = keyMeta{
		seqNum: seqNum,
		opType: opType,
	}

	return key, value, deleteKey, meta, nil
}

// -------------------------------------------------------------------------------------------------

type fenchPointer struct {
	SortKeyMin   []byte
	SortKeyMax   []byte
	DeleteKeyMin []byte
	DeleteKeyMax []byte
}

type sstFileMeta struct {

	// fence pointer
	fenchPointer

	// metadata
	AgeOldestTomb uint32 // the age of oldest tomb in file, Unix seconds
	NumEntry      int    // the number of entries in file
	NumDelete     int    // the number of point delete in file
}

type deleteTileMeta struct {

	// fence pointer
	fenchPointer
}

type pageMeta struct {

	// fence pointer
	fenchPointer
}
