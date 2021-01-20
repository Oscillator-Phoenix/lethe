package lethe

import (
	"bytes"
	"fmt"
	"testing"
)

func TestKeyMetaOpType(t *testing.T) {
	fmt.Printf("opBase %0*X\n", 16, opBase)
	fmt.Printf("opPut  %0*X\n", 16, opPut)
	fmt.Printf("opDel  %0*X\n", 16, opDel)
}

func TestEntryLen(t *testing.T) {
	fmt.Printf("sortKeyLenMask   %0*X\n", 16, sortKeyLenMask)
	fmt.Printf("deleteKeyLenMask %0*X\n", 16, deleteKeyLenMask)
	fmt.Printf("valueLenMask     %0*X\n", 16, valueLenMask)

	sortKey := []byte("key")
	deleteKey := []byte("deleteKey")
	value := []byte("value")

	var lenMeta uint64 = 0
	lenMeta |= uint64(len(sortKey)) << sortKeyLenOff
	lenMeta |= uint64(len(deleteKey)) << deleteKeyLenOff
	lenMeta |= uint64(len(value)) << valueLenOff

	sortKeyLen := int((lenMeta & sortKeyLenMask) >> sortKeyLenOff)
	deleteKeyLen := int((lenMeta & deleteKeyLenMask) >> deleteKeyLenOff)
	valueLen := int((lenMeta & valueLenMask) >> valueLenOff)

	fmt.Printf("test lenMeta %0*X\n", 16, lenMeta)
	fmt.Printf("sortKeyLen   %d\n", sortKeyLen)
	fmt.Printf("deleteKeyLen %d\n", deleteKeyLen)
	fmt.Printf("valueLen  %d\n", valueLen)
}

func TestEntrySerialization(t *testing.T) {
	key := []byte("key")
	deleteKey := []byte("deleteKey")
	value := []byte("value")
	meta := keyMeta{1111, 2333}

	buf, _ := encodeEntry(key, value, deleteKey, meta)
	if len(buf) != (3*uint64EncodeLen + len(key) + len(value) + len(deleteKey)) {
		t.Fail()
	}

	_key, _value, _deleteKey, _meta, _ := decodeEntry(buf)

	if !bytes.Equal(key, _key) {
		t.Fail()
	}
	if !bytes.Equal(value, _value) {
		t.Fail()
	}
	if !bytes.Equal(deleteKey, _deleteKey) {
		t.Fail()
	}
	if (meta.seqNum != _meta.seqNum) || (meta.opType != _meta.opType) {
		t.Fail()
	}
}
