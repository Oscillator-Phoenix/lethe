package lethe

import (
	"fmt"
	"math/rand"
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

var (
	exampleEntry1 = entry{
		key:       []byte("key"),
		value:     []byte("value"),
		deleteKey: []byte("deleteKey"),
		meta:      keyMeta{1111, 2333},
	}

	exampleEntry2 = entry{
		key:       []byte("key2"),
		value:     []byte("value2"),
		deleteKey: []byte("deleteKey2"),
		meta:      keyMeta{666, 555},
	}

	exampleEntry3 = entry{
		key:       []byte("key3"),
		value:     []byte{},
		deleteKey: []byte("deleteKey3"),
		meta:      keyMeta{666, 999999},
	}

	exampleEntries = []entry{
		exampleEntry1,
		exampleEntry2,
	}
)

func testRandEntries(num int) []entry {
	es := make([]entry, num)
	for i := 0; i < num; i++ {
		es[i] = exampleEntries[rand.Intn(len(exampleEntries))]
	}
	return es
}

func testEntriesEqual(es, es2 []entry) bool {

	if len(es) != len(es2) {
		return false
	}

	for i := 0; i < len(es); i++ {
		if !entryEqual(&es[i], &es2[i]) {
			return false
		}
	}

	return true
}

func TestEncodeEntry(t *testing.T) {

	var (
		e   entry
		e2  entry
		err error
		buf []byte
	)

	e = exampleEntry1

	buf, err = encodeEntry(&e)
	if err != nil {
		t.Fatal()
	}
	if len(buf) != persistFormatLen(&e) {
		t.Fatal()
	}

	e2, err = decodeEntry(buf)
	if err != nil {
		t.Fatal()
	}

	if !entryEqual(&e, &e2) {
		t.Fail()
	}
}

func TestEncodeEntries(t *testing.T) {

	var (
		buf []byte
		es  []entry
		es2 []entry
		err error
	)

	es = testRandEntries(128)

	buf, err = encodeEntries(es)
	if err != nil {
		t.Fatal()
	}

	es2, err = decodeEntries(buf)
	fmt.Println("decoded entries len", len(es2))

	if !testEntriesEqual(es, es2) {
		t.Fail()
	}
}
