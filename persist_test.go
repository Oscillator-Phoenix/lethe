package lethe

import (
	"bytes"
	"testing"
)

func TestSplitToPages(t *testing.T) {
	sizedBytes := func(size int) []byte {
		var b bytes.Buffer
		for i := 0; i < size; i++ {
			b.WriteByte(byte(0))
		}
		return b.Bytes()
	}

	_2B := sizedBytes(2)
	_4B := sizedBytes(4)
	_8B := sizedBytes(8)
	_64B := sizedBytes(64)

	e40 := entry{
		// persist format 40B
		key:       _4B,
		value:     _4B,
		deleteKey: _2B,
		meta:      keyMeta{},
	}

	e50 := entry{
		// persist format 50B
		key:       _8B,
		value:     _8B,
		deleteKey: _4B,
		meta:      keyMeta{},
	}

	e100 := entry{
		// persist format 100B
		key:       _64B,
		value:     _4B,
		deleteKey: _2B,
		meta:      keyMeta{},
	}

	es := []entry{
		e40,
		e40,
		e100,
		e100,
		e50,
		e40,
	}

	esPages := splitToPages(es, 100) // 80B
	// page-0: 40, 40, 100
	// page-1: 100,
	// page-2: 50, 40

	for i := 0; i < len(esPages); i++ {
		p := esPages[i]
		t.Logf("page-%d has %d entries\n", i, len(p))
		for j := 0; j < len(p); j++ {
			t.Logf("%d\n", persistFormatLen(&p[j]))
		}
		t.Logf("\n")
	}
}
