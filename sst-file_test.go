package lethe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

var paddingStr string = "xxx"
var paddingBytes []byte = []byte{0x00, 0xFF, 0xFF, 0x00, 0xFF}

var exampleSSTFile *sstFile = &sstFile{
	Name:          "paddingStr",
	SortKeyMin:    paddingBytes,
	SortKeyMax:    paddingBytes,
	DeleteKeyMin:  paddingBytes,
	DeleteKeyMax:  paddingBytes,
	AgeOldestTomb: 123,
	NumEntry:      123,
	NumDelete:     123,
	Tiles: []deleteTile{
		{
			SortKeyMin:   paddingBytes,
			SortKeyMax:   paddingBytes,
			DeleteKeyMin: paddingBytes,
			DeleteKeyMax: paddingBytes,
			Pages:        []page{},
		},
		{
			SortKeyMin:   paddingBytes,
			SortKeyMax:   paddingBytes,
			DeleteKeyMin: paddingBytes,
			DeleteKeyMax: paddingBytes,
			Pages: []page{
				{
					SortKeyMin:   paddingBytes,
					SortKeyMax:   paddingBytes,
					DeleteKeyMin: paddingBytes,
					DeleteKeyMax: paddingBytes,
					Offset:       233,
					Size:         233,
				},
				{
					SortKeyMin:   paddingBytes,
					SortKeyMax:   paddingBytes,
					DeleteKeyMin: paddingBytes,
					DeleteKeyMax: paddingBytes,
					Offset:       233,
					Size:         233,
				},
			},
		},
	},
}

func TestEncodeSSTFile(t *testing.T) {

	var (
		buf            []byte
		err            error
		decodedSSTFile *sstFile
	)

	if buf, err = encodeSSTFile(exampleSSTFile); err != nil {
		t.Log(err)
		t.Fatal()
	}

	if decodedSSTFile, err = decodeSSTFile(buf); err != nil {
		t.Log(err)
		t.Fatal()
	}
	if !reflect.DeepEqual(exampleSSTFile, decodedSSTFile) {
		t.Fatal()
	}

	fmt.Println(decodedSSTFile.SortKeyMax)
	fmt.Println(exampleSSTFile.SortKeyMax)
	if !bytes.Equal(exampleSSTFile.SortKeyMax, decodedSSTFile.SortKeyMax) {
		t.Fatal()
	}

	fmt.Println("encoded json len", len(buf))
	var out bytes.Buffer
	json.Indent(&out, buf, "", "\t")
	fmt.Println(string(out.Bytes()))

}

func TestLoadEntries(t *testing.T) {
	var (
		file *sstFile
		p    *page

		off int64
		es  []entry
		buf []byte
		es2 []entry
		err error
	)

	// file
	file = &sstFile{}
	file.fd = openMemSSTFileDesc("")

	es = testRandEntries(256)
	buf, err = encodeEntries(es)
	if err != nil {
		t.Fatal()
	}

	paddingBuf := make([]byte, off)
	if n, err := file.fd.Write(paddingBuf); err != nil || n != len(paddingBuf) {
		t.Fatal()

	}

	if n, err := file.fd.Write(buf); err != nil || n != len(buf) {
		t.Fatal()
	}

	// page
	p = &page{
		Offset: off,
		Size:   int64(len(buf)),
	}

	es2, err = loadEntries(file, p)
	if err != nil {
		t.Fatal()
	}

	if !testEntriesEqual(es, es2) {
		t.Fail()
	}
}
