package lethe

import (
	"bytes"
	"io"
	"lethe/bloomfilter"
)

type page struct {
	Num    int
	Offset int64
	Size   int64

	SortKeyMin   []byte
	SortKeyMax   []byte
	DeleteKeyMin []byte
	DeleteKeyMax []byte

	// Range Secondary Deletes in a page: in place operation, just shrink size
	// Range Secondary Deletes in a file: full drop, partial drop

	// --------------------------
	// not exported

	// As the paper 4.2.3 says, lethe maintains Bloom Filters on primay key at the granularity of page.
	bloom *bloomfilter.BloomFilter
}

type deleteTile struct {
	SortKeyMin   []byte
	SortKeyMax   []byte
	DeleteKeyMin []byte
	DeleteKeyMax []byte

	Pages []page
}

// Note that os.File implements sstFileDesc interface.
type sstFileDesc interface {
	Name() string
	io.ReaderAt // ReadAt(p []byte, off int64) (n int, err error)
	io.Writer   // Write(p []byte) (n int, err error)
	// io.WriterAt  // WriteAt(b []byte, off int64) (n int, err error)
	io.Closer // Close() error
}

// sstFile is the in-memory format of SST-file.
type sstFile struct {
	Name string

	SortKeyMin   []byte
	SortKeyMax   []byte
	DeleteKeyMin []byte
	DeleteKeyMax []byte

	// the age of oldest tomb in file, Unix seconds
	AgeOldestTomb uint32
	// the number of entries in file
	NumEntry int
	// the number of point delete in file
	NumDelete int

	Tiles []deleteTile

	// --------------------------
	// not exported

	fd sstFileDesc
}

// -----------------------------------------------------------------------------
// encode & decode sstFile
// -----------------------------------------------------------------------------

func encodeSSTFile(file *sstFile) (buf []byte) {
	// TODO
	return nil
}

func decodeSSTFile(buf []byte) (*sstFile, error) {
	// TODO
	return nil, nil
}

// -----------------------------------------------------------------------------
// bloom filter
// -----------------------------------------------------------------------------

func (p *page) buildBloomFilter() error {
	// TODO
	return nil
}

// bloomFilterExists returns false if the key is exactly not in this page
func (p *page) bloomFilterExists(key []byte) bool {
	// TODO
	return true
}

// -----------------------------------------------------------------------------
// load data
// -----------------------------------------------------------------------------

func loadEntries(file *sstFile, p *page) ([]entry, error) {
	buf := make([]byte, p.Size)
	es := make([]entry, p.Num)

	if _, err := file.fd.ReadAt(buf, p.Offset); err != nil {
		return nil, err
	}

	off := 0
	for i := 0; i < p.Num; i++ {
		if err := decodeEntry(buf[off:], &es[i]); err != nil {
			return nil, err
		}
		off += persistFormatLen(&es[i])
	}

	return nil, nil
}

// -----------------------------------------------------------------------------
// get
// -----------------------------------------------------------------------------

func (lsm *collection) getFromSSTFile(file *sstFile, key []byte) (found bool, value []byte, meta keyMeta) {

	// Note that there are no duplicate keys in a SST-File, i.e. all keys in a SST-File are unique.

	less := lsm.options.SortKeyLess

	// sstFile fence pointer check (i.e. SortKeyMin <= key <= SortKeyMax)
	if less(key, file.SortKeyMin) || less(file.SortKeyMax, key) {
		return false, nil, meta
	}

	// get from a page
	pageGet := func(p *page) (found bool, value []byte, meta keyMeta) {

		// page fence pointer check (i.e. SortKeyMin <= key <= SortKeyMax)
		if less(key, p.SortKeyMin) || less(p.SortKeyMax, key) {
			return false, nil, meta
		}

		// check key existence via page-granularity bloom filter
		if p.bloomFilterExists(key) == false {
			return false, nil, meta
		}

		// load data form disk...
		es, _ := loadEntries(file, p)

		// TODO
		// binary search because entries within every page are sorted on sort key
		for i := 0; i < len(es); i++ {
			if bytes.Equal(es[i].key, key) {
				return true, es[i].value, es[i].meta
			}
		}

		// key is not found in this page
		return false, nil, meta
	}

	// get from a delete-tile
	tileGet := func(dt *deleteTile) (found bool, value []byte, meta keyMeta) {

		// delet tile fence pointer check (i.e. SortKeyMin <= key <= SortKeyMax)
		if less(key, dt.SortKeyMin) || less(dt.SortKeyMax, key) {
			return false, nil, meta
		}

		// linear search because pages within a delete-tile are sorted on delete key but not sort key
		for i := 0; i < len(dt.Pages); i++ {

			if found, value, meta = pageGet(&dt.Pages[i]); found {
				return true, value, meta
			}

			// If key is not found and not deleted, keep searching in next pages
		}

		// key is not found in this delete-tile
		return false, nil, meta
	}

	// TODO
	// binary search because delete tiles within a sstfile are sorted on sort key
	for i := 0; i < len(file.Tiles); i++ {

		if found, value, meta = tileGet(&file.Tiles[i]); found {
			return true, value, meta
		}

		// If key is not found and not deleted, keep searching in next delete-tiles
	}

	// key is not found in this SST-file
	return false, nil, meta
}
