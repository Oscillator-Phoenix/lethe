package lethe

import (
	"bytes"
	"io"
)

type sstFile struct {
	// file reader
	fd sstFileDesc

	// delete tiles
	tiles []*deleteTile

	// fence pointer
	primaryKeyMin []byte
	primaryKeyMax []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte

	// metadata
	aMAX float64
	b    int
}

// Note that os.File implements sstFileDesc interface.
type sstFileDesc interface {
	Name() string
	io.ReaderAt // ReadAt(p []byte, off int64) (n int, err error)
	io.Writer   // Write(p []byte) (n int, err error)
	io.Closer   // Close() error
}

// -----------------------------------------------------------------------------

// sstFileDescMock implements sstFileReader interface and sstFileWriter interface.
// Mock storage in memory.
type sstFileDescMock struct {
	buf  bytes.Buffer
	name string
}

// newSSTFileDescMock returns a in-memory mock of sstFileDesc
func newSSTFileDescMock(name string) sstFileDesc {
	mock := &sstFileDescMock{}
	// A bytes.Buffer needs no initialization.
	mock.name = name

	return mock
}

func (fd *sstFileDescMock) Name() string {
	return fd.name
}

// ReadAt is an io.ReaderAt interface.
// ReadAt reads len(p) bytes into p starting at offset off in the underlying input source.
func (fd *sstFileDescMock) ReadAt(p []byte, off int64) (n int, err error) {
	tmpBuf := bytes.NewBuffer(fd.buf.Bytes()[off:])
	return tmpBuf.Read(p)
}

// Close is an io.Closer interface
func (fd *sstFileDescMock) Close() error {
	// do nothing in mock
	return nil
}

// Write is an io.Writer interface
// Write writes len(p) bytes from p to the underlying data stream.
// Write appends the contents of p to the buffer, growing the buffer as needed.
func (fd *sstFileDescMock) Write(p []byte) (n int, err error) {
	return fd.buf.Write(p)
}

// -----------------------------------------------------------------------------

func (lsm *collection) getFromSSTFile(file *sstFile, key []byte) (value []byte, found, deleted bool) {

	// Note that there are no duplicate keys in SST-File, i.e. all keys in a SST-File are unique.

	less := lsm.options.PrimaryKeyLess

	// sstFile fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
	if less(key, file.primaryKeyMin) || less(file.primaryKeyMax, key) {
		return nil, false, false
	}

	// get from a page
	pageGet := func(p *page) (value []byte, found, deleted bool) {

		// page fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
		if less(key, p.primaryKeyMin) || less(p.primaryKeyMax, key) {
			return nil, false, false
		}

		// page-granularity bloom filter existence check
		if p.bloomFilterExists(key) == false {
			return nil, false, false
		}

		// load data form disk...
		ks, vs, metas := p.load(file.fd)

		// TODO
		// binary search because entries within every page are sorted on primary key
		for i := 0; i < len(ks); i++ {
			if bytes.Equal(ks[i], key) {
				if metas[i].opType == constOpDel { // If there is a tombstone
					return nil, false, true
				}
				return vs[i], true, false
			}
		}

		return nil, false, false
	}

	// get from a delet tile
	deleteTileGet := func(dt *deleteTile) (value []byte, found, deleted bool) {

		// delet tile fence pointer check (i.e. primaryKeyMin <= key <= primaryKeyMax)
		if less(key, dt.primaryKeyMin) || less(dt.primaryKeyMax, key) {
			return nil, false, false
		}

		// linear search because pages within a delete tile are sorted on delete key but not primary key
		for i := 0; i < len(dt.pages); i++ {
			value, found, deleted := pageGet(dt.pages[i])
			if found {
				return value, true, false
			}
			if deleted {
				return nil, false, true
			}
			// If key is not found and not deleted, keep searching in next pages
		}

		return nil, false, false
	}

	// TODO
	// binary search because delete tiles within a sstfile are sorted on primary key
	for i := 0; i < len(file.tiles); i++ {
		value, found, deleted := deleteTileGet(file.tiles[i])
		if found {
			return value, true, false
		}
		if deleted {
			return nil, false, true
		}
		// If key is not found and not deleted, keep searching in next delete-tiles
	}

	return nil, false, false
}
