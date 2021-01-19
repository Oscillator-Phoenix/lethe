package lethe

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path"
)

type sstFile struct {
	// file reader
	fd sstFileDesc

	// delete tiles
	tiles []*deleteTile

	// fence pointer
	SortKeyMin   []byte
	SortKeyMax   []byte
	deleteKeyMin []byte
	deleteKeyMax []byte

	// metadata
	aMax uint64
	b    int
}

// -----------------------------------------------------------------------------

// Note that os.File implements sstFileDesc interface.
type sstFileDesc interface {
	Name() string
	io.ReaderAt // ReadAt(p []byte, off int64) (n int, err error)
	io.Writer   // Write(p []byte) (n int, err error)
	io.Closer   // Close() error
}

// -----------------------------------------------------------------------------

// memSSTFileDesc implements sstFileReader interface and sstFileWriter interface.
// memSSTFileDesc is a mock.
type memSSTFileDesc struct {
	buf  bytes.Buffer
	name string
}

// newMemSSTFileDesc returns a in-memory mock of sstFileDesc
func newMemSSTFileDesc(name string) sstFileDesc {
	fd := &memSSTFileDesc{}

	// A bytes.Buffer needs no initialization.

	fd.name = name

	return fd
}

// Name returns unique name of fd.
func (fd *memSSTFileDesc) Name() string {
	return fd.name
}

// ReadAt is an io.ReaderAt interface.
// ReadAt reads len(p) bytes into p starting at offset off in the underlying input source.
func (fd *memSSTFileDesc) ReadAt(p []byte, off int64) (n int, err error) {
	tmpBuf := bytes.NewBuffer(fd.buf.Bytes()[off:])
	return tmpBuf.Read(p)
}

// Close is an io.Closer interface
func (fd *memSSTFileDesc) Close() error {
	// do nothing in mock
	return nil
}

// Write is an io.Writer interface
// Write writes len(p) bytes from p to the underlying data stream.
// Write appends the contents of p to the buffer, growing the buffer as needed.
func (fd *memSSTFileDesc) Write(p []byte) (n int, err error) {
	return fd.buf.Write(p)
}

// -----------------------------------------------------------------------------

type diskSSTFileDesc struct {
	name string
	file *os.File
	wbuf *bufio.Writer
}

func newDiskSSTFileDesc(dirPath, name string) sstFileDesc {
	fd := &diskSSTFileDesc{}

	fd.name = name

	fpath := path.Join(dirPath, name)
	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	fd.file = f

	fd.wbuf = bufio.NewWriterSize(f, 1<<20) // 1MB write buffer

	return fd
}

func (fd *diskSSTFileDesc) Name() string {
	return fd.name
}

// ReadAt is an io.ReaderAt interface.
func (fd *diskSSTFileDesc) ReadAt(p []byte, off int64) (n int, err error) {
	return fd.file.ReadAt(p, off)
}

// Write is an io.Writer interface
func (fd *diskSSTFileDesc) Write(p []byte) (n int, err error) {
	return fd.wbuf.Write(p) // buffer write
}

// Close is an io.Closer interface
func (fd *diskSSTFileDesc) Close() error {

	// sync flush
	if fd.wbuf != nil {
		if err := fd.wbuf.Flush(); err != nil {
			return err
		}
	}

	return fd.file.Close()
}

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
		ks, vs, metas := p.load(file.fd)

		// TODO
		// binary search because entries within every page are sorted on sort key
		for i := 0; i < len(ks); i++ {
			if bytes.Equal(ks[i], key) {
				return true, vs[i], metas[i]
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

		// linear search because pages within a delete tile are sorted on delete key but not sort key
		for i := 0; i < len(dt.pages); i++ {

			if found, value, meta = pageGet(dt.pages[i]); found {
				return true, value, meta
			}

			// If key is not found and not deleted, keep searching in next pages
		}

		// key is not found in this delete-tile
		return false, nil, meta
	}

	// TODO
	// binary search because delete tiles within a sstfile are sorted on sort key
	for i := 0; i < len(file.tiles); i++ {

		if found, value, meta = tileGet(file.tiles[i]); found {
			return true, value, meta
		}

		// If key is not found and not deleted, keep searching in next delete-tiles
	}

	// key is not found in this SST-file
	return false, nil, meta
}
