package lethe

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path"
	"sync"
)

// sstFileDesc is the interface for SST-file IO.
type sstFileDesc interface {
	Name() string
	io.ReaderAt // ReadAt(p []byte, off int64) (n int, err error)
	io.Writer   // Write(p []byte) (n int, err error)
	// io.WriterAt  // WriteAt(b []byte, off int64) (n int, err error)
	io.Closer // Close() error
}

// -----------------------------------------------------------------------------

// memSSTFileDesc implements sstFileReader interface and sstFileWriter interface.
// memSSTFileDesc is always used as a mock.
type memSSTFileDesc struct {
	buf  bytes.Buffer
	name string
}

// disk
type diskSSTFileDesc os.File

// disk with IO buffer
type diskBufSSTFileDesc struct {
	name string
	file *os.File
	wbuf *bufio.Writer
}

// -----------------------------------------------------------------------------

var _memFds sync.Map

// openMemSSTFileDesc returns a in-memory mock of sstFileDesc
func openMemSSTFileDesc(name string) sstFileDesc {
	fd := &memSSTFileDesc{}

	// A bytes.Buffer needs no initialization.

	fd.name = name

	_memFds.Store(name, fd)

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

	_memFds.Delete(fd.name)

	return nil
}

// Write is an io.Writer interface
// Write writes len(p) bytes from p to the underlying data stream.
// Write appends the contents of p to the buffer, growing the buffer as needed.
func (fd *memSSTFileDesc) Write(p []byte) (n int, err error) {
	return fd.buf.Write(p)
}

// -----------------------------------------------------------------------------

func openDiskSSTFileDesc(dirPath, name string) sstFileDesc {
	// TODO

	fpath := path.Join(dirPath, name)

	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	return f
}

// -----------------------------------------------------------------------------

func openDiskBufSSTFileDesc(dirPath, name string) sstFileDesc {
	fd := &diskBufSSTFileDesc{}

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

func (fd *diskBufSSTFileDesc) Name() string {
	return fd.name
}

// ReadAt is an io.ReaderAt interface.
func (fd *diskBufSSTFileDesc) ReadAt(p []byte, off int64) (n int, err error) {
	return fd.file.ReadAt(p, off)
}

// Write is an io.Writer interface
func (fd *diskBufSSTFileDesc) Write(p []byte) (n int, err error) {
	return fd.wbuf.Write(p) // buffer write
}

// Close is an io.Closer interface
func (fd *diskBufSSTFileDesc) Close() error {

	// sync flush
	if fd.wbuf != nil {
		if err := fd.wbuf.Flush(); err != nil {
			return err
		}
	}

	return fd.file.Close()
}
