package lethe

import (
	"bufio"
	"bytes"
	"os"
	"path"
)

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
