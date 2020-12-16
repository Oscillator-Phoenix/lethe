package lethe

import (
	"bytes"
	"io"
)

// Note that os.File implements sstFileReader interface.
type sstFileReader interface {
	io.ReaderAt
	io.Closer
}

// Note that os.File implements sstFileWriter interface.
type sstFileWriter interface {
	io.Writer
}

// sstFileDescMock implements sstFileReader interface and sstFileWriter interface.
// Mock storage in memory.
type sstFileDescMock struct {
	buf *bytes.Buffer
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
