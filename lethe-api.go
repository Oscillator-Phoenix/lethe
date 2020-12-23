package lethe

import (
	"errors"
	"time"
)

var (
	// ErrPlaceholder is used when you need to return an error that is undefined.
	ErrPlaceholder = errors.New("error-placeholder")

	// ErrKeyNotFound is returned when the key is not found.
	ErrKeyNotFound = errors.New("key-not-found")

	// ErrPrimaryKeyTooLarge is returned when the length of the primary key exceeds the limit.
	ErrPrimaryKeyTooLarge = errors.New("primary-key-too-large")

	// ErrDeleteKeyTooLarge is returned when the length of the delete key exceeds the limit.
	ErrDeleteKeyTooLarge = errors.New("delete-key-too-large")

	// ErrValueTooLarge is returned when the length of the value exceeds the limit of ???.
	ErrValueTooLarge = errors.New("value-too-large")

	// ErrClosed is returned when the collection is already closed.
	ErrClosed = errors.New("closed")

	// TODO
	// define other errors
)

// CollectionOptions allows applications to specify config settings.
type CollectionOptions struct {

	// PrimaryKeyLess defines the order of primary key
	PrimaryKeyLess func(s, t []byte) bool

	// DeleteKeyLess defines the order of delete key
	DeleteKeyLess func(s, t []byte) bool

	// MemTableBytesLimit the number of bytes of MemTable/Buffer
	MemTableBytesLimit int

	// LevelSizeRatio is a factor that the capacity of Level_(i) is greater than that of Level_(i−1).
	LevelSizeRatio int

	// persist
	// Path is the file path of the Collection directory.
	DirPath string

	// CreateIfMissing create a new Collection if filePath is not existed.
	CreateDirIfMissing bool

	// DeletePersistThreshold, all tombstones are persisted within a delete persistence threshold.
	// DeletePersistThreshold is denoted by D_th in paper 4.1 .
	DeletePersistThreshold time.Duration

	// InitialLevelNum is the initial number of level.
	// A LSM with L levels has one memTable(`Level 0`) and L-1 perist levels(`Level 1` to `Level L-1` ).
	InitialLevelNum int

	// ----------------------------------------------------------------------------

	// Unexposed data filed
	// buffer length of chan which is persistence trigger
	persistTriggerBufLen int
	// buffer length of chan which is comapction trigger
	compactTriggerBufLen int
}

// DefaultCollectionOptions are the default configuration options.
var DefaultCollectionOptions = CollectionOptions{
	PrimaryKeyLess:         func(s, t []byte) bool { return string(s) < string(t) }, //
	DeleteKeyLess:          func(s, t []byte) bool { return string(s) < string(t) }, //
	MemTableBytesLimit:     64 * 1024,                                               // 64KB
	LevelSizeRatio:         10.0,                                                    //
	DirPath:                "",                                                      //
	CreateDirIfMissing:     false,                                                   //
	DeletePersistThreshold: 24 * time.Hour,                                          // one day
	InitialLevelNum:        6,                                                       //

	// -------------------------------------------

	persistTriggerBufLen: 5, //
	compactTriggerBufLen: 5, //
}

// CollectionStats shows a status of collection.
type CollectionStats struct {
	// TODO
	// TotXXX
	// CurXXX
}

// ReadOptions are provided to Snapshot.Get().
type ReadOptions struct {
	// define some read options if necessary
}

// WriteOptions are provided to Collection.ExecuteBatch().
type WriteOptions struct {
	// define some write options if necessary
}

// A Collection represents an ordered mapping of key-val entries,
// where a Collection is snapshot'able and atomically updatable.
type Collection interface {

	// Close synchronously stops background tasks and releases
	// resources.
	Close() error

	// Get retrieves a value from the collection for a given key
	// and returns nil if the key is not found.
	Get(key []byte, readOptions *ReadOptions) ([]byte, error)

	// Put creates or updates an key-val entry in the Collection.
	Put(key, value, dKey []byte, writeOptions *WriteOptions) error

	// Del deletes a key-val entry from the Collection.
	Del(key []byte, writeOptions *WriteOptions) error

	// RangeDel deletes the range [lowKey, highKey] on the secondary key
	RangeDel(lowKey, highKey []byte, writeOptions *WriteOptions) error

	// Options returns the options currently being used.
	Options() CollectionOptions

	// Stats returns stats for this collection.  Note that stats might
	// be updated asynchronously.
	Stats() (*CollectionStats, error)

	/*
		// TODO
		// advanced feature below:

		// Snapshot returns a stable ready-only Snapshot of the key-value entries.
		Snapshot() (Snapshot, error)

		// WriteBatch returns a new WriteBatch instance.
		WriteBatch() (Batch, error)

		// ExecuteWriteBatch atomically incorporates the provided Batch into
		// the Collection.  The Batch instance should be Close()'ed and
		// not reused after ExecuteBatch() returns.
		ExecuteWriteBatch(b Batch, writeOptions WriteOptions) error
	*/
}

/*
// A Snapshot is a stable view of a Collection for readers, isolated
// from concurrent mutation activity.
type Snapshot interface {
	// Close must be invoked to release resources.
	Close() error

	// Get retrieves a val from the Snapshot, and will return nil val
	// if the entry does not exist in the Snapshot.
	Get(key []byte, readOptions ReadOptions) ([]byte, error)
}

// A Batch is a set of mutations that will be incorporated atomically
// into a Collection.  NOTE: the keys in a Batch must be unique.
type Batch interface {
	// Close must be invoked to release resources.
	Close() error

	// Put creates or updates an key-val entry in the Collection.  The
	// key must be unique (not repeated) within the Batch.  Put()
	// copies the key and val bytes into the Batch, so the memory
	// bytes of the key and val may be reused by the caller.
	Put(key, val []byte) error

	// Del deletes a key-val entry from the Collection.  The key must
	// be unique (not repeated) within the Batch.  Del copies the key
	// bytes into the Batch, so the memory bytes of the key may be
	// reused by the caller.  Del() on a non-existent key results in a
	// nil error.
	Del(key []byte) error
}
*/

// ---------------------------------------------------------------------

// NewCollection returns a new, unstarted Collection instance.
func NewCollection(options CollectionOptions) (Collection, error) {

	// init collection
	c := newCollection(&options)

	return c, nil
}
