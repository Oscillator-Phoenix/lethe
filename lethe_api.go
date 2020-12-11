package lethe

import "errors"

var (
	// ErrKeyTooLarge is returned when the length of the key exceeds the limit of ???.
	ErrKeyTooLarge = errors.New("key-too-large")

	// ErrValueTooLarge is returned when the length of the value exceeds the limit of ???.
	ErrValueTooLarge = errors.New("value-too-large")

	// ErrClosed is returned when the collection is already closed.
	ErrClosed = errors.New("closed")

	// TODO
	// define other errors
)

// NewCollection returns a new, unstarted Collection instance.
func NewCollection(options CollectionOptions) (Collection, error) {
	// TODO
	return nil, nil
}

// CollectionOptions allows applications to specify config settings.
type CollectionOptions struct {
	// TODO

	// IsPersist shows if the Collection needs persist.
	IsPersist bool

	// FilePath is the file path of the Collection.
	FilePath string

	// CreateIfMissing create a new Collection if filePath is not existed.
	CreateIfMissing bool
}

// DefaultCollectionOptions are the default configuration options.
var DefaultCollectionOptions = CollectionOptions{
	IsPersist:       false,
	FilePath:        "",
	CreateIfMissing: false,
}

// A Collection represents an ordered mapping of key-val entries,
// where a Collection is snapshot'able and atomically updatable.
type Collection interface {
	// Start kicks off required background tasks.
	Start() error

	// Close synchronously stops background tasks and releases
	// resources.
	Close() error

	// Options returns the options currently being used.
	Options() CollectionOptions

	// Snapshot returns a stable Snapshot of the key-value entries.
	Snapshot() (Snapshot, error)

	// Get retrieves a value from the collection for a given key
	// and returns nil if the key is not found.
	Get(key []byte, readOptions ReadOptions) ([]byte, error)

	// NewBatch returns a new Batch instance.
	NewBatch() (Batch, error)

	// ExecuteBatch atomically incorporates the provided Batch into
	// the Collection.  The Batch instance should be Close()'ed and
	// not reused after ExecuteBatch() returns.
	ExecuteBatch(b Batch, writeOptions WriteOptions) error

	// Stats returns stats for this collection.  Note that stats might
	// be updated asynchronously.
	Stats() (*CollectionStats, error)
}

// ReadOptions are provided to Snapshot.Get().
type ReadOptions struct {
	// TODO
	// define some read options if necessary
}

// WriteOptions are provided to Collection.ExecuteBatch().
type WriteOptions struct {
	// TOOD
	// define some write options if necessary
}

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

	// Set creates or updates an key-val entry in the Collection.  The
	// key must be unique (not repeated) within the Batch.  Set()
	// copies the key and val bytes into the Batch, so the memory
	// bytes of the key and val may be reused by the caller.
	Set(key, val []byte) error

	// Del deletes a key-val entry from the Collection.  The key must
	// be unique (not repeated) within the Batch.  Del copies the key
	// bytes into the Batch, so the memory bytes of the key may be
	// reused by the caller.  Del() on a non-existent key results in a
	// nil error.
	Del(key []byte) error

	// TODO
	// seconday range delete [lowKey, highKey]
	// SecondaryRangeDelete(lowKey, highKey []byte) error
}

// CollectionStats shows a status of collection.
// CollectionStats fields that are prefixed like CurXxxx are gauges
// (can go up and down), and fields that are prefixed like TotXxxx are
// monotonically increasing counters.
type CollectionStats struct {
	// TODO
	// Tot

	// TODO
	// Cur
}
