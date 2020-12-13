package lethe

// A collection implements the Collection interface.
type collection struct {
	// config
	options *CollectionOptions
	stats   *CollectionStats

	//
	currentMemTable *memTable
}

func newCollection(options *CollectionOptions) *collection {
	c := &collection{}

	c.options = options
	c.currentMemTable = newMemTable(c.options.Less)

	return c
}

func (lsm *collection) Start() error {

	go lsm.persistDaemon()
	go lsm.mergeDaemon()

	return nil
}

// Close synchronously stops background goroutines.
func (lsm *collection) Close() error {
	// TODO
	// some resource recycle
	return nil
}

// Get retrieves a value by iterating over all the segments within
// the collection, if the key is not found a nil val is returned.
func (lsm *collection) Get(key []byte, readOptions *ReadOptions) ([]byte, error) {

	// lookup on memory table
	if v, isPresent := lsm.currentMemTable.Get(key); isPresent {
		return v, nil
	}

	return nil, ErrKeyNotFound
}

// Put creates or updates an key-val entry in the Collection.
func (lsm *collection) Put(key, value []byte, writeOptions *WriteOptions) error {

	if err := lsm.currentMemTable.Put(key, value); err != nil {
		return err
	}

	if lsm.currentMemTable.nBytes() > lsm.options.MemTableBytesLimit {
		// do flush
		// new memtable
	}

	return nil
}

// Del deletes a key-val entry from the Collection.
func (lsm *collection) Del(key []byte, writeOptions *WriteOptions) error {

	if err := lsm.currentMemTable.Del(key); err != nil {
		return err
	}

	return nil
}

// RangeDel deletes key-val entry ranged [lowKey, highKey]
func (lsm *collection) RangeDel(lowKey, highKey []byte, writeOptions *WriteOptions) error {
	return nil
}

// Options returns the current options.
func (lsm *collection) Options() CollectionOptions {
	// TODO
	return *lsm.options
}

// Stats returns stats for this collection.
func (lsm *collection) Stats() (*CollectionStats, error) {
	// TODO
	cs := &CollectionStats{}
	return cs, nil
}
