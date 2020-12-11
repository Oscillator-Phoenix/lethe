package lethe

import (
	"fmt"
)

// A collection implements the Collection interface.
type collection struct {
	// config
	options *CollectionOptions
	stats   *CollectionStats

	//
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
func (lsm *collection) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	// TODO
	value := []byte(fmt.Sprintf("Get value from key %s\n", string(key)))
	return value, nil
}

// Put creates or updates an key-val entry in the Collection.
func (lsm *collection) Put(key, val []byte) error {
	// TODO
	return nil
}

// Del deletes a key-val entry from the Collection.
func (lsm *collection) Del(key []byte) error {
	// TODO
	return nil
}

// SecondaryRangeDelete deletes the range [lowKey, highKey] on the secondary key
func (lsm *collection) SecondaryRangeDelete(lowKey, highKey []byte) error {
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
