package lethe

import (
	"lethe/bloomfilter"
)

// As the paper 4.2.3 says, lethe maintains Bloom Filters on primay key at the granularity of page.
type page struct {
	bloom bloomfilter.BloomFilter

	primaryKeyMin []byte
	primaryKeyMax []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}

func (p *page) buildBloomFilter() error {
	// TODO
	return nil
}

// bloomFilterExists returns false if the key is exactly not in this page
func (p *page) bloomFilterExists(key []byte) bool {
	// TODO
	return true
}

// loadKVS loads data from disk and returns
func (p *page) loadKVs() (keys, values [][]byte, err error) {
	// TODO
	return nil, nil, nil
}
