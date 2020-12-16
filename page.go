package lethe

import (
	"lethe/bloomfilter"
)

const (
	constNumByteOfPage = 4 * 1024 // 4 *KB
)

// As the paper 4.2.3 says, lethe maintains Bloom Filters on primay key at the granularity of page.
type page struct {
	bloom  *bloomfilter.BloomFilter
	offset int64

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

// loadKVS loads data from a reader using the offset of page
func (p *page) loadKVs(r sstFileReader) (keys, values [][]byte, err error) {

	pageData := make([]byte, constNumByteOfPage)

	if _, err := r.ReadAt(pageData, p.offset); err != nil {
		return nil, nil, err
	}

	// TODO
	// build keys, values from pageData

	return nil, nil, nil
}
