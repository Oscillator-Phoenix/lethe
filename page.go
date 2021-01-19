package lethe

import (
	"lethe/bloomfilter"
	"log"
)

const (
	constNumByteOfPage = 4 * 1024 // 4 *KB
)

// As the paper 4.2.3 says, lethe maintains Bloom Filters on primay key at the granularity of page.
type page struct {
	bloom  *bloomfilter.BloomFilter
	offset int64

	SortKeyMin []byte
	SortKeyMax []byte
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

// loadKVS loads data from a sstFileDesc using the offset of page
func (p *page) load(desc sstFileDesc) (ks, vs [][]byte, metas []keyMeta) {
	pageData := make([]byte, constNumByteOfPage)

	if _, err := desc.ReadAt(pageData, p.offset); err != nil {
		log.Printf("[err] page load %v", err)
		return nil, nil, nil
	}

	// TODO
	// build ks, vs, metas from pageData

	return nil, nil, nil
}
