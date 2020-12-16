package lethe

type sstFile struct {
	// file reader
	fd sstFileReader

	// delete tiles
	tiles []*deleteTile

	// fence pointer
	primaryKeyMin []byte
	primaryKeyMax []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte

	// metadata
	aMAX float64
	b    int
}
