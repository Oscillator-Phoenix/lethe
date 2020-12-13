package lethe

// sstFileMock implements the sstFileInterface interface.
type sstFileMock struct {
	tiles []deleteTileInterface

	primaryKeyMin []byte
	primaryMax    []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}

//  deleteTileMock implements the deleteTileInterface interface.
type deleteTileMock struct {
	pages []pageInterface

	primaryKeyMin []byte
	primaryMax    []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}

// pageMock implements the pageInterface interface.
type pageMock struct {
	keys  [][]byte
	value [][]byte
}
