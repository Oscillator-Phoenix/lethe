package lethe

type deleteTile struct {
	pages []*page

	SortKeyMin []byte
	SortKeyMax []byte
	deleteKeyMin  []byte
	deleteKeyMax  []byte
}
