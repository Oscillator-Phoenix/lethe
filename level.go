package lethe

type sstFileInterface interface {
}

type deleteTileInterface interface {
}

type pageInterface interface {
}

type level struct {
	files []sstFileInterface
}
