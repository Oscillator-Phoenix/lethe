package lethe

const (
	opBase uint64 = 0 // 0x0000000000000000

	putMask uint64 = 1
	delMask uint64 = 2

	opPut uint64 = opBase | (putMask << 56) // 0x0100000000000000
	opDel uint64 = opBase | (delMask << 56) // 0x0200000000000000, tombstone
)

const (
	constKeyMetaBytesLen int = 16
)

type keyMeta struct {
	seqNum uint64 // sequence number of operation
	opType uint64 // now, opType only uses the highest 8 bits
}

type sstFileMeta struct {
	// TODO
}

type deleteTileMeta struct {
	// TODO
}

type pageMeta struct {
	// TODO
}
