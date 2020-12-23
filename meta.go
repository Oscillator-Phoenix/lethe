package lethe

const (
	constOpBase uint64 = 0 // 0x0000000000000000

	constPutMask uint64 = 1
	constDelMask uint64 = 2

	constOpPut uint64 = constOpBase & (constPutMask << 56) // 0x0100000000000000
	constOpDel uint64 = constOpBase & (constDelMask << 56) // 0x0200000000000000
)

type keyMeta struct {
	seqNum uint64 // sequence number of operation
	opType uint64 // opType only uses the highest 8 bits
}

type ssTFileMeta struct {
	// TODO
}

type deleteTileMeta struct {
	// TODO
}

type pageMeta struct {
	// TODO
}
