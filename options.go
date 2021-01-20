package lethe

import (
	"fmt"
	"strings"
)

func beautifulNumByte(nbyte int) string {
	const B = 1
	const KB = B << 10
	const MB = KB << 10
	const GB = MB << 10
	const TB = GB << 10

	if nbyte < 0 {
		return ""
	}

	if nbyte == 0 {
		return "0 B"
	}

	var helper func(n int) string

	helper = func(n int) string {
		switch {
		case n == 0:
			return ""
		case n >= TB:
			return fmt.Sprintf("%dTB", n/TB) + helper(n%TB)
		case n >= GB:
			return fmt.Sprintf("%dGB", n/GB) + helper(n%GB)
		case n >= MB:
			return fmt.Sprintf("%dMB", n/MB) + helper(n%MB)
		case n >= KB:
			return fmt.Sprintf("%dKB", n/KB) + helper(n%KB)
		default:
			return fmt.Sprintf("%dB", n)
		}
	}

	return helper(nbyte)
}

func (op CollectionOptions) String() string {
	// TODO: pretty message print
	var b strings.Builder

	b.WriteString("options of collection:\n")
	b.WriteString(fmt.Sprintf("[initial levels number] %d\n", op.NumInitialLevel))
	b.WriteString(fmt.Sprintf("[levels size ratio] %d\n", op.LevelSizeRatio))
	b.WriteString(fmt.Sprintf("[delete persistence threshold] %v\n", op.DeletePersistThreshold))
	b.WriteString(fmt.Sprintf("[in-memory table capacity limit] %v\n", beautifulNumByte(op.MemTableSizeLimit)))

	return b.String()
}
