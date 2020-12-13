package lethe

import (
	"context"
	"fmt"
)

type compactionTask struct {
	levelID   int
	sstFileID int
}

func (lsm *collection) compactDaemon(ctx context.Context) {
	for {
		var task compactionTask

		select {
		case task = <-lsm.soCompactionTrigger:
			{

			}
		case task = <-lsm.sdCompactionTrigger:
			{

			}
		case task = <-lsm.ddCompactionTrigger:
			{

			}
		case <-ctx.Done():
			{
				// stop compaction daemon
				return
			}
		}

		doCompatcion(ctx, task)
	}
}

func doCompatcion(ctx context.Context, task compactionTask) {
	fmt.Println(task)
}
