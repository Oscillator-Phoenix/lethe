package lethe

import (
	"context"
	"log"
)

type persistTask struct {
	mt *memTable
}

func (lsm *collection) persistDaemon(ctx context.Context) {
	for {
		var task persistTask

		select {
		case task = <-lsm.persistTrigger:
			{

			}
		case <-ctx.Done():
			{
				log.Println("stop compaction daemon")
				return
			}
		}

		doPersist(ctx, task)

	}
}

func doPersist(ctx context.Context, task persistTask) {

}
