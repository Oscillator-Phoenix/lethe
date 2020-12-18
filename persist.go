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
		select {
		case task := <-lsm.persistTrigger:
			{
				lsm.doPersist(task)
			}
		case <-ctx.Done():
			{
				log.Println("stop persist daemon")
				return
			}
		}
	}
}

func (lsm *collection) doPersist(task persistTask) {

}
