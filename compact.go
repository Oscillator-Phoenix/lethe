package lethe

import (
	"context"
	"fmt"
	"log"
)

const (
	enumCompactTypeSO = 1
	enumCompactTypeSD = 2
	enumCompactTypeDD = 3
)

type compactTask struct {
	compactType int
	levelID     int
	sstFileID   int
}

func (task compactTask) String() string {
	// TOOD
	return "todo"
}

func (lsm *collection) compactDaemon(ctx context.Context) {
	for {

		select {
		case task := <-lsm.compactTrigger:
			{
				// TODO
				doCompatcion(ctx, task)
			}
		case <-ctx.Done():
			{
				log.Println("stop compaction daemon")

				return
			}
		}

	}
}

func doCompatcion(ctx context.Context, task compactTask) {
	if task.compactType == enumCompactTypeSO {
		// TODO
	}
	if task.compactType == enumCompactTypeSD {
		// TODO
	}
	if task.compactType == enumCompactTypeDD {
		// TODO
	}
	fmt.Println(task)
}
