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
	compactType int // enum value
	levelIndex  int
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
				lsm.doCompatcion(ctx, task)
			}
		case <-ctx.Done():
			{
				log.Println("stop compaction daemon")

				return
			}
		}

	}
}

func (lsm *collection) doCompatcion(ctx context.Context, task compactTask) {
	if task.compactType == enumCompactTypeSO {
		lsm.compactSO(task)
	}
	if task.compactType == enumCompactTypeSD {
		// TOOD
	}
	if task.compactType == enumCompactTypeDD {
		// TODO
	}
	fmt.Println(task)
}

func (lsm *collection) compactSO(task compactTask) {
	// TODO: add LSM lock

	// if the last level needs compact, adds a new level to last
	if task.levelIndex == len(lsm.levels)-1 {
		lsm.addNewLevel()
	}

	curLevel := lsm.levels[task.levelIndex]
	nextLevel := lsm.levels[task.levelIndex+1]

	target, overlaps, isPresent := lsm.findMinOverlap(curLevel, nextLevel)

	mergedFile := lsm.merge(target, ...overlaps)

	lsm.replaceSSTFileOnLevel(curLevel, target, nil)
	lsm.replaceSSTFileOnLevel(nextLevel, overlaps, mergedFile)

	// this is all compactSO...
}
