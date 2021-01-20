package lethe

import (
	"context"
	"errors"
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
				lsm.compact(ctx, task)
			}
		case <-ctx.Done():
			{
				log.Println("stop [compaction daemon]")
				return
			}
		}

	}
}

func (lsm *collection) compact(ctx context.Context, task compactTask) error {
	var err error = nil

	switch task.compactType {
	case enumCompactTypeSO:
		err = lsm.compactSO(task)

	case enumCompactTypeSD:
		err = lsm.compactSD(task)

	case enumCompactTypeDD:
		err = lsm.compactDD(task)

	default:
		err = errors.New("invalid compaction type")
	}

	return err
}

// compactSO uses Saturation-driven trigger and Overlap-driven file selection compaction policy.
func (lsm *collection) compactSO(task compactTask) error {
	// // assert( 0 <= task.levelIndex < len(lsm.levels) )

	// // if the last level needs compaction, adds a new level to last
	// if task.levelIndex == len(lsm.levels)-1 {
	// 	lsm.addNewLevel()
	// }

	// curLevel := lsm.levels[task.levelIndex]
	// nextLevel := lsm.levels[task.levelIndex+1]

	// target, overlaps, isPresent := lsm.findMinOverlap(curLevel, nextLevel)

	// mergedFile := lsm.mergeSSTFile(overlaps..., target)

	// lsm.replaceSSTFileOnLevel(curLevel, target, nil)
	// lsm.replaceSSTFileOnLevel(nextLevel, overlaps, mergedFile)

	// // this is all compactSO...

	return nil
}

// compactSD uses Saturation-driven trigger and Delete-driven file selection compaction policy.
func (lsm *collection) compactSD(task compactTask) error {
	// TODO
	return nil
}

// compactDD uses Delete-driven trigger and Delete-driven file selection compaction policy.
func (lsm *collection) compactDD(task compactTask) error {
	// TODO
	return nil
}
