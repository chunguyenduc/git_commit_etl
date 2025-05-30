package loader

import (
	"context"
	"database/sql"
	"errors"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/chunguyenduc/git_commit_etl/internal/store"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

const (
	batchSize = 500
)

type Loader struct {
	commitStore store.CommitStore
}

func New(db *sql.DB) (*Loader, error) {
	return &Loader{
		commitStore: store.NewCommitStore(db),
	}, nil
}

func (l *Loader) Load(ctx context.Context, dataChan chan *model.Commit) error {
	pipelineRunDate := time.Now().Format(time.DateOnly)
	if err := l.commitStore.DeleteCommitsByRunDate(ctx, pipelineRunDate); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var count int64
	commits := make([]*model.Commit, 0)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range dataChan {
			commits = append(commits, data)
			count++
			if len(commits) >= batchSize {
				_ = l.commitStore.InsertBatchCommits(ctx, commits)
				commits = commits[:0]
			}
		}
	}()

	wg.Wait()
	if len(commits) > 0 {
		_ = l.commitStore.InsertBatchCommits(ctx, commits)
	}

	countRows, err := l.commitStore.CountCommitsByRunDate(ctx, pipelineRunDate)
	if err != nil {
		return err
	}

	if countRows != count {
		err := errors.New("mismatch loading total row of data")
		log.Ctx(ctx).Error().Err(err).Send()
		return err
	}

	return nil
}
