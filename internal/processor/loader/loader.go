package loader

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/database"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/chunguyenduc/git_commit_etl/internal/store"
	"github.com/rs/zerolog/log"
	"sync"
)

const (
	batchSize = 500
)

type Loader struct {
	CommitStore store.CommitStore
}

func New(ctx context.Context, cfg *config.LoaderConfig) (*Loader, error) {
	db, err := database.ConnectPostgresDB(ctx, cfg.DestinationData)
	if err != nil {
		return nil, err
	}

	if err := database.Migrate(ctx, db); err != nil {
		return nil, err
	}

	return &Loader{
		CommitStore: store.NewCommitStore(db),
	}, nil
}

func (l *Loader) Load(ctx context.Context, dataChan chan *model.Commit) error {
	var wg sync.WaitGroup
	count := 0
	commits := make([]*model.Commit, 0)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range dataChan {
			count++
			commits = append(commits, data)

			if len(commits) >= batchSize {
				_ = l.CommitStore.InsertBatchCommits(ctx, commits)
				commits = commits[:0]
			}
		}
	}()

	wg.Wait()
	if len(commits) > 0 {
		_ = l.CommitStore.InsertBatchCommits(ctx, commits)
	}

	log.Ctx(ctx).Info().Msgf("Load %d commits", count)
	return nil
}
