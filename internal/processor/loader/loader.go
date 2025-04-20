package loader

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/adapter/postgres"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/database"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/rs/zerolog/log"
	"sync"
)

type Loader struct {
	CommitStore postgres.CommitStore
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
		CommitStore: postgres.NewCommitStore(db),
	}, nil
}

func (l *Loader) Load(ctx context.Context, dataChan chan *model.Commit) error {
	var wg sync.WaitGroup
	count := 0
	wg.Add(1)
	log.Ctx(ctx).Info().Msg("Start loader")

	commits := make([]*model.Commit, 0)

	go func() {
		defer wg.Done()
		for data := range dataChan {
			count++
			commits = append(commits, data)

			if len(commits) > 500 {
				_ = l.CommitStore.UpsertBatchCommits(ctx, commits)
				commits = commits[:0]
			}
		}
	}()

	wg.Wait()
	if len(commits) > 0 {
		_ = l.CommitStore.UpsertBatchCommits(ctx, commits)
	}

	log.Ctx(ctx).Info().Msgf("Load %d commits", count)
	return nil
}
