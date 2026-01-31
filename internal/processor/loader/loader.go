package loader

import (
	"context"
	"errors"
	"time"

	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/chunguyenduc/git_commit_etl/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type Loader struct {
	commitStore store.CommitStore
	cfg         *config.LoaderConfig
}

func New(db *pgxpool.Pool, cfg *config.LoaderConfig) (*Loader, error) {
	return &Loader{
		commitStore: store.NewCommitStore(db),
		cfg:         cfg,
	}, nil
}

func (l *Loader) Load(ctx context.Context, dataChan chan *model.Commit) error {
	pipelineRunDate := time.Now().Format(time.DateOnly)
	if err := l.commitStore.DeleteCommitsByRunDate(ctx, pipelineRunDate); err != nil {
		return err
	}

	var count int64
	commits := make([]*model.Commit, 0, l.cfg.BatchSize)

	for data := range dataChan {
		commits = append(commits, data)
		if len(commits) >= l.cfg.BatchSize {
			if err := l.commitStore.InsertBatchCommits(ctx, commits); err != nil {
				return err
			}
			commits = commits[:0]
		}
		count++
	}

	if len(commits) > 0 {
		if err := l.commitStore.InsertBatchCommits(ctx, commits); err != nil {
			return err
		}
	}

	countRows, err := l.commitStore.CountCommitsByRunDate(ctx, pipelineRunDate)
	if err != nil {
		return err
	}

	if countRows != count {
		err := errors.New("miss match loading total row of data")
		log.Ctx(ctx).Error().Err(err).Send()
		return err
	}

	return nil
}
