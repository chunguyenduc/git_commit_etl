package store

import (
	"context"
	"time"

	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type CommitStore interface {
	InsertBatchCommits(ctx context.Context, commits []*model.Commit) error
	DeleteCommitsByRunDate(ctx context.Context, runDate string) error
	CountCommitsByRunDate(ctx context.Context, runDate string) (int64, error)
}

type commitStore struct {
	db *pgxpool.Pool
}

var _ CommitStore = (*commitStore)(nil)

func NewCommitStore(db *pgxpool.Pool) CommitStore {
	return &commitStore{db: db}
}

const (
	insertCommitStatement           = "INSERT INTO commit_staging(sha, committer_id, committer_username, committer_name, committer_email, commit_ts, pipeline_run_date) VALUES ($1, $2, $3, $4, $5, $6, $7)"
	deleteCommitsByRunDateStatement = "DELETE FROM commit_staging WHERE pipeline_run_date=$1"
	countCommitsByRunDateStatement  = "SELECT COUNT(*) FROM commit_staging WHERE pipeline_run_date=$1"
)

func (s *commitStore) InsertBatchCommits(ctx context.Context, commits []*model.Commit) error {
	batch := &pgx.Batch{}
	for _, commit := range commits {
		batch.Queue(insertCommitStatement,
			commit.SHA, commit.CommiterID, commit.CommiterUserName, commit.CommiterName, commit.CommiterEmail, commit.CommitTS, time.Now().Format(time.DateOnly))
	}
	results := s.db.SendBatch(ctx, batch)
	defer results.Close()

	for range commits {
		_, err := results.Exec()
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("Failed to insert a commit in batch")
			return err
		}
	}

	return nil
}

func (s *commitStore) DeleteCommitsByRunDate(ctx context.Context, runDate string) error {
	_, err := s.db.Exec(ctx, deleteCommitsByRunDateStatement, runDate)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to delete commits")
		return err
	}

	return nil
}

func (s *commitStore) CountCommitsByRunDate(ctx context.Context, runDate string) (int64, error) {
	row := s.db.QueryRow(ctx, countCommitsByRunDateStatement, runDate)

	var count int64
	if err := row.Scan(&count); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to scan row count commits")
		return -1, err
	}

	return count, nil
}
