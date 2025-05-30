package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
)

type CommitStore interface {
	InsertBatchCommits(ctx context.Context, commits []*model.Commit) error
	DeleteCommitsByRunDate(ctx context.Context, runDate string) error
	CountCommitsByRunDate(ctx context.Context, runDate string) (int64, error)
}

type commitStore struct {
	db *sql.DB
}

var _ CommitStore = (*commitStore)(nil)

func NewCommitStore(db *sql.DB) CommitStore {
	return &commitStore{db: db}
}

func (s *commitStore) InsertBatchCommits(ctx context.Context, commits []*model.Commit) error {
	valueStrings := make([]string, 0, len(commits))
	valueArgs := make([]interface{}, 0, len(commits)*6)
	for i, commit := range commits {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7))
		valueArgs = append(valueArgs, commit.SHA)
		valueArgs = append(valueArgs, commit.CommiterID)
		valueArgs = append(valueArgs, commit.CommiterUserName)
		valueArgs = append(valueArgs, commit.CommiterName)
		valueArgs = append(valueArgs, commit.CommiterEmail)
		valueArgs = append(valueArgs, commit.CommitTS)
		valueArgs = append(valueArgs, time.Now().Format(time.DateOnly))
	}

	statement := fmt.Sprintf("INSERT INTO commit_staging("+
		"sha, "+
		"committer_id, "+
		"committer_username, "+
		"committer_name, "+
		"committer_email, "+
		"commit_ts, "+
		"pipeline_run_date) VALUES %s", strings.Join(valueStrings, ","))
	_, err := s.db.ExecContext(ctx, statement, valueArgs...)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to insert commits")
		return err
	}

	return nil
}

func (s *commitStore) DeleteCommitsByRunDate(ctx context.Context, runDate string) error {
	statement := "DELETE FROM commit_staging WHERE pipeline_run_date=$1"
	_, err := s.db.ExecContext(ctx, statement, runDate)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to delete commits")
		return err
	}

	return nil
}

func (s *commitStore) CountCommitsByRunDate(ctx context.Context, runDate string) (int64, error) {
	statement := "SELECT COUNT(*) FROM commit_staging WHERE pipeline_run_date=$1"
	row := s.db.QueryRowContext(ctx, statement, runDate)
	if err := row.Err(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to count commits")
		return -1, err
	}

	var count int64
	if err := row.Scan(&count); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to scan row count commits")
		return -1, err
	}

	return count, nil
}
