package postgres

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
	UpsertBatchCommits(ctx context.Context, commits []*model.Commit) error
}

type commitStore struct {
	db *sql.DB
}

var _ CommitStore = (*commitStore)(nil)

func NewCommitStore(db *sql.DB) CommitStore {
	return &commitStore{db: db}
}

func (s *commitStore) UpsertBatchCommits(ctx context.Context, commits []*model.Commit) error {
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
		valueArgs = append(valueArgs, time.Now())
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
