package model

import (
	"time"

	"github.com/chunguyenduc/git_commit_etl/internal/adapter/github"
	jsoniter "github.com/json-iterator/go"
)

type ExtractedData struct {
	Commits []*github.CommitResponse `json:"commits"`
	Month   int                      `json:"month"`
	Year    int                      `json:"year"`
}

func (d *ExtractedData) Serialize() ([]byte, error) {
	return jsoniter.Marshal(d)
}

type Commit struct {
	SHA              string    `json:"sha"`
	CommiterID       int       `json:"committer_id"`
	CommiterUserName string    `json:"committer_username"`
	CommiterName     string    `json:"committer_name"`
	CommiterEmail    string    `json:"committer_email"`
	CommitTS         time.Time `json:"commit_ts"`
}
