package model

import (
	"github.com/chunguyenduc/git_commit_etl/internal/adapter/github"
	jsoniter "github.com/json-iterator/go"
	"time"
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
	CommiterID       int       `json:"commiter_id"`
	CommiterUserName string    `json:"commiter_username"`
	CommiterName     string    `json:"commiter_name"`
	CommiterEmail    string    `json:"commiter_email"`
	CommitTS         time.Time `json:"commit_ts"`
}
