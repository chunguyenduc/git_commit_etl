package github

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/pkg/http"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"
)

type RepoClient interface {
	ListCommits(ctx context.Context, request *ListCommitRequest) ([]*CommitResponse, error)
}

type repoClient struct {
	cfg    *config.GitHubRepoConfig
	client *http.Client
}

func NewRepoClient(cfg *config.GitHubRepoConfig) RepoClient {
	client := http.NewClient(cfg.BaseURL,
		http.WithBearerToken(os.Getenv("GITHUB_TOKEN")),
	)
	return &repoClient{
		client: client,
		cfg:    cfg,
	}
}

func (c *repoClient) ListCommits(ctx context.Context, request *ListCommitRequest) ([]*CommitResponse, error) {
	path := fmt.Sprintf("/%s/%s/commits", c.cfg.Owner, c.cfg.Repo)
	respBytes, err := c.client.Get(ctx, path, map[string]string{
		"since":    request.StartDate.Format(time.RFC3339),
		"until":    request.EndDate.Format(time.RFC3339),
		"page":     cast.ToString(request.Page),
		"per_page": "100",
	})
	if err != nil {
		return nil, err
	}

	var resp []*CommitResponse
	if err := jsoniter.Unmarshal(respBytes, &resp); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to unmarshal list")
		return nil, err
	}

	return resp, nil
}
