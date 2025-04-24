package transformer

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/chunguyenduc/git_commit_etl/internal/utils"
	"github.com/chunguyenduc/git_commit_etl/pkg/file"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
)

type Transformer struct {
	fileReader *file.Reader
}

func New(cfg *config.TransformerConfig) (*Transformer, error) {
	return &Transformer{
		fileReader: file.NewFileReader(cfg.StorageDir),
	}, nil
}

func (t *Transformer) Transform(ctx context.Context, fileNames []string) (chan *model.Commit, error) {
	dataChanFunc := func(ctx context.Context, fileName string) (chan *model.Commit, error) {
		result := make(chan *model.Commit, 500)
		data, err := t.fileReader.ReadFile(ctx, fileName)
		if err != nil {
			return nil, err
		}

		var rawData *model.ExtractedData
		if err := jsoniter.Unmarshal(data, &rawData); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("Failed to unmarshal raw data")
			return nil, err
		}

		go func() {
			defer close(result)
			for _, commit := range rawData.Commits {
				if commit.Author.IsEmpty() {
					continue
				}

				transformedData := &model.Commit{
					SHA:              commit.Sha,
					CommiterID:       commit.Author.ID,
					CommiterUserName: commit.Author.Login,
					CommiterName:     commit.Commit.Author.Name,
					CommiterEmail:    commit.Commit.Author.Email,
					CommitTS:         commit.Commit.Author.Date,
				}

				select {
				case <-ctx.Done():
					return
				case result <- transformedData:
				}
			}
		}()

		return result, nil
	}

	dataChans := make([]chan *model.Commit, 0, len(fileNames))
	for _, fileName := range fileNames {
		dataFunc, err := dataChanFunc(ctx, fileName)
		if err != nil {
			return nil, err
		}

		dataChans = append(dataChans, dataFunc)
	}

	return utils.FanIn[*model.Commit](dataChans...), nil
}
