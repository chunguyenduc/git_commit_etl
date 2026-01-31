package processor

import (
	"context"
	"time"

	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/processor/extractor"
	"github.com/chunguyenduc/git_commit_etl/internal/processor/loader"
	"github.com/chunguyenduc/git_commit_etl/internal/processor/transformer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type Processor struct {
	cfg         *config.Config
	extractor   *extractor.Extractor
	transformer *transformer.Transformer
	loader      *loader.Loader
}

func New(cfg *config.Config, db *pgxpool.Pool) (*Processor, error) {
	extractorEngine, err := extractor.New(cfg.Extractor)
	if err != nil {
		return nil, err
	}

	transformEngine, err := transformer.New(cfg.Transformer)
	if err != nil {
		return nil, err
	}

	loaderEngine, err := loader.New(db, cfg.Loader)
	if err != nil {
		return nil, err
	}

	return &Processor{
		extractor:   extractorEngine,
		transformer: transformEngine,
		loader:      loaderEngine,
		cfg:         cfg,
	}, nil
}

func (p *Processor) Run(ctx context.Context) error {
	startTime := time.Now()

	fileNames, err := p.extractor.Run(ctx)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Info().Strs("file_names", fileNames).Msg("Saved file name")

	dataChan, err := p.transformer.Transform(ctx, fileNames)
	if err != nil {
		return err
	}

	if err := p.loader.Load(ctx, dataChan); err != nil {
		return err
	}

	log.Ctx(ctx).Info().Msgf("Processed commits in %s", time.Since(startTime).String())
	return nil
}
