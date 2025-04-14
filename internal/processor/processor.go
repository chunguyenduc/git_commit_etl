package processor

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/processor/extractor"
	"github.com/chunguyenduc/git_commit_etl/internal/sensor"
	"github.com/rs/zerolog/log"
	"time"
)

type Processor struct {
	cfg       *config.Config
	extractor *extractor.Extractor
}

func New(cfg *config.Config) *Processor {
	return &Processor{
		extractor: extractor.New(cfg.SourceData),
		cfg:       cfg,
	}
}

func (e *Processor) Run(ctx context.Context) error {
	startTime := time.Now()

	go func(ctx context.Context) {
		fs, err := sensor.NewFileSensor([]string{e.cfg.SourceData.StorageDir})
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to create sensor")
			return
		}
		if err := fs.Listen(ctx); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to listen")
			return
		}
	}(ctx)

	if err := e.extractor.Run(ctx); err != nil {
		return err
	}

	log.Ctx(ctx).Info().Msgf("Processed commits in %s", time.Since(startTime).String())
	return nil
}
