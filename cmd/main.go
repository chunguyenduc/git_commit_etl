package main

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/logger"
	"github.com/chunguyenduc/git_commit_etl/internal/processor"
	"github.com/rs/zerolog/log"
)

func main() {
	initLogger := logger.InitLogger()
	ctx := initLogger.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		panic(err)
	}

	process, err := processor.New(ctx, cfg)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("Failed to create processor")
	}

	if err := process.Run(ctx); err != nil {
		log.Err(err).Msg("process error")
		return
	}
}
