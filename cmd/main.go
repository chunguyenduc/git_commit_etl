package main

import (
	"context"

	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/database"
	"github.com/chunguyenduc/git_commit_etl/internal/logger"
	"github.com/chunguyenduc/git_commit_etl/internal/processor"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

func main() {
	initLogger := logger.InitLogger()
	ctx := initLogger.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("Failed to load config")
	}

	db, err := database.ConnectPostgresDB(ctx, cfg.Loader.DestinationData)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("Failed to connect to postgres db")
	}
	defer func(db *pgxpool.Pool) {
		db.Close()
		log.Ctx(ctx).Info().Msg("Close database successfully")
	}(db)

	if err := database.Migrate(ctx, db); err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("Failed to migrate database")
	}

	process, err := processor.New(cfg, db)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("Failed to create processor")
	}

	if err := process.Run(ctx); err != nil {
		log.Err(err).Msg("process error")
		return
	}
}
