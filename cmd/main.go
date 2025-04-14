package main

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/logger"
	"github.com/chunguyenduc/git_commit_etl/internal/processor"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	initLogger := logger.InitLogger()
	ctx := initLogger.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		panic(err)
	}

	process := processor.New(cfg)
	go func() {
		if err := process.Run(ctx); err != nil {
			log.Err(err).Msg("process error")
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Info().Msg("Shutting down")
	cancel()
	time.Sleep(2 * time.Second)
}
