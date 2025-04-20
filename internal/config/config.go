package config

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/database"
	"github.com/chunguyenduc/git_commit_etl/internal/validator"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"path"
)

type Config struct {
	Extractor   *ExtractorConfig   `mapstructure:"extractor" validate:"required"`
	Transformer *TransformerConfig `mapstructure:"transformer" validate:"required"`
	Loader      *LoaderConfig      `mapstructure:"loader" validate:"required"`
}

type (
	GitHubRepoConfig struct {
		BaseURL string `mapstructure:"base_url" validate:"required"`
		Owner   string `mapstructure:"owner" validate:"required"`
		Repo    string `mapstructure:"repo" validate:"required"`
	}
	ExtractorConfig struct {
		SourceData  *GitHubRepoConfig `mapstructure:"source_data" validate:"required"`
		MonthCounts int               `mapstructure:"month_counts" validate:"required"`
		StorageDir  string            `mapstructure:"storage_dir" validate:"required"`
	}

	TransformerConfig struct {
		StorageDir string `mapstructure:"storage_dir" validate:"required"`
	}

	LoaderConfig struct {
		DestinationData *database.PostgresConfig `mapstructure:"destination_data" validate:"required"`
	}
)

const (
	configDir = "conf"
)

func LoadConfig(ctx context.Context) (*Config, error) {
	viper.SetConfigFile(path.Join(configDir, "config.yaml"))
	err := viper.ReadInConfig()
	if err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to read config")
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to unmarshal config")
		return nil, err
	}

	if err := validator.Struct(cfg); err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to validate config")
		return nil, err
	}

	log.Ctx(ctx).Info().Interface("config", cfg).Msg("Loaded config")
	return &cfg, nil
}
