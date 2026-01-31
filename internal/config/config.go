package config

import (
	"context"
	"path"

	"github.com/chunguyenduc/git_commit_etl/internal/database"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
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
		SourceData      *GitHubRepoConfig `mapstructure:"source_data" validate:"required"`
		MonthCounts     int               `mapstructure:"month_counts" validate:"required"`
		StorageDir      string            `mapstructure:"storage_dir" validate:"required"`
		IngestorWorker  int               `mapstructure:"ingestor_worker" validate:"required,gt=0,lte=10"`
		ExtractorWorker int               `mapstructure:"extractor_worker" validate:"required,gt=0,lte=10"`
	}

	TransformerConfig struct {
		StorageDir string `mapstructure:"storage_dir" validate:"required"`
		BatchSize  int    `mapstructure:"batch_size" validate:"required,gt=0"`
	}

	LoaderConfig struct {
		DestinationData *database.PostgresConfig `mapstructure:"destination_data" validate:"required"`
		BatchSize       int                      `mapstructure:"batch_size" validate:"required,gt=0"`
	}
)

const (
	configDir = "conf"
)

var validate = validator.New()

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

	if err := validate.Struct(cfg); err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to validate config")
		return nil, err
	}

	log.Ctx(ctx).Info().Interface("config", cfg).Msg("Loaded config")
	return &cfg, nil
}
