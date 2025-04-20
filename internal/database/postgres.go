package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
	"time"
)

type PostgresConfig struct {
	Host               string        `mapstructure:"host" validate:"required"`
	Port               string        `mapstructure:"port" validate:"required"`
	Username           string        `mapstructure:"username" validate:"required"`
	Password           string        `mapstructure:"password" validate:"required"`
	Schema             string        `mapstructure:"schema" validate:"required"`
	MaxLifeTime        time.Duration `mapstructure:"max_life_time" validate:"required"`
	MaxIdleConnections int           `mapstructure:"max_idle_connections" validate:"required"`
	MaxOpenConnections int           `mapstructure:"max_open_connections" validate:"required"`
}

func ConnectPostgresDB(ctx context.Context, cfg *PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Schema)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to init database")
		return nil, err
	}

	db.SetConnMaxLifetime(cfg.MaxLifeTime)
	db.SetMaxIdleConns(cfg.MaxIdleConnections)
	db.SetMaxOpenConns(cfg.MaxOpenConnections)

	if err := db.Ping(); err != nil {
		log.Error().Err(err).Msg("Failed to ping database")
		return nil, err
	}

	return db, nil
}

func Migrate(ctx context.Context, db *sql.DB) error {
	instance, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to init instance")
		return err
	}

	m, err := migrate.NewWithDatabaseInstance("file://migrations", "postgres", instance)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to init migrate instance")
		return err
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to migrate database")
		return err
	}

	log.Ctx(ctx).Info().Msg("Migrate database successfully")
	return nil
}
