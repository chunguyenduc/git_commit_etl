package database

import (
	"context"

	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"
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

func ConnectPostgresDB(ctx context.Context, cfg *PostgresConfig) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Schema)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	config.MaxConns = int32(cfg.MaxOpenConnections)
	config.MinConns = int32(cfg.MaxIdleConnections)
	config.MaxConnLifetime = cfg.MaxLifeTime

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to init database")
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to ping database")
		return nil, err
	}

	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
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
