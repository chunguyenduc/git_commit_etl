package file

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/rs/zerolog/log"
	"os"
	"path"
)

type Reader struct {
	storageDir string
}

func NewFileReader(cfg *config.TransformerConfig) *Reader {
	return &Reader{
		storageDir: cfg.StorageDir,
	}
}

func (r *Reader) ReadFile(ctx context.Context, fileName string) ([]byte, error) {
	data, err := os.ReadFile(path.Join(r.storageDir, fileName))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("file_name", fileName).Msg("Failed to read file")
		return nil, err
	}

	return data, nil
}
