package file

import (
	"context"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"log/slog"
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
		slog.ErrorContext(ctx, "Failed to read file", "error", err, "file_path", fileName)
		return nil, err
	}

	return data, nil
}
