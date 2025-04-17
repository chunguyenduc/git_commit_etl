package file

import (
	"context"
	"log/slog"
	"os"
)

type Reader struct {
}

func NewFileReader() (*Reader, error) {
	return &Reader{}, nil
}

func (r *Reader) ReadFile(ctx context.Context, filePath string) ([]byte, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read file", "error", err, "file_path", filePath)
		return nil, err
	}

	return data, nil
}
