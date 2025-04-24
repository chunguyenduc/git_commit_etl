package file

import (
	"context"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
)

type Writer struct {
	StorageDir string
}

func NewFileWriter(storageDir string) (*Writer, error) {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Error().Err(err).Msg("Failed to create storage directory")
		return nil, err
	}

	return &Writer{
		StorageDir: storageDir,
	}, nil
}

func (fw *Writer) WriteFile(ctx context.Context, filename string, content []byte) error {
	filePath := filepath.Join(fw.StorageDir, filename)

	if err := os.WriteFile(filePath, content, 0644); err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to write file")
		return err
	}

	return nil
}
