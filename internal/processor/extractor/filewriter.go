package extractor

import (
	"context"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
)

type FileWriter struct {
	StorageDir string
}

func NewFileWriter(storageDir string) *FileWriter {
	return &FileWriter{
		StorageDir: storageDir,
	}
}

func (fw *FileWriter) WriteFile(ctx context.Context, filename string, content []byte) error {
	if err := os.MkdirAll(fw.StorageDir, 0755); err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to create storage directory")
		return err
	}

	filePath := filepath.Join(fw.StorageDir, filename)

	if err := os.WriteFile(filePath, content, 0644); err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to write file")
		return err
	}

	return nil
}
