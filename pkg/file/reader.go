package file

import (
	"context"
	"os"
	"path"

	"github.com/rs/zerolog/log"
)

type Reader struct {
	storageDir string
}

func NewFileReader(storageDir string) *Reader {
	return &Reader{
		storageDir: storageDir,
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
