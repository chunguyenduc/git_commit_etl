package sensor

import (
	"context"
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
)

type HandlerFunc func(event fsnotify.Event) error

type FileSensor struct {
	watcher *fsnotify.Watcher
	handler HandlerFunc
}

func NewFileSensor(dirs []string) (*FileSensor, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error().Err(err).Msg("Error creating file watcher")
		return nil, err
	}

	for _, dir := range dirs {
		if err := watcher.Add(dir); err != nil {
			log.Error().Err(err).Msg("Error adding file watcher")
			return nil, err
		}
	}

	return &FileSensor{watcher: watcher}, nil
}

func (fs *FileSensor) Add(path string) error {
	return fs.watcher.Add(path)
}

func (fs *FileSensor) Listen(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-fs.watcher.Events:
			if !ok {
				return nil
			}
			log.Ctx(ctx).Info().Interface("event", event).Msg("file event")
			if event.Has(fsnotify.Write) {
			}
		case err, ok := <-fs.watcher.Errors:
			if !ok {
				return nil
			}
			log.Ctx(ctx).Error().Err(err).Msg("errors event")
			return err
		}
	}
}
