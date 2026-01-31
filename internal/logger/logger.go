package logger

import (
	"path/filepath"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogger() zerolog.Logger {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	return log.With().Caller().Logger()
}
