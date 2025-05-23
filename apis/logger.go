package apis

import (
	"log/slog"
	"os"
)

// LoggerLevel log levels.
type LoggerLevel string

const (
	LoggerLevelDebug LoggerLevel = "debug"
	LoggerLevelInfo  LoggerLevel = "info"
	LoggerLevelWarn  LoggerLevel = "warn"
	LoggerLevelError LoggerLevel = "error"
)

// Logger represent configuration for any logger.
type Logger struct {
	Level LoggerLevel `json:"level" yaml:"level" env:"LEVEL,required" valid:"required"`
	Fmt   string      `json:"fmt" yaml:"fmt" env:"FMT,default=json"`
}

// InitSlog init slog logger instance with version field and hook for Sentry.
func InitSlog(cfg *Logger, version string, hook bool) *slog.Logger {
	var handler slog.Handler

	levels := map[LoggerLevel]slog.Level{
		LoggerLevelDebug: slog.LevelDebug,
		LoggerLevelInfo:  slog.LevelInfo,
		LoggerLevelWarn:  slog.LevelWarn,
		LoggerLevelError: slog.LevelError,
	}

	opt := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	if lvl, ok := levels[cfg.Level]; ok {
		opt.Level = lvl
	}

	handler = slog.NewJSONHandler(os.Stdout, &opt)

	if cfg.Fmt == "text" {
		handler = slog.NewTextHandler(os.Stdout, &opt)
	}

	if hook {
		// TODO: add OTEL hook
	}

	logger := slog.New(handler).With("version", version)

	return logger
}
