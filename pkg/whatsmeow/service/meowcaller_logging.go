package whatsmeow_service

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/rs/zerolog"
)

type meowcallerInstanceLogger interface {
	LogInfo(format string, args ...interface{})
	LogWarn(format string, args ...interface{})
	LogError(format string, args ...interface{})
	LogDebug(format string, args ...interface{})
}

type meowcallerLogWriter struct {
	logger     meowcallerInstanceLogger
	instanceID string
}

func newMeowcallerLogger(logger meowcallerInstanceLogger, instanceID string) zerolog.Logger {
	return zerolog.New(meowcallerLogWriter{
		logger:     logger,
		instanceID: instanceID,
	}).Level(meowcallerLogLevel(os.Getenv("MEOWCALLER_LOG_LEVEL"))).
		With().
		Timestamp().
		Str("component", "meowcaller").
		Logger()
}

func meowcallerLogLevel(raw string) zerolog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "trace":
		return zerolog.TraceLevel
	case "debug":
		return zerolog.DebugLevel
	case "warn", "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}

func (w meowcallerLogWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	if line == "" || w.logger == nil {
		return len(p), nil
	}

	var entry struct {
		Level string `json:"level"`
	}
	_ = json.Unmarshal([]byte(line), &entry)

	switch strings.ToLower(entry.Level) {
	case "trace", "debug":
		w.logger.LogDebug("[%s] meowcaller %s", w.instanceID, line)
	case "warn", "warning":
		w.logger.LogWarn("[%s] meowcaller %s", w.instanceID, line)
	case "error", "fatal", "panic":
		w.logger.LogError("[%s] meowcaller %s", w.instanceID, line)
	default:
		w.logger.LogInfo("[%s] meowcaller %s", w.instanceID, line)
	}
	return len(p), nil
}
