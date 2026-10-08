package whatsmeow_service

import (
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type fakeMeowcallerLogger struct {
	infos  []string
	warns  []string
	errors []string
	debugs []string
}

func (f *fakeMeowcallerLogger) LogInfo(format string, args ...interface{}) {
	f.infos = append(f.infos, sprintf(format, args...))
}

func (f *fakeMeowcallerLogger) LogWarn(format string, args ...interface{}) {
	f.warns = append(f.warns, sprintf(format, args...))
}

func (f *fakeMeowcallerLogger) LogError(format string, args ...interface{}) {
	f.errors = append(f.errors, sprintf(format, args...))
}

func (f *fakeMeowcallerLogger) LogDebug(format string, args ...interface{}) {
	f.debugs = append(f.debugs, sprintf(format, args...))
}

func sprintf(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

func TestMeowcallerLogLevelDefaultsToInfo(t *testing.T) {
	if got := meowcallerLogLevel(""); got != zerolog.InfoLevel {
		t.Fatalf("empty level = %s, want info", got)
	}
	if got := meowcallerLogLevel("DEBUG"); got != zerolog.DebugLevel {
		t.Fatalf("DEBUG level = %s, want debug", got)
	}
	if got := meowcallerLogLevel("warning"); got != zerolog.WarnLevel {
		t.Fatalf("warning level = %s, want warn", got)
	}
}

func TestMeowcallerLogWriterRoutesByZerologLevel(t *testing.T) {
	logger := &fakeMeowcallerLogger{}
	writer := meowcallerLogWriter{logger: logger, instanceID: "inst"}

	if _, err := writer.Write([]byte(`{"level":"warn","message":"relay silent"}`)); err != nil {
		t.Fatal(err)
	}
	if len(logger.warns) != 1 || !strings.Contains(logger.warns[0], "relay silent") {
		t.Fatalf("warn logs = %#v", logger.warns)
	}

	if _, err := writer.Write([]byte(`{"level":"debug","message":"packet"}`)); err != nil {
		t.Fatal(err)
	}
	if len(logger.debugs) != 1 || !strings.Contains(logger.debugs[0], "packet") {
		t.Fatalf("debug logs = %#v", logger.debugs)
	}
}
