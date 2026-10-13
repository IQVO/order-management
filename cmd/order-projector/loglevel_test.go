package main

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewLogger_AcceptsWarningAlias pins fleet decision D22: LOG_LEVEL
// "warning" (any case) means warn in every binary, like "warn".
func TestNewLogger_AcceptsWarningAlias(t *testing.T) {
	for _, level := range []string{"warn", "warning", "WARNING", "Warn"} {
		logger := newLogger(level)
		if logger.Enabled(context.Background(), slog.LevelInfo) {
			t.Errorf("LOG_LEVEL=%q: info is enabled, want warn", level)
		}
		if !logger.Enabled(context.Background(), slog.LevelWarn) {
			t.Errorf("LOG_LEVEL=%q: warn is disabled, want warn", level)
		}
	}
}
