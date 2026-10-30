package main

import (
	"log/slog"
	"testing"
)

// TestLogLevel_AcceptsWarningAlias pins fleet decision D22: LOG_LEVEL
// "warning" (any case) means warn in every binary, like "warn"; the other
// levels are case-insensitive too, like cmd/order's newLogger.
func TestLogLevel_AcceptsWarningAlias(t *testing.T) {
	cases := map[string]slog.Level{
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"WARNING": slog.LevelWarn,
		"DEBUG":   slog.LevelDebug,
		"Error":   slog.LevelError,
		"bogus":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := logLevel(in); got != want {
			t.Errorf("logLevel(%q) = %v, want %v", in, got, want)
		}
	}
}
