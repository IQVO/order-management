package main

import "testing"

// TestOutboxMaxAttemptsFromEnv pins OUTBOX_MAX_ATTEMPTS (ADR 0040): unset
// means the fleet default of 20, 0 means unlimited, and an invalid or
// negative value falls back to the default instead of disabling the cap.
func TestOutboxMaxAttemptsFromEnv(t *testing.T) {
	if defaultOutboxMaxAttempts != 20 {
		t.Fatalf("defaultOutboxMaxAttempts = %d, want the fleet default 20", defaultOutboxMaxAttempts)
	}
	tests := []struct {
		name string
		env  string
		want int
	}{
		{"unset defaults to 20", "", 20},
		{"explicit cap", "5", 5},
		{"surrounding spaces are tolerated", " 7 ", 7},
		{"zero means unlimited", "0", 0},
		{"negative falls back", "-1", 20},
		{"garbage falls back", "many", 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(outboxMaxAttemptsEnv, tt.env)
			if got := nonNegativeIntEnv(outboxMaxAttemptsEnv, defaultOutboxMaxAttempts, discardLogger()); got != tt.want {
				t.Fatalf("nonNegativeIntEnv(%q) = %d, want %d", tt.env, got, tt.want)
			}
		})
	}
}
