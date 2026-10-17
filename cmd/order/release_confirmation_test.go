package main

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

func rcEnv(mut func(*releaseConfirmationEnv)) releaseConfirmationEnv {
	in := releaseConfirmationEnv{
		mode: "kafka", groupID: "order-management-release-confirmation", brokers: "kafka-1:9092, kafka-2:9092",
		databaseURL: "postgres://x", eventPublisher: "kafka",
	}
	if mut != nil {
		mut(&in)
	}
	return in
}

func TestParseReleaseConfirmationConfig_OffIsTheDefaultAndNeedsNothing(t *testing.T) {
	for _, mode := range []string{"", "off", "OFF", "  off  "} {
		cfg, err := parseReleaseConfirmationConfig(releaseConfirmationEnv{mode: mode})
		if err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
		if cfg.enabled() || cfg.mode != releaseConfirmationModeOff {
			t.Fatalf("mode %q: cfg = %+v, want off", mode, cfg)
		}
	}
}

func TestParseReleaseConfirmationConfig_KafkaWithEveryPrerequisite(t *testing.T) {
	cfg, err := parseReleaseConfirmationConfig(rcEnv(nil))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !cfg.enabled() || cfg.groupID != "order-management-release-confirmation" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if len(cfg.brokers) != 2 || cfg.brokers[0] != "kafka-1:9092" || cfg.brokers[1] != "kafka-2:9092" {
		t.Fatalf("brokers = %v, want the trimmed list", cfg.brokers)
	}
	if _, err := parseReleaseConfirmationConfig(rcEnv(func(e *releaseConfirmationEnv) { e.mode = " Kafka " })); err != nil {
		t.Fatalf("the mode is case- and space-insensitive: %v", err)
	}
	if _, err := parseReleaseConfirmationConfig(rcEnv(func(e *releaseConfirmationEnv) { e.eventPublisher = "KAFKA" })); err != nil {
		t.Fatalf("EVENT_PUBLISHER is case-insensitive like buildRepoAdapters: %v", err)
	}
}

// Every missing prerequisite fails boot, and the message names the variable
// the operator must set.
func TestParseReleaseConfirmationConfig_KafkaMissingAPrerequisiteFailsBootNamingIt(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*releaseConfirmationEnv)
		want string
	}{
		{"no consumer group", func(e *releaseConfirmationEnv) { e.groupID = "" }, "RELEASE_CONFIRMATION_CONSUMER_GROUP"},
		{"blank consumer group", func(e *releaseConfirmationEnv) { e.groupID = "   " }, "RELEASE_CONFIRMATION_CONSUMER_GROUP"},
		{"no brokers", func(e *releaseConfirmationEnv) { e.brokers = "" }, "KAFKA_BROKERS"},
		{"only separators in brokers", func(e *releaseConfirmationEnv) { e.brokers = " , ," }, "KAFKA_BROKERS"},
		{"no database", func(e *releaseConfirmationEnv) { e.databaseURL = "" }, "DATABASE_URL"},
		{"log publisher", func(e *releaseConfirmationEnv) { e.eventPublisher = "log" }, "EVENT_PUBLISHER=kafka"},
		{"empty publisher", func(e *releaseConfirmationEnv) { e.eventPublisher = "" }, "EVENT_PUBLISHER=kafka"},
		{"unknown mode", func(e *releaseConfirmationEnv) { e.mode = "http" }, "RELEASE_CONFIRMATION_MODE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseReleaseConfirmationConfig(rcEnv(tt.mut))
			if !errors.Is(err, errReleaseConfirmationConfig) {
				t.Fatalf("err = %v, want errReleaseConfirmationConfig", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %q, want it to name %q", err, tt.want)
			}
		})
	}
}

func TestReleaseConfirmationConfigFromEnv_DefaultsTimeoutTo15MinutesAndZeroDisablesIt(t *testing.T) {
	quiet := slog.New(slog.DiscardHandler)
	t.Setenv(releaseConfirmationModeEnv, "")
	cfg, err := releaseConfirmationConfigFromEnv(quiet)
	if err != nil || cfg.enabled() {
		t.Fatalf("default = %+v, %v; want off and no error", cfg, err)
	}
	if cfg.timeout != 15*time.Minute {
		t.Fatalf("timeout = %v, want the 15m default", cfg.timeout)
	}

	t.Setenv(releaseConfirmationModeEnv, "kafka")
	t.Setenv(releaseConfirmationGroupEnv, "grp")
	t.Setenv("KAFKA_BROKERS", "b:9092")
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv(releaseConfirmationTimeoutEnv, "45m")
	cfg, err = releaseConfirmationConfigFromEnv(quiet)
	if err != nil || !cfg.enabled() || cfg.timeout != 45*time.Minute {
		t.Fatalf("configured = %+v, %v; want kafka with a 45m timeout", cfg, err)
	}

	t.Setenv(releaseConfirmationTimeoutEnv, "0")
	cfg, _ = releaseConfirmationConfigFromEnv(quiet)
	if cfg.timeout != 0 {
		t.Fatalf("timeout = %v, want 0 (sweep disabled)", cfg.timeout)
	}

	t.Setenv(releaseConfirmationTimeoutEnv, "soon")
	cfg, _ = releaseConfirmationConfigFromEnv(quiet)
	if cfg.timeout != 15*time.Minute {
		t.Fatalf("an unparseable timeout must fall back to the default, got %v", cfg.timeout)
	}
}

// The chart ConfigMap and the Sweeper share one default retention.
func TestHousekeepingSettingsFromEnv_ReleaseConfirmationRetentionDefaultsToSevenDays(t *testing.T) {
	quiet := slog.New(slog.DiscardHandler)
	got := housekeepingSettingsFromEnv(quiet).releaseConfirmationRetention
	if got != 7*24*time.Hour {
		t.Fatalf("retention = %v, want 168h", got)
	}
	if got != postgres.DefaultReleaseConfirmationRetention {
		t.Fatalf("retention = %v, want the Sweeper's own default %v", got, postgres.DefaultReleaseConfirmationRetention)
	}
	t.Setenv(releaseConfirmationRetentionEnv, "0")
	if got := housekeepingSettingsFromEnv(quiet).releaseConfirmationRetention; got != 0 {
		t.Fatalf("retention = %v, want 0 (keep forever)", got)
	}
}

func TestBuildFlagUnconfirmed_IsNilUnlessTheLoopIsOnWithATimeout(t *testing.T) {
	quiet := slog.New(slog.DiscardHandler)
	// Off (the default): no hook, whatever else is set.
	if hook := buildFlagUnconfirmed(nil, quiet); hook != nil {
		t.Fatal("no pool and mode off must give no sweep hook")
	}
	t.Setenv(releaseConfirmationModeEnv, "kafka")
	t.Setenv(releaseConfirmationGroupEnv, "grp")
	t.Setenv("KAFKA_BROKERS", "b:9092")
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("EVENT_PUBLISHER", "kafka")
	if hook := buildFlagUnconfirmed(nil, quiet); hook != nil {
		t.Fatal("kafka mode without a pool (in-memory run) must give no sweep hook")
	}
}

func TestBuildReleaseConfirmations_NilInterfaceWhenOffOrWithoutAPool(t *testing.T) {
	if got := buildReleaseConfirmations(releaseConfirmationConfig{mode: releaseConfirmationModeOff}, nil); got != nil {
		t.Fatalf("off: %v, want a nil interface so the use cases' nil check is honest", got)
	}
	if got := buildReleaseConfirmations(releaseConfirmationConfig{mode: releaseConfirmationModeKafka}, nil); got != nil {
		t.Fatalf("kafka without a pool: %v, want a nil interface (never a typed nil)", got)
	}
}
