package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/order-management/internal/adapters/outbound/productclassificationcopy"
)

// ADR 0036: the removed live-lookup mode is a boot error, never a silent
// fallback to permissive.
func TestParseClassificationConfig_HTTPIsRejectedAtBoot(t *testing.T) {
	for _, mode := range []string{"http", "HTTP", " http "} {
		_, err := parseClassificationConfig(mode, "om-classification", "kafka:9092")
		if !errors.Is(err, errClassificationConfig) {
			t.Fatalf("mode %q: err = %v, want errClassificationConfig", mode, err)
		}
		if !strings.Contains(err.Error(), "ADR 0036") || !strings.Contains(err.Error(), "kafka") {
			t.Fatalf("mode %q: the error must say why and what to set instead: %v", mode, err)
		}
	}
}

func TestParseClassificationConfig_KafkaRequiresAGroupAndBrokers(t *testing.T) {
	if _, err := parseClassificationConfig("kafka", "", "kafka:9092"); !errors.Is(err, errClassificationConfig) ||
		!strings.Contains(err.Error(), productClassificationGroupEnv) {
		t.Fatalf("kafka without a group: err = %v, want a boot error naming %s", err, productClassificationGroupEnv)
	}
	if _, err := parseClassificationConfig("kafka", "  ", "kafka:9092"); !errors.Is(err, errClassificationConfig) {
		t.Fatalf("kafka with a blank group: err = %v", err)
	}
	if _, err := parseClassificationConfig("kafka", "om-classification", " , "); !errors.Is(err, errClassificationConfig) ||
		!strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("kafka without brokers: err = %v, want a boot error naming KAFKA_BROKERS", err)
	}
}

func TestParseClassificationConfig_UnknownModeIsRejected(t *testing.T) {
	if _, err := parseClassificationConfig("rest", "", ""); !errors.Is(err, errClassificationConfig) {
		t.Fatalf("err = %v, want errClassificationConfig", err)
	}
}

func TestParseClassificationConfig_ValidModes(t *testing.T) {
	for _, mode := range []string{"", "permissive", "Permissive"} {
		cfg, err := parseClassificationConfig(mode, "", "")
		if err != nil || cfg.mode != classificationModePermissive {
			t.Fatalf("mode %q = %+v, %v; want permissive", mode, cfg, err)
		}
	}
	cfg, err := parseClassificationConfig("KAFKA", " om-classification ", "b1:9092, b2:9092")
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	if cfg.mode != classificationModeKafka || cfg.groupID != "om-classification" ||
		len(cfg.brokers) != 2 || cfg.brokers[0] != "b1:9092" || cfg.brokers[1] != "b2:9092" {
		t.Fatalf("kafka cfg = %+v", cfg)
	}
}

func TestClassificationConfigFromEnv_ReadsTheEnvironment(t *testing.T) {
	t.Setenv(productClassificationModeEnv, "")
	t.Setenv(productClassificationGroupEnv, "")
	if cfg, err := classificationConfigFromEnv(); err != nil || cfg.mode != classificationModePermissive {
		t.Fatalf("unset = %+v, %v; want the permissive default", cfg, err)
	}
	t.Setenv(productClassificationModeEnv, "http")
	if _, err := classificationConfigFromEnv(); !errors.Is(err, errClassificationConfig) {
		t.Fatalf("http from env: err = %v", err)
	}
	t.Setenv(productClassificationModeEnv, "kafka")
	t.Setenv(productClassificationGroupEnv, "om-classification-test")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	if cfg, err := classificationConfigFromEnv(); err != nil || cfg.groupID != "om-classification-test" {
		t.Fatalf("kafka from env = %+v, %v", cfg, err)
	}
}

func TestBuildProductClassification_PermissiveStartsNothing(t *testing.T) {
	pc := buildProductClassification(classificationConfig{mode: classificationModePermissive}, nil, quietLogger())
	if _, ok := pc.lookup.(productclassificationcopy.PermissiveLookup); !ok {
		t.Fatalf("lookup = %T, want PermissiveLookup", pc.lookup)
	}
	stop := pc.startConsumer(quietLogger())
	if stop == nil {
		t.Fatal("startConsumer must always return a stop function")
	}
	stop() // must not panic or block
}

// No DATABASE_URL: the in-memory copy answers the port AND is what the
// consumer writes, and the UnitOfWork is a true nil interface.
func TestBuildProductClassification_KafkaWithoutDatabaseUsesTheMemoryCopy(t *testing.T) {
	cfg := classificationConfig{mode: classificationModeKafka, groupID: "om-classification-test", brokers: []string{"127.0.0.1:1"}}
	pc := buildProductClassification(cfg, nil, quietLogger())
	store, ok := pc.lookup.(*productclassificationcopy.MemoryStore)
	if !ok {
		t.Fatalf("lookup = %T, want *MemoryStore", pc.lookup)
	}
	if w, ok := pc.copy.(*productclassificationcopy.MemoryStore); !ok || w != store {
		t.Fatal("the consumer must write the same copy the lookup reads")
	}
	if _, ok := pc.processed.(*productclassificationcopy.MemoryProcessedEvents); !ok {
		t.Fatalf("processed = %T, want the in-memory gate", pc.processed)
	}
	if pc.uow != nil {
		t.Fatalf("uow = %v, want a true nil interface", pc.uow)
	}
	// The consumer starts against an unreachable broker and still stops
	// promptly on shutdown.
	pc.startConsumer(quietLogger())()
}
