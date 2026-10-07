package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// Environment variables of the product-classification local copy (ADR 0036).
const (
	// productClassificationModeEnv selects how ports.ProductClassificationLookup
	// is answered: "permissive" (default, always unknown) or "kafka" (a local
	// copy fed by product-master's ProductClassified events). "http" — the
	// live inventory-storage lookup of ADR 0016 — was removed and is a boot
	// error, so a stale deployment fails loudly.
	productClassificationModeEnv = "PRODUCT_CLASSIFICATION_MODE"
	// productClassificationGroupEnv is the STABLE Kafka consumer group of the
	// copy's consumer. Required in kafka mode; there is deliberately NO
	// default, so a locally run process can never join a live cluster's group.
	productClassificationGroupEnv = "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"
)

const (
	classificationModePermissive = "permissive"
	classificationModeKafka      = "kafka"
	classificationModeRemoved    = "http"
)

// errClassificationConfig marks a PRODUCT_CLASSIFICATION_* configuration
// the service refuses to boot with.
var errClassificationConfig = errors.New("invalid product classification configuration")

// classificationConfig is the validated PRODUCT_CLASSIFICATION_* settings.
type classificationConfig struct {
	mode    string
	groupID string
	brokers []string
}

// classificationConfigFromEnv reads and validates the settings BEFORE any
// adapter is built, so a bad configuration fails boot first.
func classificationConfigFromEnv() (classificationConfig, error) {
	return parseClassificationConfig(
		getenv(productClassificationModeEnv, classificationModePermissive),
		os.Getenv(productClassificationGroupEnv),
		os.Getenv("KAFKA_BROKERS"),
	)
}

// parseClassificationConfig validates mode/group/brokers: permissive needs
// nothing; kafka needs a consumer group and brokers; http and anything else
// are rejected.
func parseClassificationConfig(mode, groupID, brokers string) (classificationConfig, error) {
	switch m := strings.ToLower(strings.TrimSpace(mode)); m {
	case "", classificationModePermissive:
		return classificationConfig{mode: classificationModePermissive}, nil
	case classificationModeKafka:
		if strings.TrimSpace(groupID) == "" {
			return classificationConfig{}, fmt.Errorf("%w: %s=kafka requires %s (a stable consumer group id; there is no default)",
				errClassificationConfig, productClassificationModeEnv, productClassificationGroupEnv)
		}
		list := splitBrokers(brokers)
		if len(list) == 0 {
			return classificationConfig{}, fmt.Errorf("%w: %s=kafka requires KAFKA_BROKERS",
				errClassificationConfig, productClassificationModeEnv)
		}
		return classificationConfig{mode: classificationModeKafka, groupID: strings.TrimSpace(groupID), brokers: list}, nil
	case classificationModeRemoved:
		return classificationConfig{}, fmt.Errorf("%w: %s=http was removed by ADR 0036 (classification moved to product-master; this service reads a local copy of its ProductClassified events): set %s=kafka with %s, or %s=permissive",
			errClassificationConfig, productClassificationModeEnv, productClassificationModeEnv, productClassificationGroupEnv, productClassificationModeEnv)
	default:
		return classificationConfig{}, fmt.Errorf("%w: unknown %s %q (want kafka or permissive)",
			errClassificationConfig, productClassificationModeEnv, mode)
	}
}

func splitBrokers(raw string) []string {
	var out []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// productClassification is the wired lookup plus, in kafka mode, what the
// copy's consumer needs.
type productClassification struct {
	cfg       classificationConfig
	lookup    ports.ProductClassificationLookup
	copy      ports.ProductClassificationCopy
	processed ports.ProductClassificationProcessedEvents
	uow       ports.UnitOfWork
}

// buildProductClassification selects the lookup adapter for cfg. In kafka
// mode it uses the Postgres copy when pool is non-nil (migration 0011 ran
// in buildRepoAdapters) and the in-memory copy otherwise. The Postgres
// configuration ALWAYS gets its own UnitOfWork over pool, so the claim and
// the upsert commit atomically regardless of EVENT_PUBLISHER; a nil
// *postgres.UnitOfWork is never boxed into the interface.
func buildProductClassification(cfg classificationConfig, pool *pgxpool.Pool, logger *slog.Logger) *productClassification {
	pc := &productClassification{cfg: cfg}
	if cfg.mode != classificationModeKafka {
		logger.Warn("product classification lookup in permissive (fail-open) mode; path eligibility routing will see no derived product attributes",
			"hint", "set "+productClassificationModeEnv+"=kafka and "+productClassificationGroupEnv+" for a real deployment (ADR 0036)")
		pc.lookup = productclassificationcopy.NewPermissiveLookup()
		return pc
	}
	if pool == nil {
		logger.Warn("database url not configured; using the in-memory product classification copy (empty on every start)")
		store := productclassificationcopy.NewMemoryStore()
		pc.lookup, pc.copy = store, store
		pc.processed = productclassificationcopy.NewMemoryProcessedEvents()
		return pc
	}
	store := productclassificationcopy.NewPostgresStore(pool, logger)
	pc.lookup, pc.copy = store, store
	pc.processed = productclassificationcopy.NewPostgresProcessedEvents(pool)
	pc.uow = postgres.NewUnitOfWork(pool)
	return pc
}

// startConsumer runs the copy's Kafka consumer in kafka mode and returns a
// stop function that cancels it and waits (bounded) for the loop to return,
// so the in-flight message's offset commit is not abandoned. In permissive
// mode it starts nothing and returns a no-op.
func (pc *productClassification) startConsumer(logger *slog.Logger) (stop func()) {
	if pc.cfg.mode != classificationModeKafka {
		return func() {}
	}
	apply := &usecases.ApplyProductClassification{Copy: pc.copy, Processed: pc.processed, UnitOfWork: pc.uow, Logger: logger}
	consumer := inboundkafka.NewProductClassificationConsumer(pc.cfg.brokers, pc.cfg.groupID, apply, logger)
	logger.Info("product classification consumer configured",
		"topic", inboundkafka.ProductMasterEventsTopic, "group_id", pc.cfg.groupID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("product classification consumer stopped", "error", err)
		}
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warn("product classification consumer did not stop before the shutdown deadline")
		}
		if err := consumer.Close(); err != nil {
			logger.Error("error closing product classification consumer", "error", err)
		}
	}
}
