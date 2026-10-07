// Package usecases: product classification local copy (ADR 0036).
//
// order-management keeps a LOCAL copy of product-master's classifications,
// fed by its ProductClassified events, and answers
// ports.ProductClassificationLookup from it. This file is the write side:
// it never calls product-master and never touches an order.
package usecases

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/claudioed/order-management/internal/application/ports"
)

// ErrInvalidProductClassification marks a classification record that can
// never be applied (no SKU, or a version below 1). It is deterministic: the
// consumer skips the message instead of retrying it.
var ErrInvalidProductClassification = errors.New("invalid product classification")

// ApplyProductClassification is the Kafka-consumer-driven use case that
// writes the local copy. It is idempotent on the CloudEvents id and atomic:
// the idempotency claim and the version-guarded upsert run in ONE
// UnitOfWork, so a failure after the claim rolls the claim back too and the
// redelivery is processed instead of being skipped as "already handled".
// Mirrors ApplyPlannedCapacity (ADR 0031).
type ApplyProductClassification struct {
	Copy      ports.ProductClassificationCopy
	Processed ports.ProductClassificationProcessedEvents
	// UnitOfWork brackets the claim and the upsert. Optional: nil means
	// no transactional backing (the in-memory configuration).
	UnitOfWork ports.UnitOfWork
	// Logger receives non-fatal records (duplicate id, stale version).
	// Optional.
	Logger *slog.Logger
}

// ApplyProductClassificationRequest is one decoded ProductClassified event:
// the CloudEvents id (idempotency key) and the classification it states.
type ApplyProductClassificationRequest struct {
	EventID string
	Record  ports.ProductClassificationRecord
}

// Validate reports ErrInvalidProductClassification for a request that can
// never be applied.
func (r ApplyProductClassificationRequest) Validate() error {
	switch {
	case r.EventID == "":
		return fmt.Errorf("%w: empty event id", ErrInvalidProductClassification)
	case r.Record.SKU == "":
		return fmt.Errorf("%w: empty sku", ErrInvalidProductClassification)
	case r.Record.Version < 1:
		return fmt.Errorf("%w: sku %q has version %d, want >= 1", ErrInvalidProductClassification, r.Record.SKU, r.Record.Version)
	}
	return nil
}

// Execute applies req. An invalid request returns
// ErrInvalidProductClassification BEFORE any transaction opens. Every other
// error comes from the claim or the upsert and leaves nothing written.
func (uc *ApplyProductClassification) Execute(ctx context.Context, req ApplyProductClassificationRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.EventID)
		if err != nil {
			return err
		}
		if !isNew {
			uc.log("product classification: event already processed, skipping", "event_id", req.EventID)
			return nil
		}
		applied, err := uc.Copy.Upsert(ctx, req.Record)
		if err != nil {
			return err
		}
		if !applied {
			uc.log("product classification: stale version ignored",
				"event_id", req.EventID, "sku", req.Record.SKU, "version", req.Record.Version)
		}
		return nil
	})
}

func (uc *ApplyProductClassification) log(msg string, args ...any) {
	if uc.Logger != nil {
		uc.Logger.Warn(msg, args...)
	}
}
