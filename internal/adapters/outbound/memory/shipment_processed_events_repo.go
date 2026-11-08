package memory

import (
	"context"
	"sync"
)

// ShipmentProcessedEventsRepo is an in-memory implementation of
// ports.ShipmentProcessedEvents (ADR 0051), used for tests and `go run
// ./cmd/order` with no DATABASE_URL set.
type ShipmentProcessedEventsRepo struct {
	mu        sync.Mutex
	processed map[string]bool
}

// NewShipmentProcessedEventsRepo constructs an empty
// ShipmentProcessedEventsRepo.
func NewShipmentProcessedEventsRepo() *ShipmentProcessedEventsRepo {
	return &ShipmentProcessedEventsRepo{processed: make(map[string]bool)}
}

// MarkProcessed records eventId if absent, returning true iff this call
// newly recorded it.
func (r *ShipmentProcessedEventsRepo) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.processed[eventId] {
		return false, nil
	}
	r.processed[eventId] = true
	return true, nil
}
