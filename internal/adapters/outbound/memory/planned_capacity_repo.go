package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

// PlannedCapacityRepo is an in-memory ports.PlannedCapacityRepo (ADR 0031),
// used by tests and `go run ./cmd/order` with no DATABASE_URL. It applies
// the same last-writer-wins rule as the Postgres adapter, through the one
// domain definition (order.PlannedCapacityWindow.Supersedes).
type PlannedCapacityRepo struct {
	mu      sync.Mutex
	windows map[string]order.PlannedCapacityWindow
}

// NewPlannedCapacityRepo constructs an empty PlannedCapacityRepo.
func NewPlannedCapacityRepo() *PlannedCapacityRepo {
	return &PlannedCapacityRepo{windows: make(map[string]order.PlannedCapacityWindow)}
}

// Upsert stores w unless it is stale or a downgrade of what is stored.
func (r *PlannedCapacityRepo) Upsert(_ context.Context, w order.PlannedCapacityWindow) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.windows[w.PlanID]; ok && !w.Supersedes(prev) {
		return false, nil
	}
	r.windows[w.PlanID] = w
	return true, nil
}

// ListByLocation returns the windows at location ending after endingAfter,
// ordered by Start then PlanID.
func (r *PlannedCapacityRepo) ListByLocation(_ context.Context, location string, endingAfter time.Time) ([]order.PlannedCapacityWindow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []order.PlannedCapacityWindow
	for _, w := range r.windows {
		if w.Location == location && w.End.After(endingAfter) {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].PlanID < out[j].PlanID
	})
	return out, nil
}

// PlannedCapacityProcessedEventsRepo is an in-memory
// ports.PlannedCapacityProcessedEvents.
type PlannedCapacityProcessedEventsRepo struct {
	mu        sync.Mutex
	processed map[string]bool
}

// NewPlannedCapacityProcessedEventsRepo constructs an empty repo.
func NewPlannedCapacityProcessedEventsRepo() *PlannedCapacityProcessedEventsRepo {
	return &PlannedCapacityProcessedEventsRepo{processed: make(map[string]bool)}
}

// MarkProcessed records eventId if absent, returning true iff this call
// newly recorded it.
func (r *PlannedCapacityProcessedEventsRepo) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.processed[eventId] {
		return false, nil
	}
	r.processed[eventId] = true
	return true, nil
}
