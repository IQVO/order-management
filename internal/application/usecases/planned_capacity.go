// Package usecases: planned capacity (ADR 0031).
//
// order-management mirrors warehouse-planning's published CapacityPlan
// events into a LOCAL read model (ports.PlannedCapacityRepo) and lets a
// published shortage ANNOTATE the orders whose promise it overlaps. It
// never calls warehouse-planning, never moves a promise, never rejects an
// order and never touches allocation or reservations.
package usecases

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
)

// ApplyPlannedCapacity is the Kafka-consumer-driven use case that writes
// the read model. It is idempotent on the CloudEvents id and atomic: the
// idempotency claim and the upsert run in ONE UnitOfWork, so a failure
// after the claim rolls the claim back too and the redelivery is processed
// instead of being skipped as "already handled" (which would be silent data
// loss). Mirrors RepromiseOrder's shape and its transaction mechanism.
type ApplyPlannedCapacity struct {
	Windows   ports.PlannedCapacityRepo
	Processed ports.PlannedCapacityProcessedEvents
	// UnitOfWork brackets the claim and the upsert. Optional: nil means
	// no transactional backing (the in-memory dev configuration), exactly
	// as for RepromiseOrder.
	UnitOfWork ports.UnitOfWork
	// Logger receives non-fatal records (duplicate, stale write). Optional.
	Logger *slog.Logger
}

// ApplyPlannedCapacityRequest is one decoded warehouse-planning event:
// the CloudEvents id (idempotency key) and the window it states.
type ApplyPlannedCapacityRequest struct {
	EventID string
	Window  order.PlannedCapacityWindow
}

// Execute applies req. An invalid window returns
// order.ErrInvalidPlannedCapacity BEFORE any transaction opens, so a bad
// message never touches the database. Every other error is an
// infrastructure failure from the claim or the upsert and leaves nothing
// written (the transaction rolls back).
func (uc *ApplyPlannedCapacity) Execute(ctx context.Context, req ApplyPlannedCapacityRequest) error {
	if err := req.Window.Validate(); err != nil {
		return err
	}
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.EventID)
		if err != nil {
			return err
		}
		if !isNew {
			uc.log("planned capacity: event already processed, skipping", "event_id", req.EventID)
			return nil
		}
		applied, err := uc.Windows.Upsert(ctx, req.Window)
		if err != nil {
			return err
		}
		if !applied {
			uc.log("planned capacity: stale or downgrading write ignored (last writer wins)",
				"event_id", req.EventID, "plan_id", req.Window.PlanID)
		}
		return nil
	})
}

func (uc *ApplyPlannedCapacity) log(msg string, args ...any) {
	if uc.Logger != nil {
		uc.Logger.Warn(msg, args...)
	}
}

// GetPlannedCapacity reads the read model for one site
// (GET /planned-capacity?site=).
type GetPlannedCapacity struct {
	Windows ports.PlannedCapacityRepo
	Clock   ports.Clock
}

// Execute lists the windows at site that end after from (any status).
// A nil from means "now".
func (uc *GetPlannedCapacity) Execute(ctx context.Context, site string, from *time.Time) ([]order.PlannedCapacityWindow, error) {
	at := uc.Clock.Now()
	if from != nil {
		at = *from
	}
	return uc.Windows.ListByLocation(ctx, site, at)
}

// OrderCapacityConstraints answers, for one order, which published
// shortage windows its promise is exposed to (order.CapacityConstraints).
// SiteID is the planning `location` it matches on: order-management does
// not model which site an order ships from (see order.PromisePolicy), so
// this is the single configured site, exactly as the promise itself is.
type OrderCapacityConstraints struct {
	Windows ports.PlannedCapacityRepo
	Clock   ports.Clock
	SiteID  string
}

// For returns the constraining windows for o, or nil when there are none.
// An order with no promise yet is answered without touching the read model.
func (uc *OrderCapacityConstraints) For(ctx context.Context, o *order.Order) ([]order.PlannedCapacityWindow, error) {
	if o.PromiseDate() == nil && len(o.PromiseGroups()) == 0 {
		return nil, nil
	}
	now := uc.Clock.Now()
	windows, err := uc.Windows.ListByLocation(ctx, uc.SiteID, now)
	if err != nil {
		return nil, err
	}
	return order.CapacityConstraints(o, now, uc.SiteID, windows), nil
}
