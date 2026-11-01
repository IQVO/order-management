package order

import (
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// OrderSnapshot is the persisted state of an Order, the single input of
// Rehydrate. Field zero values are the safe defaults a caller that does
// not care about a field should get:
//
//   - PromiseDate / PromiseCptID / PromiseBasis are nil for orders
//     persisted before ADR 0014, or for a promise never given a CPT
//     identity (a LeadTime-basis promise leaves PromiseCptID nil;
//     PromiseBasis is still recorded).
//   - PromiseGroups (ADR 0014 §3 / ADR 0017) may be nil — an order
//     persisted before that ADR, or one whose summary fields were set via
//     the legacy SetPromise path. PromiseGroups() then returns an empty
//     slice and the summary fields are exactly what was passed in.
//   - HeldAtIntake is ADR 0020 §1's releaseOnAllocation=false as its
//     INVERSE, deliberately (see Order.heldAtIntake): the zero value is
//     an ordinary order that releases on allocation. A row written before
//     migration 0005 reads back releaseOnAllocation=TRUE (the column's
//     DEFAULT), i.e. HeldAtIntake=false — pre-ADR orders rehydrate as
//     un-held, which is what they are.
//   - RequiredShipBy (ADR 0020 §2) is nil for the overwhelming majority
//     of orders.
//   - FulfillmentSiteID (ADR 0044) is nil for every order whose caller
//     named no site, including every row written before migration 0013
//     (the nullable column reads back NULL).
//   - Version is the optimistic-concurrency version the row was loaded at.
//     The zero value means "never persisted" and rehydrates as 1, the
//     same starting version New assigns; every persisted row is >= 1
//     (the version migration's DEFAULT), so a pre-existing order's first
//     post-migration Save is guarded against version 1, never a mismatch
//     it could never have satisfied.
type OrderSnapshot struct {
	ID                   shared.OrderId
	Lines                []*OrderLine
	AllowPartialShipment bool
	PromiseDate          *time.Time
	PromiseCptID         *string
	PromiseBasis         *PromiseBasis
	PromiseGroups        []PromiseGroup
	HeldAtIntake         bool
	RequiredShipBy       *time.Time
	FulfillmentSiteID    *shared.FulfillmentSiteID
	Version              int
}

// Rehydrate rebuilds an Order from persisted state without re-running
// construction invariants. It is the ONE persistence entry point: the
// outbound repository adapter calls it with what it read; tests and
// PromisePolicy's in-memory scratch computation call it with a partial
// snapshot (nothing that does persists the result, so the defaulted
// Version is inert there).
func Rehydrate(s OrderSnapshot) *Order {
	version := s.Version
	if version == 0 {
		version = 1
	}
	return &Order{
		id: s.ID, lines: s.Lines, allowPartialShipment: s.AllowPartialShipment,
		promiseDate: s.PromiseDate, promiseCptId: s.PromiseCptID, promiseBasis: s.PromiseBasis,
		promiseGroups:   s.PromiseGroups,
		heldAtIntake:    s.HeldAtIntake,
		requiredShipBy:  s.RequiredShipBy,
		fulfillmentSite: copySite(s.FulfillmentSiteID),
		version:         version,
	}
}

func copySite(s *shared.FulfillmentSiteID) *shared.FulfillmentSiteID {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}
