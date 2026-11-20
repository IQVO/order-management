package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// statusFor maps a typed domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, usecases.ErrOrderNotFound):
		return http.StatusNotFound

	case errors.Is(err, shared.ErrEmptyOrderID),
		errors.Is(err, shared.ErrEmptySKU),
		errors.Is(err, shared.ErrEmptyPathID),
		errors.Is(err, shared.ErrUnknownProcessPath),
		errors.Is(err, shared.ErrInvalidFulfillmentSiteID),
		errors.Is(err, order.ErrNoLines),
		errors.Is(err, order.ErrLineNotFound):
		return http.StatusBadRequest

	case errors.Is(err, shared.ErrNonPositiveQuantity),
		errors.Is(err, shared.ErrLineIneligibleForResolvedPath),
		errors.Is(err, order.ErrHeldOrderMustBeShipComplete):
		return http.StatusUnprocessableEntity

	case errors.Is(err, order.ErrOrderAlreadyReleased),
		errors.Is(err, order.ErrLineAlreadyAllocated),
		errors.Is(err, order.ErrLineNotPending),
		errors.Is(err, order.ErrLineNotBackordered),
		errors.Is(err, order.ErrLineNotAllocated),
		errors.Is(err, usecases.ErrNoAllocatedLines),
		errors.Is(err, usecases.ErrNoBackorderedLines),
		errors.Is(err, usecases.ErrPromiseDateNotSet),
		errors.Is(err, usecases.ErrOrderNotHeld),
		errors.Is(err, ports.ErrConcurrentModification):
		return http.StatusConflict

	// The downstream Suppliers are not wired up (permissive mode), or an
	// ambiguous transport/timeout/5xx failure reached this context (or the
	// circuit breaker is open). Neither is the caller's fault and neither
	// is a business fact, so both surface as 503 rather than being
	// papered over with a 2xx (ADR-0003).
	case errors.Is(err, ports.ErrDownstreamNotConfigured),
		errors.Is(err, ports.ErrDownstreamUnavailable),
		errors.Is(err, ports.ErrInsufficientStock):
		return http.StatusServiceUnavailable

	default:
		return http.StatusInternalServerError
	}
}

// problemBaseURI is the namespace for this service's RFC 7807 "type" URIs.
// It does not need to resolve to a real page — it is an identifier, unique
// per distinct error category in this service.
const problemBaseURI = "https://errors.order-management.warehouse-systems.dev/"

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response, with the replay class of the category. slug becomes
// the last path segment of "type"; title is a fixed human string for the
// category (the dynamic detail comes from err.Error() at write time, not from
// this table); class says what the Idempotency-Key middleware does with this
// answer on a retry (replay_class.go, ADR 0054). A problemInfo is only ever
// built in this file or with register (problems.go), so none exists without a
// class: replay_class_test.go scans for any other literal.
type problemInfo struct {
	slug  string
	title string
	class replayClass
}

// problemCatalog maps each typed domain/application error to its fixed
// RFC 7807 (type, title, replay class) triple, mirroring statusFor's
// groupings one-for-one. Entries are checked with errors.Is in order, so this
// sequence preserves the original switch's first-match precedence exactly.
//
// The class is the last field of each entry (ADR 0054 holds the reason of every
// non-FINAL one): FINAL is a function of the request alone; RETRYABLE is an
// answer that depends on state another request or event can change, or a 5xx.
var problemCatalog = []struct {
	err  error
	info problemInfo
}{
	// 404 on the command's own target: ids are minted here (OrderRepo.NextID,
	// ord-<uuid>) and no order is ever deleted, so a missing id stays missing.
	{usecases.ErrOrderNotFound, problemInfo{"order-not-found", "Order not found", classFinal}},

	{shared.ErrEmptyOrderID, problemInfo{"empty-order-id", "Order id must not be empty", classFinal}},
	{shared.ErrEmptySKU, problemInfo{"empty-sku", "SKU must not be empty", classFinal}},
	{shared.ErrEmptyPathID, problemInfo{"empty-path-id", "Path id must not be empty", classFinal}},
	{shared.ErrInvalidFulfillmentSiteID, problemInfo{"invalid-fulfillment-site-id", "Fulfillment site id must be a site code of uppercase letters and digits", classFinal}},
	// The path catalogue and the product classification are local copies of
	// other contexts' events (ADR 0013, 0036): what they refuse today a later
	// event can admit.
	{shared.ErrUnknownProcessPath, problemInfo{"unknown-process-path", "Resolved process path is not active in the process-path catalogue", classRetryable}},
	{shared.ErrLineIneligibleForResolvedPath, problemInfo{"line-ineligible-for-resolved-path", "Line's attributes are not eligible for its resolved process path", classRetryable}},
	{order.ErrNoLines, problemInfo{"order-without-lines", "An order must have at least one line", classFinal}},
	{order.ErrLineNotFound, problemInfo{"order-line-not-found", "Order line not found", classRetryable}},

	{shared.ErrNonPositiveQuantity, problemInfo{"non-positive-quantity", "Quantity must be greater than zero", classFinal}},
	{order.ErrHeldOrderMustBeShipComplete, problemInfo{"held-order-must-be-ship-complete", "A held order (releaseOnAllocation=false) must be ship-complete", classFinal}},

	{order.ErrOrderAlreadyReleased, problemInfo{"order-already-released", "Order already has released lines and can no longer be cancelled", classRetryable}},
	// order.ErrShipCompleteBlocked (BR3) is deliberately NOT mapped: a
	// ship-complete order whose release is blocked is a HOLD, not an error
	// (ADR 0003/0005). The release leg treats it as "nothing to release", so
	// POST /orders answers 201 and POST .../allocation-retries and
	// POST .../releases answer 200, all
	// with the order Backordered. There is no ship-complete-blocked problem type.
	{order.ErrLineAlreadyAllocated, problemInfo{"order-line-already-allocated", "Order line is already allocated", classRetryable}},
	{order.ErrLineNotPending, problemInfo{"order-line-not-pending", "Order line is not pending allocation", classRetryable}},
	{order.ErrLineNotBackordered, problemInfo{"order-line-not-backordered", "Order line is not backordered", classRetryable}},
	{order.ErrLineNotAllocated, problemInfo{"order-line-not-allocated", "Order line is not allocated", classRetryable}},
	{usecases.ErrNoAllocatedLines, problemInfo{"no-allocated-lines", "Order has no allocated lines to release", classRetryable}},
	{usecases.ErrNoBackorderedLines, problemInfo{"no-backordered-lines", "Order has no backordered lines to retry", classRetryable}},
	{usecases.ErrPromiseDateNotSet, problemInfo{"promise-date-not-set", "Order has no promise date; allocate it first", classRetryable}},
	{usecases.ErrOrderNotHeld, problemInfo{"order-not-held", "Order was not held at intake and has nothing to release on demand", classRetryable}},
	{ports.ErrConcurrentModification, problemInfo{"concurrent-modification", "Order was modified by another request in the meantime; reload and retry", classRetryable}},

	// The downstream Suppliers are not wired up (permissive mode), or an
	// ambiguous transport/5xx failure reached this context. Neither is the
	// caller's fault and neither is a business fact, so both surface as
	// 503 rather than being papered over with a 2xx. A 5xx is never stored.
	// downstream-unavailable is listed BEFORE downstream-not-configured:
	// an open circuit breaker wraps both sentinels, and "unavailable" is
	// the accurate type for it.
	{ports.ErrDownstreamUnavailable, problemInfo{"downstream-unavailable", "A downstream service is unavailable or answered unexpectedly", classRetryable}},
	{ports.ErrDownstreamNotConfigured, problemInfo{"downstream-not-configured", "A downstream service is running in permissive (no-op) mode", classRetryable}},
	{ports.ErrInsufficientStock, problemInfo{"insufficient-stock", "Insufficient usable stock reported by inventory-storage", classRetryable}},
}

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title, class) triple by looking it up in problemCatalog, which mirrors
// statusFor's groupings one-for-one. An unmapped error falls back to the
// generic internal-error problem (a 500, never stored).
func problemFor(err error) problemInfo {
	for _, entry := range problemCatalog {
		if errors.Is(err, entry.err) {
			return entry.info
		}
	}
	return problemInternalError
}
