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
		errors.Is(err, order.ErrNoLines),
		errors.Is(err, order.ErrLineNotFound):
		return http.StatusBadRequest

	case errors.Is(err, shared.ErrNonPositiveQuantity),
		errors.Is(err, shared.ErrLineIneligibleForResolvedPath),
		errors.Is(err, order.ErrHeldOrderMustBeShipComplete):
		return http.StatusUnprocessableEntity

	case errors.Is(err, order.ErrOrderAlreadyReleased),
		errors.Is(err, order.ErrShipCompleteBlocked),
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
	// ambiguous transport/5xx failure reached this context. Neither is the
	// caller's fault and neither is a business fact, so both surface as
	// 503 rather than being papered over with a 2xx.
	case errors.Is(err, ports.ErrDownstreamNotConfigured),
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
// 7807 problem response. slug becomes the last path segment of "type";
// title is a fixed human string for the category (the dynamic detail comes
// from err.Error() at write time, not from this table).
type problemInfo struct {
	slug  string
	title string
}

// problemCatalog maps each typed domain/application error to its fixed
// RFC 7807 (type, title) pair, mirroring statusFor's groupings
// one-for-one. Entries are checked with errors.Is in order, so this
// sequence preserves the original switch's first-match precedence
// exactly.
var problemCatalog = []struct {
	err  error
	info problemInfo
}{
	{usecases.ErrOrderNotFound, problemInfo{"order-not-found", "Order not found"}},

	{shared.ErrEmptyOrderID, problemInfo{"empty-order-id", "Order id must not be empty"}},
	{shared.ErrEmptySKU, problemInfo{"empty-sku", "SKU must not be empty"}},
	{shared.ErrEmptyPathID, problemInfo{"empty-path-id", "Path id must not be empty"}},
	{shared.ErrUnknownProcessPath, problemInfo{"unknown-process-path", "Resolved process path is not active in the process-path catalogue"}},
	{shared.ErrLineIneligibleForResolvedPath, problemInfo{"line-ineligible-for-resolved-path", "Line's attributes are not eligible for its resolved process path"}},
	{order.ErrNoLines, problemInfo{"order-without-lines", "An order must have at least one line"}},
	{order.ErrLineNotFound, problemInfo{"order-line-not-found", "Order line not found"}},

	{shared.ErrNonPositiveQuantity, problemInfo{"non-positive-quantity", "Quantity must be greater than zero"}},
	{order.ErrHeldOrderMustBeShipComplete, problemInfo{"held-order-must-be-ship-complete", "A held order (releaseOnAllocation=false) must be ship-complete"}},

	{order.ErrOrderAlreadyReleased, problemInfo{"order-already-released", "Order already has released lines and can no longer be cancelled"}},
	{order.ErrShipCompleteBlocked, problemInfo{"ship-complete-blocked", "Ship-complete order cannot be released while any line is unallocated"}},
	{order.ErrLineAlreadyAllocated, problemInfo{"order-line-already-allocated", "Order line is already allocated"}},
	{order.ErrLineNotPending, problemInfo{"order-line-not-pending", "Order line is not pending allocation"}},
	{order.ErrLineNotBackordered, problemInfo{"order-line-not-backordered", "Order line is not backordered"}},
	{order.ErrLineNotAllocated, problemInfo{"order-line-not-allocated", "Order line is not allocated"}},
	{usecases.ErrNoAllocatedLines, problemInfo{"no-allocated-lines", "Order has no allocated lines to release"}},
	{usecases.ErrNoBackorderedLines, problemInfo{"no-backordered-lines", "Order has no backordered lines to retry"}},
	{usecases.ErrPromiseDateNotSet, problemInfo{"promise-date-not-set", "Order has no promise date; allocate it first"}},
	{usecases.ErrOrderNotHeld, problemInfo{"order-not-held", "Order was not held at intake and has nothing to release on demand"}},
	{ports.ErrConcurrentModification, problemInfo{"concurrent-modification", "Order was modified by another request in the meantime; reload and retry"}},

	// The downstream Suppliers are not wired up (permissive mode), or an
	// ambiguous transport/5xx failure reached this context. Neither is the
	// caller's fault and neither is a business fact, so both surface as
	// 503 rather than being papered over with a 2xx.
	{ports.ErrDownstreamNotConfigured, problemInfo{"downstream-not-configured", "A downstream service is running in permissive (no-op) mode"}},
	{ports.ErrInsufficientStock, problemInfo{"insufficient-stock", "Insufficient usable stock reported by inventory-storage"}},
}

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title) pair by looking it up in problemCatalog, which mirrors
// statusFor's groupings one-for-one. An unmapped error falls back to
// the generic internal-error pair.
func problemFor(err error) problemInfo {
	for _, entry := range problemCatalog {
		if errors.Is(err, entry.err) {
			return entry.info
		}
	}
	return problemInfo{"internal-error", "An unexpected internal error occurred"}
}
