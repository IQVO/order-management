// Package shared holds the value objects, domain events, and error types
// common to every aggregate in the Order Management domain.
package shared

import "errors"

var (
	// ErrEmptyOrderID is returned when an OrderId is constructed from an
	// empty string. An order without an identity cannot be referenced by
	// inventory-storage (as a demandRef) or by wes-work-planning (as a
	// work-unit reference), so the domain refuses to create one.
	ErrEmptyOrderID = errors.New("order id must not be empty")

	// ErrEmptySKU is returned when a SKU is constructed from an empty
	// string. Every OrderLine must say what is being ordered.
	ErrEmptySKU = errors.New("sku must not be empty")

	// ErrInvalidFulfillmentSiteID is returned when a caller-assigned
	// fulfillment site is empty or is not an uppercase-letters-and-digits
	// site code (ADR 0044). A caller-facing input error (RFC 7807 400).
	ErrInvalidFulfillmentSiteID = errors.New("fulfillment site id must be a non-empty site code of uppercase letters and digits")

	// ErrEmptyPathID is returned when a PathId is constructed from an
	// empty string. Callers that omit a path get DefaultPathId instead of
	// this error — see NewPathIdOrDefault.
	ErrEmptyPathID = errors.New("path id must not be empty")

	// ErrNonPositiveQuantity is returned when an OrderLine quantity is
	// zero or negative. Ordering nothing is not a business fact worth
	// persisting.
	ErrNonPositiveQuantity = errors.New("quantity must be greater than zero")

	// ErrUnknownProcessPath is returned by ReceiveOrder when a line's
	// resolved PathId (via order.PathSelectionPolicy) is not currently
	// active in process-path-management's real catalogue. This is a
	// caller-facing input error (RFC 7807 400), not an infrastructure
	// failure: it means the order, as constructed, cannot be routed
	// anywhere real right now. See order-management ADR-0013 -- this
	// closes the gap where an invalid path was previously discovered
	// only by wes-work-planning, one saga step after the caller already
	// received a 201.
	ErrUnknownProcessPath = errors.New("resolved process path is not active in the process-path catalogue")

	// ErrLineIneligibleForResolvedPath is returned by ReceiveOrder when a
	// line's resolved path (in this phase, always shared.DefaultPathId --
	// see order.PathSelectionPolicy's doc comment for the honest v1 scope
	// limitation: this repo has no catalogue "list all paths" method yet,
	// so there is nowhere else to route an ineligible line to) is active
	// in the catalogue but its declared Eligibility explicitly rejects
	// this line: the requested quantity exceeds MaxUnitsPerLine, a
	// required product attribute is missing, or an excluded product
	// attribute is present. This is a caller-facing input error (RFC 7807
	// 422 -- the line is well-formed but cannot be fulfilled by any path
	// this policy currently knows how to route to), not an infrastructure
	// failure. See order-management ADR-0016.
	ErrLineIneligibleForResolvedPath = errors.New("line's attributes are not eligible for its resolved process path")
)
