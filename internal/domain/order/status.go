// Package order holds the Order aggregate root, its OrderLine entities,
// the order/line status model, and the promise-date policy. It is pure Go:
// no HTTP, no SQL, no framework types.
package order

// LineStatus is the per-line lifecycle state.
//
//	Pending -> Allocated  (AllocateOrder, once inventory-storage reserves)
//	Pending -> Backordered (inventory-storage answered 409: no usable stock)
//	Backordered -> Allocated (ONLY via RetryAllocation)
//	Allocated -> Released (ReleaseOrder, once wes-work-planning accepts work)
//	Released -> Shipped (Ship, once outbound-shipping confirms the load left; ADR 0051)
//	any pre-release state -> Cancelled (CancelOrder)
type LineStatus string

const (
	LinePending     LineStatus = "Pending"
	LineAllocated   LineStatus = "Allocated"
	LineBackordered LineStatus = "Backordered"
	LineReleased    LineStatus = "Released"
	LineShipped     LineStatus = "Shipped"
	LineCancelled   LineStatus = "Cancelled"
)

// Status is the order-level state. It is ALWAYS derived from the line
// statuses (see Order.Status) and is never stored as a redundant field
// that could drift out of sync with the lines it summarises.
type Status string

const (
	StatusReceived           Status = "Received"
	StatusAllocated          Status = "Allocated"
	StatusPartiallyAllocated Status = "PartiallyAllocated"
	StatusBackordered        Status = "Backordered"
	StatusReleased           Status = "Released"
	StatusPartiallyReleased  Status = "PartiallyReleased"
	StatusShipped            Status = "Shipped"
	StatusPartiallyShipped   Status = "PartiallyShipped"
	StatusCancelled          Status = "Cancelled"
)
