package mcp

import (
	"context"
	"fmt"
	"time"
)

// get_promise_health -- ADR 0014 §6 / ADR 0019's read-only MCP tool over
// the Order Funnel & Allocation Health data product's promise KPIs: promise
// basis distribution, re-promise rate, split-shipment rate, and
// promise-to-cutoff gap, for a requested time window and optional path
// filter.
//
// DESIGN DECISION: this package does NOT import internal/analytics/report
// directly -- internal/architecture/fitness_test.go's
// TestMCPAdapterDependencyRule (ADR-0008) restricts this adapter to depend
// only on internal/application and internal/domain, so the MCP surface
// stays additive and never becomes a second load-bearing consumer of an
// unrelated internal region. Instead, PromiseHealthStore below is a small
// port THIS package owns (mirroring the existing Deps.GetOrder pattern of
// depending on an application-layer use case, here substituted with a
// locally-declared interface + DTO since routing through
// internal/application/usecases would just push the same report.ReportStore
// dependency into a layer that is ALSO arch-test-restricted to
// domain+application only). cmd/mcp -- the composition root, exempt from
// both layering rules by design -- adapts the real
// internal/analytics/report.ReportStore into this interface, translating
// report.Row into PromiseHealthRow at the wiring boundary.
type PromiseHealthStore interface {
	// QueryPromiseHealth returns the promise-KPI-bearing rows for the
	// window [from, to) and optional exact-match pathId filter (empty
	// means no filter), at hour granularity -- the same query shape
	// report.ReportQuery already uses.
	QueryPromiseHealth(ctx context.Context, from, to time.Time, pathId string) ([]PromiseHealthRow, error)
}

// PromiseHealthRow is this package's OWN primitive-typed projection of one
// analytics funnel row's promise KPI fields (report.Row, translated by
// cmd/mcp) -- never the report package's own struct, to keep this package
// free of that dependency per PromiseHealthStore's doc comment.
type PromiseHealthRow struct {
	PathID                    string
	HourBucket                time.Time
	PromiseBasisCapability    int
	PromiseBasisLeadTime      int
	PromiseBasisNetwork       int
	OrdersRepromised          int
	OrdersSplitShipment       int
	PromiseToCutoffGapSeconds float64
	PromiseToCutoffGapSamples int
}

// promiseHealthInput is the tool's request shape -- the SAME from/to/pathId
// fields ReportsHandlers.GetFunnel accepts over REST, so a caller filtering
// this tool and the REST funnel report by the same window gets consistent
// answers.
type promiseHealthInput struct {
	From   string `json:"from" jsonschema:"inclusive start of the time window, RFC3339"`
	To     string `json:"to" jsonschema:"exclusive end of the time window, RFC3339"`
	PathId string `json:"pathId,omitempty" jsonschema:"optional exact-match process path filter"`
}

// promiseHealthDTO is the tool's output: the aggregated promise KPI summary
// across every funnel row in the requested window/path.
type promiseHealthDTO struct {
	// PromiseBasisCapability/PromiseBasisLeadTime/PromiseBasisNetwork/
	// OrdersAllocatedTotal are the promise basis distribution: how many
	// allocation-outcome events in the window carried a real
	// capability-derived CPT promise, the LeadTimePolicy fallback, or a
	// promise DICTATED by an external deadline (ADR 0020, "Network" basis —
	// kept separate so promises we never chose do not dilute the KPIs that
	// measure the ones we did).
	PromiseBasisCapability int `json:"promiseBasisCapability"`
	PromiseBasisLeadTime   int `json:"promiseBasisLeadTime"`
	PromiseBasisNetwork    int `json:"promiseBasisNetwork"`
	OrdersAllocatedTotal   int `json:"ordersAllocatedTotal"`

	// OrdersSplitShipment/SplitShipmentRate: how many orders promised their
	// lines to more than one cutoff (ADR 0014 §3 / ADR 0017), and that as a
	// fraction of OrdersAllocatedTotal.
	OrdersSplitShipment int     `json:"ordersSplitShipment"`
	SplitShipmentRate   float64 `json:"splitShipmentRate"`

	// OrdersRepromised/RepromiseRate: how many orders had their promise
	// moved by the ADR 0018 feedback loop. OrdersRepromised carries NO
	// path dimension (see report.Row.OrdersRepromised's doc comment) --
	// filtering PathId to a real path makes this always zero, which is
	// documented on the tool description, not a silent surprise.
	OrdersRepromised int     `json:"ordersRepromised"`
	RepromiseRate    float64 `json:"repromiseRate"`

	// PromiseToCutoffGapSeconds: the sample-weighted mean of
	// (cutoffAt - allocatedAt) across every Capability-basis promise in the
	// window -- how far in advance of the actual departure the promise was
	// typically made.
	PromiseToCutoffGapSeconds float64 `json:"promiseToCutoffGapSeconds"`
}

// aggregatePromiseHealth folds report rows into one promiseHealthDTO. A
// package-level function (not a method) so it is directly unit-testable
// against hand-built PromiseHealthRow slices without a store.
func aggregatePromiseHealth(rows []PromiseHealthRow) promiseHealthDTO {
	var (
		out             promiseHealthDTO
		gapWeightedSum  float64
		gapSampleWeight int
	)
	for _, row := range rows {
		out.PromiseBasisCapability += row.PromiseBasisCapability
		out.PromiseBasisLeadTime += row.PromiseBasisLeadTime
		out.PromiseBasisNetwork += row.PromiseBasisNetwork
		out.OrdersSplitShipment += row.OrdersSplitShipment
		out.OrdersRepromised += row.OrdersRepromised
		if row.PromiseToCutoffGapSamples > 0 {
			gapWeightedSum += row.PromiseToCutoffGapSeconds * float64(row.PromiseToCutoffGapSamples)
			gapSampleWeight += row.PromiseToCutoffGapSamples
		}
	}

	out.OrdersAllocatedTotal = out.PromiseBasisCapability + out.PromiseBasisLeadTime + out.PromiseBasisNetwork
	if out.OrdersAllocatedTotal > 0 {
		out.SplitShipmentRate = float64(out.OrdersSplitShipment) / float64(out.OrdersAllocatedTotal)
		out.RepromiseRate = float64(out.OrdersRepromised) / float64(out.OrdersAllocatedTotal)
	}
	if gapSampleWeight > 0 {
		out.PromiseToCutoffGapSeconds = gapWeightedSum / float64(gapSampleWeight)
	}
	return out
}

// getPromiseHealth is the tool handler: parse from/to, query the store, and
// aggregate.
func (d Deps) getPromiseHealth(ctx context.Context, in promiseHealthInput) (promiseHealthDTO, error) {
	from, err := time.Parse(time.RFC3339, in.From)
	if err != nil {
		return promiseHealthDTO{}, fmt.Errorf("from must be an RFC3339 timestamp: %w", err)
	}
	to, err := time.Parse(time.RFC3339, in.To)
	if err != nil {
		return promiseHealthDTO{}, fmt.Errorf("to must be an RFC3339 timestamp: %w", err)
	}

	rows, err := d.PromiseHealth.QueryPromiseHealth(ctx, from, to, in.PathId)
	if err != nil {
		return promiseHealthDTO{}, err
	}

	return aggregatePromiseHealth(rows), nil
}
