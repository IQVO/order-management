package a2a

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// promise_health -- the A2A twin of the MCP get_promise_health tool (ADR 0014
// §6 / ADR 0019): the promise KPIs of the Order Funnel & Allocation Health data
// product for a window and an optional path filter.
//
// DESIGN DECISION (same as the MCP adapter's, ADR-0008): this package does NOT
// import internal/analytics/report directly, and it cannot import the MCP
// adapter either (nothing may depend on it). PromiseHealthStore is therefore a
// small port THIS package owns; cmd/a2a -- the composition root, exempt from
// the layering rules by design -- adapts the real report.ReportStore into it.
// The ~40-line aggregation below mirrors the MCP adapter's; the two are pinned
// to the same numbers by their own table tests.

// PromiseHealthStore is the read port behind promise_health.
type PromiseHealthStore interface {
	// QueryPromiseHealth returns the promise-KPI-bearing rows for the window
	// [from, to) and optional exact-match pathId filter (empty means no
	// filter), at hour granularity -- the same query shape report.ReportQuery
	// uses.
	QueryPromiseHealth(ctx context.Context, from, to time.Time, pathID string) ([]PromiseHealthRow, error)
}

// PromiseHealthRow is this package's OWN primitive-typed projection of one
// analytics funnel row's promise KPI fields, translated by cmd/a2a.
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

// promiseHealthArgs is the skill's argument shape -- the same from/to/pathId
// the REST funnel report and the MCP tool accept.
type promiseHealthArgs struct {
	From   string `json:"from"`
	To     string `json:"to"`
	PathID string `json:"pathId"`
}

// promiseHealthDTO is the skill's data: the aggregated promise KPI summary
// across every funnel row in the requested window/path.
type promiseHealthDTO struct {
	// PromiseBasis*: how many allocation outcomes in the window carried a real
	// capability-derived CPT promise, the LeadTimePolicy fallback, or a promise
	// DICTATED by an external deadline (ADR 0020, kept separate so promises we
	// never chose do not dilute the KPIs that measure the ones we did).
	PromiseBasisCapability int `json:"promiseBasisCapability"`
	PromiseBasisLeadTime   int `json:"promiseBasisLeadTime"`
	PromiseBasisNetwork    int `json:"promiseBasisNetwork"`
	OrdersAllocatedTotal   int `json:"ordersAllocatedTotal"`

	// OrdersSplitShipment/SplitShipmentRate: orders promised to more than one
	// cutoff (ADR 0017), and that as a fraction of OrdersAllocatedTotal.
	OrdersSplitShipment int     `json:"ordersSplitShipment"`
	SplitShipmentRate   float64 `json:"splitShipmentRate"`

	// OrdersRepromised/RepromiseRate: orders whose promise moved (ADR 0018).
	// The re-promise count carries NO path dimension: a pathId filter makes it
	// zero, which the skill description states.
	OrdersRepromised int     `json:"ordersRepromised"`
	RepromiseRate    float64 `json:"repromiseRate"`

	// PromiseToCutoffGapSeconds: the sample-weighted mean of (cutoffAt -
	// allocatedAt) across every Capability-basis promise in the window.
	PromiseToCutoffGapSeconds float64 `json:"promiseToCutoffGapSeconds"`
}

// aggregatePromiseHealth folds report rows into one promiseHealthDTO.
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

// promiseHealthSummary is the one-line text form of a promise_health answer.
func promiseHealthSummary(d promiseHealthDTO) string {
	return fmt.Sprintf("%d order(s) allocated in the window (capability %d, lead-time %d, network %d); split-shipment rate %.3f, re-promise rate %.3f, mean promise-to-cutoff gap %.0fs",
		d.OrdersAllocatedTotal, d.PromiseBasisCapability, d.PromiseBasisLeadTime, d.PromiseBasisNetwork,
		d.SplitShipmentRate, d.RepromiseRate, d.PromiseToCutoffGapSeconds)
}

// parseWindow validates the required RFC3339 from/to. The wording is owned by
// this adapter (the underlying time.ParseError text is not echoed).
func parseWindow(args promiseHealthArgs) (from, to time.Time, p *problem) {
	from, p = parseRequiredTime("from", args.From)
	if p != nil {
		return time.Time{}, time.Time{}, p
	}
	to, p = parseRequiredTime("to", args.To)
	if p != nil {
		return time.Time{}, time.Time{}, p
	}
	return from, to, nil
}

func parseRequiredTime(name, raw string) (time.Time, *problem) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, problemInvalidArgument(name + " is required (RFC3339)")
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, problemInvalidArgument(name + " must be an RFC3339 timestamp")
	}
	return t, nil
}

// promiseHealth is the analytics read skill. A store failure is an internal
// failure, classified exactly as the REST reports adapter does (500).
func (d Deps) promiseHealth(ctx context.Context, args promiseHealthArgs) (promiseHealthDTO, *problem) {
	from, to, p := parseWindow(args)
	if p != nil {
		return promiseHealthDTO{}, p
	}
	if d.PromiseHealth == nil {
		return promiseHealthDTO{}, problemFromUseCase(errPromiseHealthNotWired)
	}
	rows, err := d.PromiseHealth.QueryPromiseHealth(ctx, from, to, args.PathID)
	if err != nil {
		return promiseHealthDTO{}, problemFromUseCase(fmt.Errorf("%w: %w", errReportStore, err))
	}
	return aggregatePromiseHealth(rows), nil
}
