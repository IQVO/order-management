// Shared harness for the MCP eval suites (E1-E3): one place that builds a
// real Streamable HTTP server over in-memory adapters and connects a real
// SDK client to it, so schema evals, wire conformance evals, and the
// Gherkin behavioral evals all exercise exactly the surface a model host
// would.
package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/order-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/analytics/report"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// evalHarness is a fully wired MCP server over in-memory repos plus the
// client session talking to it, with the knobs the evals assert on.
type evalHarness struct {
	session         *sdk.ClientSession
	server          *httptest.Server
	orders          *memory.OrderRepo
	reports         *analyticsstore.MemoryStore
	lastCallResult  *sdk.CallToolResult
	lastCallErr     error
	lastCallContent string
}

// evalPromiseStore adapts the analytics MemoryStore test double into this
// adapter's own PromiseHealthStore port — the same translation cmd/mcp's
// reportStoreAdapter and tools_test.go's reportStoreTestAdapter perform,
// duplicated here because the eval suite is an external test package.
type evalPromiseStore struct {
	store *analyticsstore.MemoryStore
}

func (a evalPromiseStore) QueryPromiseHealth(ctx context.Context, from, to time.Time, pathId string) ([]inboundmcp.PromiseHealthRow, error) {
	rep, err := a.store.Query(ctx, report.ReportQuery{
		From:        from,
		To:          to,
		PathId:      pathId,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]inboundmcp.PromiseHealthRow, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, inboundmcp.PromiseHealthRow{
			PathID:                    row.Key.PathId,
			HourBucket:                row.Key.HourBucket,
			PromiseBasisCapability:    row.PromiseBasisCapability,
			PromiseBasisLeadTime:      row.PromiseBasisLeadTime,
			PromiseBasisNetwork:       row.PromiseBasisNetwork,
			OrdersRepromised:          row.OrdersRepromised,
			OrdersSplitShipment:       row.OrdersSplitShipment,
			PromiseToCutoffGapSeconds: row.PromiseToCutoffGapSeconds,
			PromiseToCutoffGapSamples: row.PromiseToCutoffGapSamples,
		})
	}
	return rows, nil
}

// evalWindowBase is the hour the canonical promise-health analytics are
// seeded at (see newEvalHarness); the eval scenarios query around it.
var evalWindowBase = time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)

// newEvalDeps builds the default tool surface over empty in-memory
// adapters — enough for schema and conformance evals that do not seed
// state. Both of this repo's tools (get_order, get_promise_health) are
// registered unconditionally by Deps.registerTools — unlike
// inventory-storage, whose report tool appears only when
// REPORTS_BASE_URL is set, there is no conditionally-registered tool
// here, so this IS the deployed surface.
func newEvalDeps() inboundmcp.Deps {
	orders := memory.NewOrderRepo()
	return inboundmcp.Deps{
		GetOrder:      &usecases.GetOrder{Orders: orders},
		PromiseHealth: evalPromiseStore{store: analyticsstore.NewMemoryStore()},
	}
}

// newEvalHarness seeds the canonical eval state over a real Streamable
// HTTP server and connects a client session to it:
//
//   - ORD-1: allow-partial, line 1 SKU-1 qty 2 allocated against RES-1,
//     line 2 SKU-2 qty 1 gift-wrapped and backordered (the same shape
//     tools_test.go's canonical order uses, rehydrated via the domain's
//     own constructor rather than ReceiveOrder — how the order came to
//     hold this state is not the MCP adapter's concern);
//   - one hour of promise-health analytics at 2026-09-14T09:00Z: one
//     Capability-basis allocation with a 4h cutoff and a split shipment,
//     one LeadTime-basis allocation, and one fleet-wide re-promise.
func newEvalHarness(t *testing.T) *evalHarness {
	t.Helper()

	h := &evalHarness{}
	h.orders = memory.NewOrderRepo()
	h.reports = analyticsstore.NewMemoryStore()

	ctx := context.Background()
	orderID, err := shared.NewOrderId("ORD-1")
	if err != nil {
		t.Fatalf("order id: %v", err)
	}
	resID := "RES-1"
	o := order.Rehydrate(orderID, []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 2, "pick", false, order.LineAllocated, &resID),
		order.RehydrateOrderLine(2, "SKU-2", 1, "pick", true, order.LineBackordered, nil),
	}, true, nil, nil, nil)
	if err := h.orders.Save(ctx, o); err != nil {
		t.Fatalf("seed order: %v", err)
	}

	cutoff := evalWindowBase.Add(4 * time.Hour)
	for _, apply := range []struct {
		name string
		run  func() error
	}{
		{"capability allocation", func() error {
			return h.reports.ApplyOrderAllocated(ctx, "e1", "pick", evalWindowBase, "Capability", &cutoff, true)
		}},
		{"lead-time allocation", func() error {
			return h.reports.ApplyOrderAllocated(ctx, "e2", "pick", evalWindowBase, "LeadTime", nil, false)
		}},
		{"fleet-wide re-promise", func() error {
			return h.reports.ApplyOrderRepromised(ctx, "e3", evalWindowBase)
		}},
	} {
		if err := apply.run(); err != nil {
			t.Fatalf("seed %s: %v", apply.name, err)
		}
	}

	deps := inboundmcp.Deps{
		GetOrder:      &usecases.GetOrder{Orders: h.orders},
		PromiseHealth: evalPromiseStore{store: h.reports},
	}
	server := inboundmcp.NewServer(deps)
	h.server = httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(h.server.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: h.server.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("eval harness connect: %v", err)
	}
	h.session = session
	t.Cleanup(func() { _ = session.Close() })
	return h
}

// wireSession builds a real Streamable HTTP server over the given deps and
// connects a client session to it, for evals that do not need seeded
// state.
func wireSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: httpSrv.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("wire session connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// callTool invokes a tool and records the result for the Then steps.
func (h *evalHarness) callTool(ctx context.Context, name string, args map[string]any) error {
	res, err := h.session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	h.lastCallResult, h.lastCallErr = res, err
	h.lastCallContent = ""
	if res != nil {
		for _, c := range res.Content {
			if text, ok := c.(*sdk.TextContent); ok {
				h.lastCallContent += text.Text
			}
		}
	}
	return err
}
