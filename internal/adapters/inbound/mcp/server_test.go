package mcp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/order-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// newServer builds a real MCP HTTP server over an in-memory repo seeded
// with one order (directly via order.Rehydrate, mirroring the domain's
// own test suite), and returns its httptest URL. The server is mounted
// unauthenticated -- the fleet REST/MCP static-bearer auth layer was
// removed.
func newServer(t *testing.T) string {
	t.Helper()
	orders := memory.NewOrderRepo()
	ctx := context.Background()

	orderID, err := shared.NewOrderId("ORD-1")
	if err != nil {
		t.Fatalf("order id: %v", err)
	}
	resID := "RES-1"
	o := order.Rehydrate(order.OrderSnapshot{ID: orderID, Lines: []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 2, "pick", false, order.LineAllocated, &resID),
	}})
	if err := orders.Save(ctx, o); err != nil {
		t.Fatalf("seed order: %v", err)
	}

	deps := inboundmcp.Deps{
		GetOrder: &usecases.GetOrder{Orders: orders},
	}
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL
}

func connect(t *testing.T, url string) *sdk.ClientSession {
	t.Helper()
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{
		Endpoint:   url,
		HTTPClient: &http.Client{},
	}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestServer_ToolsListAndCall(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	ctx := context.Background()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	want := map[string]bool{"get_order": false}
	for _, tool := range tools.Tools {
		if _, ok := want[tool.Name]; ok {
			want[tool.Name] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("tool %q not advertised", name)
		}
	}
	// This context has no write use case over MCP: no write tool must
	// ever be advertised.
	for _, tool := range tools.Tools {
		if tool.Annotations != nil && !tool.Annotations.ReadOnlyHint {
			t.Errorf("tool %q is not annotated read-only; this context exposes no write tool", tool.Name)
		}
	}

	res, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "get_order",
		Arguments: map[string]any{"orderId": "ORD-1"},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool returned error: %+v", res.Content)
	}
	out, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content: %+v", res.StructuredContent)
	}
	if out["id"] != "ORD-1" {
		t.Fatalf("id = %v, want ORD-1", out["id"])
	}
}

func TestServer_CallToolRejectsUnknownOrder(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{
		Name:      "get_order",
		Arguments: map[string]any{"orderId": "GHOST"},
	})
	if err != nil {
		t.Fatalf("call tool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected tool-level error for an unknown order")
	}
}
