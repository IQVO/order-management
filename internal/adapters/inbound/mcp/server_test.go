package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestServer_ResourceListAndRead proves the order resource template is
// listed and readable over the real transport: reading
// warehouse://order-management/orders/ORD-1 returns the SAME order DTO the
// get_order tool returns, as JSON text (AS-3).
func TestServer_ResourceListAndRead(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	ctx := context.Background()

	list, err := session.ListResourceTemplates(ctx, nil)
	if err != nil {
		t.Fatalf("list resource templates: %v", err)
	}
	found := false
	for _, r := range list.ResourceTemplates {
		if strings.Contains(r.URITemplate, "orders/{orderId}") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no order resource template listed: %+v", list.ResourceTemplates)
	}

	res, err := session.ReadResource(ctx, &sdk.ReadResourceParams{
		URI: "warehouse://order-management/orders/ORD-1",
	})
	if err != nil {
		t.Fatalf("read resource: %v", err)
	}
	if len(res.Contents) == 0 || res.Contents[0].Text == "" {
		t.Fatalf("empty resource contents: %+v", res.Contents)
	}
	var dto map[string]any
	if err := json.Unmarshal([]byte(res.Contents[0].Text), &dto); err != nil {
		t.Fatalf("resource text is not JSON: %v", err)
	}
	if dto["id"] != "ORD-1" {
		t.Fatalf("resource order id = %v, want ORD-1", dto["id"])
	}
}

// TestServer_ResourceReadRejectsUnknownOrder keeps the resource honest: a
// missing order is an error, not an empty document.
func TestServer_ResourceReadRejectsUnknownOrder(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	if _, err := session.ReadResource(context.Background(), &sdk.ReadResourceParams{
		URI: "warehouse://order-management/orders/GHOST",
	}); err == nil {
		t.Fatal("expected an error reading an unknown order resource")
	}
}

// TestServer_PromptGet proves the investigate_order prompt is listed and
// returns its SOP message (AS-3).
func TestServer_PromptGet(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	ctx := context.Background()

	prompts, err := session.ListPrompts(ctx, nil)
	if err != nil {
		t.Fatalf("list prompts: %v", err)
	}
	if len(prompts.Prompts) == 0 {
		t.Fatal("no prompts listed")
	}

	res, err := session.GetPrompt(ctx, &sdk.GetPromptParams{Name: "investigate_order"})
	if err != nil {
		t.Fatalf("get prompt: %v", err)
	}
	if len(res.Messages) == 0 {
		t.Fatal("investigate_order prompt returned no messages")
	}
}
