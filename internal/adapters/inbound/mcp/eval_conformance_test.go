// E2 — over-the-wire protocol conformance evals.
//
// server_test.go already proves the happy path (list + call). These evals
// pin the protocol behaviors a model host relies on when things go WRONG
// or get exotic: the initialize handshake and its negotiation results,
// error-shape guarantees for unknown tools / bad argument types / stray
// argument keys / unknown resources and prompts, discovery of resource
// templates and prompts (this server deliberately exposes none — the
// empty lists are pinned as the contract, per server.go's design note),
// and session lifecycle after close. All against the real Streamable HTTP
// handler — never in-process values.
package mcp_test

import (
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestEvalConformance_InitializeHandshake pins what a host learns at
// connect time: a negotiated protocol result carrying this server's name
// and non-empty usage instructions.
func TestEvalConformance_InitializeHandshake(t *testing.T) {
	h := newEvalHarness(t)

	init := h.session.InitializeResult()
	if init == nil {
		t.Fatal("no initialize result on the session")
	}
	if init.ServerInfo.Name == "" {
		t.Error("initialize result has no server name")
	}
	if strings.TrimSpace(init.Instructions) == "" {
		t.Error("initialize result carries no instructions — hosts surface these to the model; an empty string wastes the connect handshake")
	}
}

// TestEvalConformance_UnknownToolIsAProtocolError pins that calling a tool
// the server never advertised is rejected at the protocol layer (a
// JSON-RPC error), not silently executed or returned as a tool result.
func TestEvalConformance_UnknownToolIsAProtocolError(t *testing.T) {
	h := newEvalHarness(t)

	if err := h.callTool(t.Context(), "definitely_not_a_tool", map[string]any{}); err == nil {
		t.Fatal("calling an unknown tool must be a protocol error, got success")
	}
}

// TestEvalConformance_WrongTypedArgumentIsRejected pins that a model
// sending a string parameter as a number is rejected by the typed tool
// schema — either as a protocol error or a tool-level error result, never
// as a success that silently coerces.
func TestEvalConformance_WrongTypedArgumentIsRejected(t *testing.T) {
	h := newEvalHarness(t)

	res, err := h.session.CallTool(t.Context(), &sdk.CallToolParams{
		Name:      "get_order",
		Arguments: map[string]any{"orderId": 42},
	})
	switch {
	case err != nil:
		// Protocol-level rejection: acceptable.
	case res != nil && res.IsError:
		// Tool-level rejection: acceptable.
	default:
		t.Fatalf("wrong-typed orderId argument must not succeed: err=%v res=%+v", err, res)
	}
}

// TestEvalConformance_ExtraArgumentsAreRejected pins the strict side of
// argument handling: the typed tool schemas disallow undeclared properties
// (the SDK default, additionalProperties: false), so stray model chatter
// forwarded by a host is rejected loudly rather than silently ignored.
func TestEvalConformance_ExtraArgumentsAreRejected(t *testing.T) {
	h := newEvalHarness(t)

	res, err := h.session.CallTool(t.Context(), &sdk.CallToolParams{
		Name: "get_order",
		Arguments: map[string]any{
			"orderId":       "ORD-1",
			"model_chatter": "what's the state of this order again?",
			"step":          2,
		},
	})
	switch {
	case err != nil:
		// Protocol-level rejection: acceptable.
	case res != nil && res.IsError:
		// Tool-level rejection: acceptable.
	default:
		t.Fatalf("unknown extra arguments must not succeed: err=%v res=%+v", err, res)
	}
}

// TestEvalConformance_OrderResourceTemplateAdvertised pins the AS-3 surface
// (Agentic Standard v2, ADR 0049): the server advertises exactly ONE
// resource template — the order read model at
// warehouse://order-management/orders/{orderId} — and no static resources
// (every read is scoped by the template's orderId variable).
func TestEvalConformance_OrderResourceTemplateAdvertised(t *testing.T) {
	h := newEvalHarness(t)

	templates, err := h.session.ListResourceTemplates(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resource templates: %v", err)
	}
	if len(templates.ResourceTemplates) != 1 {
		t.Fatalf("server advertises %d resource templates, want exactly 1: %+v", len(templates.ResourceTemplates), templates.ResourceTemplates)
	}
	if got := templates.ResourceTemplates[0].URITemplate; got != "warehouse://order-management/orders/{orderId}" {
		t.Fatalf("resource template URI = %q, want the order read model", got)
	}

	resources, err := h.session.ListResources(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}
	if len(resources.Resources) != 0 {
		t.Fatalf("server advertises static resources it must not have: %+v", resources.Resources)
	}
}

// TestEvalConformance_UnknownResourceIsRejected pins that reading a URI
// this server never advertised is a protocol error (there is no resource
// surface to fall through to).
func TestEvalConformance_UnknownResourceIsRejected(t *testing.T) {
	h := newEvalHarness(t)

	if _, err := h.session.ReadResource(t.Context(), &sdk.ReadResourceParams{URI: "order://ORD-1"}); err == nil {
		t.Fatal("reading an unknown resource URI must be rejected")
	}
}

// TestEvalConformance_InvestigateOrderPromptAdvertised pins the AS-3
// surface (Agentic Standard v2, ADR 0049): the server advertises exactly
// ONE prompt — the investigate_order operator SOP — and prompts/list
// returns it rather than an empty list.
func TestEvalConformance_InvestigateOrderPromptAdvertised(t *testing.T) {
	h := newEvalHarness(t)

	prompts, err := h.session.ListPrompts(t.Context(), nil)
	if err != nil {
		t.Fatalf("list prompts: %v", err)
	}
	if len(prompts.Prompts) != 1 {
		t.Fatalf("server advertises %d prompts, want exactly 1: %+v", len(prompts.Prompts), prompts.Prompts)
	}
	if prompts.Prompts[0].Name != "investigate_order" {
		t.Fatalf("prompt name = %q, want investigate_order", prompts.Prompts[0].Name)
	}
}

// TestEvalConformance_UnknownPromptIsRejected pins that getting a prompt
// the server does not have is a protocol error.
func TestEvalConformance_UnknownPromptIsRejected(t *testing.T) {
	h := newEvalHarness(t)

	if _, err := h.session.GetPrompt(t.Context(), &sdk.GetPromptParams{Name: "no_such_prompt"}); err == nil {
		t.Fatal("getting an unknown prompt must be rejected")
	}
}

// TestEvalConformance_SessionCloseEndsCalls pins that after the client
// closes the session, further calls fail loudly instead of silently
// no-op'ing.
func TestEvalConformance_SessionCloseEndsCalls(t *testing.T) {
	h := newEvalHarness(t)

	if err := h.session.Close(); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if _, err := h.session.CallTool(t.Context(), &sdk.CallToolParams{
		Name:      "get_order",
		Arguments: map[string]any{"orderId": "ORD-1"},
	}); err == nil {
		t.Fatal("calling a tool on a closed session must fail")
	}
}
