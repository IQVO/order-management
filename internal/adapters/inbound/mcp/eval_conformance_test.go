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

// TestEvalConformance_NoResourceTemplatesAdvertised pins this server's
// deliberate zero-resource surface: order-management's only meaningful
// read is one order by id, which the get_order tool already covers (a
// resource template would be the same lookup behind a second, redundant
// surface — server.go's design note). A host therefore discovers NO
// resource templates, and that emptiness is the contract.
func TestEvalConformance_NoResourceTemplatesAdvertised(t *testing.T) {
	h := newEvalHarness(t)

	templates, err := h.session.ListResourceTemplates(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resource templates: %v", err)
	}
	if len(templates.ResourceTemplates) != 0 {
		t.Fatalf("server advertises resource templates it must not have: %+v", templates.ResourceTemplates)
	}

	resources, err := h.session.ListResources(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}
	if len(resources.Resources) != 0 {
		t.Fatalf("server advertises resources it must not have: %+v", resources.Resources)
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

// TestEvalConformance_NoPromptsAdvertised pins this server's deliberate
// zero-prompt surface: there is no multi-step operational SOP here worth a
// prompt (server.go's design note), so prompts/list returns an empty list
// rather than an error.
func TestEvalConformance_NoPromptsAdvertised(t *testing.T) {
	h := newEvalHarness(t)

	prompts, err := h.session.ListPrompts(t.Context(), nil)
	if err != nil {
		t.Fatalf("list prompts: %v", err)
	}
	if len(prompts.Prompts) != 0 {
		t.Fatalf("server advertises prompts it must not have: %+v", prompts.Prompts)
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
