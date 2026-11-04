package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer builds the MCP server for this bounded context with the
// get_order and get_promise_health read tools, the order resource template
// and the investigate_order prompt registered.
//
// order-management exposes no write use case over MCP: CancelOrder,
// ReceiveOrder, Allocate and RetryAllocate are all order-lifecycle
// commands with real business invariants (EnsureCancellable,
// EnsureReleasable) that this context's own domain enforces -- none of
// them is a decision an MCP-calling agent should make on this context's
// behalf, so none is wired here.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "order-management-mcp", Version: "1.0.0"},
		&mcp.ServerOptions{
			Instructions: "Read-only access to order-management: look up one order's current state (status, lines, allocation/release/cancellation progress) by id, and read promise-health KPIs for a window. Start with the investigate_order prompt.",
		},
	)

	deps.registerTools(server)
	deps.registerResources(server)
	deps.registerPrompts(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server. The
// server is mounted unauthenticated: the fleet-wide REST/MCP static-bearer
// auth layer was removed (see the ADR recorded alongside this change), so
// no Authorization header is required or checked here.
func Handler(server *mcp.Server) http.Handler {
	// Stateless (Agentic Standard v2, AS-4): no Mcp-Session-Id is issued, so
	// the server also answers the 2026-07-28 revision's per-request
	// handshake (server/discover) while legacy initialize clients keep
	// working. Nothing here relies on cross-call session state.
	return mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true},
	)
}
