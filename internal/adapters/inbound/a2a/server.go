// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same application-layer read use case and the same analytics read port the
// HTTP and MCP adapters use. It is built on the official A2A Go SDK
// (a2aproject/a2a-go) and speaks JSON-RPC 2.0 over HTTP, with the Agent Card
// served at the well-known path.
//
// Per ADR-0046, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel -- facts between contexts still travel exclusively
// as CloudEvents on Kafka (ADR-0030).
//
// READ-ONLY BY DESIGN. order-management is the system of record for customer
// orders, and its MCP surface (ADR-0010) deliberately offers no write: placing,
// cancelling, allocating and releasing an order are lifecycle commands with
// real business invariants and are not decisions a delegating agent makes on
// this context's behalf. The WRITE GATE of the A2A rollout says a skill may
// change state only if the MCP surface already offers that write, so this
// agent has no write skill and Deps carries no write use case (a fitness test
// pins that).
//
// Like the MCP adapter (ADR-0008), this package depends inward on the
// application layer and the domain only -- never on an outbound adapter and
// never on the MCP adapter. The composition root (cmd/a2a) wires the concrete
// repository and analytics store. Domain structs never leak across the message
// boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/order-management/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent Card)
// and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the SAME read use case the HTTP and MCP adapters use for
// get_order plus the analytics read port behind get_promise_health; the
// adapter never constructs an outbound adapter itself.
//
// No write use case (ReceiveOrder, CancelOrder, RetryAllocation,
// ReleaseHeldOrder) is wired here, by the write gate described in the package
// documentation.
type Deps struct {
	// GetOrder is the existing read use case behind order_status, reused
	// unchanged.
	GetOrder *usecases.GetOrder
	// PromiseHealth backs promise_health: a port THIS package declares
	// (promise_health.go), not internal/analytics/report directly, for the
	// same dependency-rule reason as the MCP adapter (ADR-0008). cmd/a2a
	// adapts the real analytics store into it.
	PromiseHealth PromiseHealthStore
}

// Card builds this agent's Agent Card (ADR-0046). The surface is two curated,
// goal-shaped read skills -- not one skill per endpoint, the same curation
// discipline ADR-0008 applied to MCP tools. order_status answers "where is
// this order"; promise_health answers "how good are our delivery promises in
// this window". The card declares no security schemes on purpose: this
// surface is unauthenticated by the same fleet-wide decision as REST and MCP
// (ADR-0012); schemes are added here, not in a redesign, if that decision is
// ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "order-management",
		Description: "Order Management agent for the Warehouse Execution System, the system of record for customer orders. READ-ONLY: it answers where one order stands (status, promise date and every line's SKU, quantity, allocation and release progress) and how healthy the delivery promises have been over a time window (capability vs lead-time vs network promise basis, re-promise and split-shipment rates, promise-to-cutoff gap). It never places, cancels, allocates or releases an order.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/order-management",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillOrderStatus,
				Name:        "Where does an order stand",
				Description: "Current state of one order by id: status, allow-partial-shipment flag, promise date and every line's SKU, quantity, process path, gift-wrap, status and reservation id (present only once the line is allocated). Use it for allocation, backorder, release or cancellation progress. An unknown id is answered as a 404 problem.",
				Tags:        []string{"orders", "status", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"order_status","arguments":{"orderId":"ORD-1"}}`,
				},
			},
			{
				ID:          skillPromiseHealth,
				Name:        "How healthy are our delivery promises",
				Description: "Promise-health KPIs for a time window [from, to) (RFC3339) and an optional exact-match pathId, from the Order Funnel & Allocation Health data product: promise basis distribution (capability-derived CPT window, LeadTimePolicy fallback, or an external network deadline), re-promise rate (fleet-wide, NOT path-scoped: a pathId filter zeroes it), split-shipment rate and the mean promise-to-cutoff gap in seconds. Does NOT include on-time-to-CPT, which lives in fulfillment-execution's own analytics. The data product is eventually consistent.",
				Tags:        []string{"orders", "promise", "analytics", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"promise_health","arguments":{"from":"2026-10-01T00:00:00Z","to":"2026-10-08T00:00:00Z"}}`,
					`{"skill":"promise_health","arguments":{"from":"2026-10-01T00:00:00Z","to":"2026-10-08T00:00:00Z","pathId":"pick"}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are request/response reads, so there is no
// long-running Task lifecycle to track in this first A2A surface. The response
// carries the caller's contextId (and taskId when the client referenced one)
// so a delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler -- the only transport this
// agent ships (ADR-0046), mirroring ADR-0008's "Streamable HTTP only"
// minimalism for MCP.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
