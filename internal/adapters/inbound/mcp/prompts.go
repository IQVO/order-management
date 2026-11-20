package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// investigateOrderSOP is the operational standard-operating-procedure the
// investigate_order prompt hands to the model. Per the charter, prompts
// encode how to interpret results, when to escalate, and what "done" means —
// they standardise agent behaviour across clients rather than leaving
// procedure implicit. The prompt has no arguments and performs no writes
// (this surface has none): it may NAME tools, never call them.
const investigateOrderSOP = `You are helping a warehouse operator answer "where does this order stand, and is its promise healthy?" for order-management, the system of record for customer orders. Use only this context's MCP tools and resources; never assume state. This surface is read-only by design: receiving, allocating, retrying, releasing and cancelling orders are lifecycle commands with business invariants a human owns — report and recommend, never try to act.

Procedure:
1. Read the order with get_order(orderId), or load the resource warehouse://order-management/orders/{orderId}. Report the derived order status (Received, Allocated, PartiallyAllocated, Backordered, Released, PartiallyReleased or Cancelled), the allow-partial-shipment flag, the promise date, and every line's SKU, quantity, process path, gift-wrap flag, status and reservation id.
2. Interpret the lines: a line carrying a reservation id is Allocated (stock held by inventory-storage); Backordered means no usable stock was available at allocation time and the line waits for a retry; Released lines have handed off to fulfillment-execution; Pending lines have not been through allocation yet.
3. When the promise slipped, lines sit backordered, or the order was split across shipments, call get_promise_health(from, to) for the covering window [from, to) — optionally with an exact-match pathId — to see whether promises were capability-derived CPT windows, LeadTimePolicy fallbacks or network-dictated deadlines, the fleet-wide re-promise rate, the split-shipment rate and the mean promise-to-cutoff gap. A pathId filter scopes every KPI except the re-promise rate, which is fleet-wide by definition.
4. Do not attempt to re-promise, retry allocation or release anything from here: this context offers no write over MCP, and those commands belong to operators through the surfaces that own them.

Interpretation:
- Backordered lines under a capability-derived promise usually point at upstream stock, not a promise error; a rising LeadTime or network promise basis in the KPIs means the promise engine is falling back.
- on-time-to-CPT is NOT one of this context's KPIs: it is measured in fulfillment-execution, where the evidence (cpt vs manifested_at) lives.

Escalate to a human when: an order is still backordered after its promise date has passed, a promise has moved more than once without an explanation, or covering the order needs a policy decision (expedite, split, substitute). Those are operator calls — this context makes the order legible, it does not decide.

Done means: for the order you were asked about you have reported its status, every line's state and reservation, whether its promise is healthy in the window, and any escalation with a one-line reason.`

// registerPrompts adds the workflow prompt (the operator SOP).
func (d Deps) registerPrompts(server *mcp.Server) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "investigate_order",
		Description: "Standard operating procedure for investigating one order: read its state, interpret line statuses, check promise health when a promise slipped, and know when to escalate to a human.",
	}, func(ctx context.Context, _ *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{
			Description: "How to investigate an order: read its state and lines, interpret the statuses, check promise health when relevant, and escalate policy calls to a human.",
			Messages: []*mcp.PromptMessage{{
				Role:    "user",
				Content: &mcp.TextContent{Text: investigateOrderSOP},
			}},
		}, nil
	})
}
