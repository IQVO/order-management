package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// orderResourceURITemplate is the ONE resource this server offers (Agentic
// Standard v2, AS-3): the order read model this context owns, scoped to a
// single order -- the same GetOrder read use case behind get_order, behind a
// URI an agent can load without a tool call. Bounded by construction (one
// order, never a table dump), JSON text.
const orderResourceURITemplate = "warehouse://order-management/orders/{orderId}"

// mimeTypeJSON is the MIME type of every resource this server serves.
const mimeTypeJSON = "application/json"

// registerResources adds the scoped read-model resource. Per the charter
// (docs/docs/mcp/governance-charter.md), resources are bounded-context
// contracts tied to a decision, not bulk dumps: this one answers "what is
// the current state of THIS order" for a specific id, from the same read
// model the get_order tool and the REST surface use.
func (d Deps) registerResources(server *mcp.Server) {
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: orderResourceURITemplate,
		Name:        "order",
		Description: "Current state of one customer order by id: status, allow-partial-shipment flag, promise date and every line's SKU/quantity/process-path/gift-wrap/status/reservation-id -- the system-of-record view this context owns.",
		MIMEType:    mimeTypeJSON,
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		orderID, err := parseOrderResourceURI(req.Params.URI)
		if err != nil {
			return nil, err
		}
		o, err := d.GetOrder.Execute(ctx, orderID)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(toOrderDTO(o))
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      req.Params.URI,
				MIMEType: mimeTypeJSON,
				Text:     string(body),
			}},
		}, nil
	})
}

// parseOrderResourceURI extracts the orderId from a
// warehouse://order-management/orders/{orderId} URI. The segment is
// model-supplied and therefore untrusted; a malformed URI or an empty id is
// rejected rather than silently defaulted. The template's {orderId} variable
// never matches a slash, so the remainder after the prefix is exactly one
// segment.
func parseOrderResourceURI(uri string) (shared.OrderId, error) {
	const prefix = "warehouse://order-management/orders/"
	if !strings.HasPrefix(uri, prefix) {
		return "", fmt.Errorf("resource uri %q must start with %q", uri, prefix)
	}
	id := strings.TrimPrefix(uri, prefix)
	if id == "" || strings.Contains(id, "/") {
		return "", fmt.Errorf("resource uri %q must be warehouse://order-management/orders/{orderId} with a non-empty orderId", uri)
	}
	return shared.NewOrderId(id)
}
