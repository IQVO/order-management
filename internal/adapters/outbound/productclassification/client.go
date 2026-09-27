// Package productclassification provides outbound
// ports.ProductClassificationLookup implementations: an HTTP client that
// calls inventory-storage's product-classification endpoint, and a
// permissive no-op used by default so existing tests, CI and deployments
// are unaffected. This mirrors wes-work-planning's own
// internal/adapters/outbound/productclassification package (its ADR-0009)
// exactly in shape and intent -- order-management never imports another
// service's Go packages (see .claude/rules/bounded-context-boundary.md),
// so this is an independently-written adapter calling the same real
// inventory-storage contract, not a copy of that package's code.
//
// Selected via PRODUCT_CLASSIFICATION_MODE=http|permissive (default
// "permissive"), matching wes-work-planning's exact env-var convention
// and this repo's own INVENTORY_STORAGE_MODE=http|permissive pattern for
// its InventoryReservationClient.
//
// Unlike InventoryReservationClient's PermissiveClient (which fails LOUD
// on every call because reserving real stock must never appear to
// succeed against a no-op), PermissiveLookup here fails OPEN: a
// classification lookup is a soft, optional routing/enrichment input to
// order.PathSelectionPolicy, not a mutation of real state, so a missing
// or failed lookup must never block order intake -- see this fleet's
// "fail loud for anything that mutates real state, fail quiet/open for a
// soft enrichment input" rule (order-management ADR-0016).
package productclassification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
)

// DefaultTimeout bounds a single classification lookup request, so a slow
// or hanging inventory-storage does not stall order intake indefinitely.
const DefaultTimeout = 5 * time.Second

// ErrUnexpectedStatus wraps an inventory-storage response status this
// client does not have specific handling for (anything other than 200 or
// 404). Client.GetClassification itself never returns this to
// ReceiveOrder -- see the package doc comment's fail-open rule -- it
// exists so a caller/test can distinguish "the client saw something
// unexpected" from "the client saw a network error" if it chooses to,
// exactly as inventory-storage's own ErrUnexpectedStatus does for
// Reserve.
var ErrUnexpectedStatus = errors.New("inventory-storage: unexpected response status")

// HTTPDoer is the subset of *http.Client this adapter depends on, so unit
// tests can substitute a fake transport without a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is a plain net/http implementation of
// ports.ProductClassificationLookup, calling inventory-storage's
// GET /products/{sku}/classification (apis/openapi.yaml's
// getProductClassification operation there).
type Client struct {
	baseURL string
	doer    HTTPDoer
}

// NewClient builds a Client against baseURL (from
// INVENTORY_STORAGE_BASE_URL -- the SAME env var this repo's existing
// InventoryReservationClient already uses; there is deliberately no
// second base-URL knob for the same downstream service). A nil doer
// defaults to an *http.Client with DefaultTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

// classificationResponse mirrors inventory-storage's real
// ProductClassification schema (apis/openapi.yaml). temperatureClass and
// dotHazardClass are intentionally omitted from this DTO -- this port
// only needs handlingTags (see ports.ProductClassification's own doc
// comment on why this is a narrower view than inventory-storage's full
// resource).
type classificationResponse struct {
	SKU          string   `json:"sku"`
	HandlingTags []string `json:"handlingTags"`
}

// GetClassification calls inventory-storage's product-classification
// endpoint for sku.
//
//   - A 404 is treated as Known=false (fail-open / permissive): that SKU
//     has no registered classification yet.
//   - Any transport error or non-2xx/404 status ALSO returns Known=false
//     with a nil error -- unlike inventory-storage's own placement checks,
//     a classification-lookup problem here must never block or reject
//     order intake, it only omits the derived routing hint (see the
//     package doc comment).
//
// This is a thin fail-open wrapper around fetch -- see BreakerClient
// (breaker.go) for the retrying, circuit-broken caller that needs to
// observe fetch's real errors instead of having them swallowed here.
func (c *Client) GetClassification(ctx context.Context, sku string) (ports.ProductClassification, error) {
	result, err := c.fetch(ctx, sku)
	if err != nil {
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	}
	return result, nil
}

// fetch is Client's raw call to inventory-storage's product-
// classification endpoint, with NO fail-open conversion: a transport
// error, a malformed 200 body, or any status other than 200/404 is
// returned as a real error. A 404 is a legitimate business fact (this
// SKU has no registered classification yet), not a failure -- it returns
// Known=false with a nil error, exactly like a 200 does for Known=true,
// so a caller (BreakerClient's retry/breaker logic) never retries or
// trips its breaker on an ordinary unclassified SKU.
func (c *Client) fetch(ctx context.Context, sku string) (ports.ProductClassification, error) {
	endpoint := fmt.Sprintf("%s/products/%s/classification", c.baseURL, url.PathEscape(sku))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ports.ProductClassification{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.doer.Do(req)
	if err != nil {
		return ports.ProductClassification{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var body classificationResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return ports.ProductClassification{}, err
		}
		return ports.ProductClassification{SKU: sku, HandlingTags: body.HandlingTags, Known: true}, nil
	case http.StatusNotFound:
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	default:
		return ports.ProductClassification{}, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}
}
