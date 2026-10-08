package inventorystorage

import (
	"bytes"
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

// DefaultTimeout bounds a single call to inventory-storage, so a slow or
// hanging Supplier does not stall AllocateOrder indefinitely.
const DefaultTimeout = 5 * time.Second

// IdempotencyKeyHeader is the header inventory-storage's own
// RequireIdempotencyKey middleware (mirrored from this repo's ADR 0023)
// requires on POST /reservations — see idempotencyKeyFor's doc comment
// for how this client derives a value that satisfies its exact replay
// contract.
const IdempotencyKeyHeader = "Idempotency-Key"

// idempotencyKeyFor derives the Idempotency-Key this client sends on
// POST /reservations, from req.DemandRef (the OrderId), req.LineNo, and
// req.Attempt — see ports.ReservationRequest's doc comment for what each
// of those means and why they compose into exactly the "same key for a
// retry of THIS attempt, different key for a genuinely new attempt"
// shape inventory-storage's middleware expects (read against its own
// idempotency.go / idempotency_integration_test.go: a replayed key +
// identical body returns the cached response with no second
// reservation; the same key with a DIFFERENT body is 422
// idempotency-key-reused). Deterministic and side-effect-free — calling
// it twice with the same ReservationRequest always yields the same
// string, by construction, with no random/UUID component: this client
// must reproduce the SAME key across an actual network retry of the
// same outbound call (e.g. a transport-level retry that never reaches
// this method a second time anyway, since the header is set once on the
// same *http.Request) as well as across two genuinely independent Go
// calls to Reserve for the same (order, line, attempt) — which only
// happens today if a caller retries Reserve directly rather than going
// through allocateLines' one-call-per-line loop, but the derivation is
// safe for that case too, by construction.
func idempotencyKeyFor(req ports.ReservationRequest) string {
	return fmt.Sprintf("res-%s-line-%d-att-%d", req.DemandRef.String(), req.LineNo, req.Attempt)
}

// ErrUnexpectedStatus wraps an inventory-storage response status this
// client has no specific handling for. It is deliberately NOT
// ports.ErrInsufficientStock: only a 409 means "no usable stock", and
// anything else must fail AllocateOrder outright rather than be recorded
// as a backorder.
var ErrUnexpectedStatus = errors.New("inventory-storage: unexpected response status")

// unavailable tags a transport/timeout/decode failure of a call to
// inventory-storage as ports.ErrDownstreamUnavailable while keeping the
// cause (context.Canceled / DeadlineExceeded, net errors) reachable
// through errors.Is/As — the circuit breaker's own IsExcluded check
// relies on the latter. The inbound HTTP adapter answers 503 for it.
func unavailable(cause error) error {
	return fmt.Errorf("%w: %w", ports.ErrDownstreamUnavailable, cause)
}

// HTTPDoer is the subset of *http.Client this adapter depends on, so unit
// tests can substitute a fake transport without a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is a plain net/http implementation of
// ports.InventoryReservationClient, calling inventory-storage's published
// contract:
//
//	POST   /reservations       -> 201 with the reservation, or 409 (RFC 7807)
//	                              when there is not enough usable stock
//	DELETE /reservations/{id}  -> 204
//
// It imports nothing from the inventory-storage module: the request and
// response shapes below are local mirrors of that service's published
// wire contract (see ADR 0002).
type Client struct {
	baseURL string
	doer    HTTPDoer
}

// NewClient builds a Client against baseURL (from INVENTORY_STORAGE_BASE_URL).
// A nil doer defaults to an *http.Client with DefaultTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

// reserveRequest mirrors inventory-storage's ReserveStockRequest.
type reserveRequest struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demandRef"`
	// LineNo is the order line this reservation is for (decision 18,
	// ADR 0037). Optional and additive: nil (omitted) means "unknown".
	// inventory-storage's handler decodes with a plain json.Decoder, so
	// a body carrying it is accepted whether or not that service has
	// shipped the field yet.
	LineNo *int `json:"lineNo,omitempty"`
}

// lineNoField returns the wire value of lineNo: the line number when it
// is a real one (>= 1), nil otherwise so the field is omitted.
func lineNoField(lineNo int) *int {
	if lineNo < 1 {
		return nil
	}
	return &lineNo
}

// reservationResponse mirrors inventory-storage's Reservation response.
// Only `id` is consumed: this context stores the reference and nothing
// else, because inventory-storage owns reservation state.
type reservationResponse struct {
	ID string `json:"id"`
}

// Reserve calls POST /reservations for one order line.
//
//   - 201 -> the reservation id.
//   - 409 -> ports.ErrInsufficientStock, the business fact that maps to a
//     Backordered line.
//   - anything else, including any transport error -> a hard error.
func (c *Client) Reserve(ctx context.Context, req ports.ReservationRequest) (ports.ReservationResult, error) {
	body, err := json.Marshal(reserveRequest{
		SKU:       req.SKU.String(),
		Quantity:  req.Quantity,
		DemandRef: req.DemandRef.String(),
		LineNo:    lineNoField(req.LineNo),
	})
	if err != nil {
		return ports.ReservationResult{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/reservations", bytes.NewReader(body))
	if err != nil {
		return ports.ReservationResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set(IdempotencyKeyHeader, idempotencyKeyFor(req))

	resp, err := c.doer.Do(httpReq)
	if err != nil {
		return ports.ReservationResult{}, unavailable(err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
		var decoded reservationResponse
		if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
			return ports.ReservationResult{}, unavailable(err)
		}
		if decoded.ID == "" {
			return ports.ReservationResult{}, fmt.Errorf("%w: %w: 2xx response carried no reservation id", ports.ErrDownstreamUnavailable, ErrUnexpectedStatus)
		}
		return ports.ReservationResult{ReservationID: decoded.ID}, nil
	case http.StatusConflict:
		return ports.ReservationResult{}, ports.ErrInsufficientStock
	default:
		return ports.ReservationResult{}, fmt.Errorf("%w: %w: %d", ports.ErrDownstreamUnavailable, ErrUnexpectedStatus, resp.StatusCode)
	}
}

// RevokeReservation calls DELETE /reservations/{id}.
//
// No Idempotency-Key header is sent here: inventory-storage's
// RequireIdempotencyKey middleware is wired ONLY onto POST /stock/receive
// and POST /reservations (confirmed by reading that service's own
// server.go route table — DELETE /reservations/{id} is registered plain,
// with no r.With(RequireIdempotencyKey(...)) wrapper). DELETE is already
// idempotent by ordinary HTTP semantics on this specific route, which is
// exactly why inventory-storage never gated it: deleting an
// already-deleted (or never-existing) reservation id has no
// double-creation risk to protect against, unlike POST.
//
// A 404 is treated as success: the reservation this context wanted gone is
// gone. That is idempotence, not fail-open — the desired end state holds
// either way, so a cancellation retry after a partial failure converges
// instead of deadlocking.
func (c *Client) RevokeReservation(ctx context.Context, reservationID string) error {
	endpoint := fmt.Sprintf("%s/reservations/%s", c.baseURL, url.PathEscape(reservationID))

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.doer.Do(httpReq)
	if err != nil {
		return unavailable(err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("%w: %w: %d", ports.ErrDownstreamUnavailable, ErrUnexpectedStatus, resp.StatusCode)
	}
}
