// Package http is the inbound chi adapter: DTOs, handlers, routing, and
// domain-error-to-HTTP-status mapping. Domain structs never cross this
// boundary — every response below is a DTO owned by this package.
package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type receiveOrderLineRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	GiftWrap bool   `json:"giftWrap,omitempty"`
}

// UnmarshalJSON rejects an explicit JSON null on any of this DTO's known
// fields. None of them is nullable in apis/openapi.yaml, and Go's decoder
// would otherwise silently coerce `null` to the field's zero value (a null
// giftWrap or quantity quietly becoming false/0) — accepting a
// schema-violating request as if it were valid. The error surfaces through
// decodeJSON's existing 400 problem+json path, exactly like any other
// type-mismatched body.
func (r *receiveOrderLineRequest) UnmarshalJSON(data []byte) error {
	if err := rejectNullFields(data, "sku", "quantity", "giftWrap"); err != nil {
		return err
	}
	type alias receiveOrderLineRequest
	return json.Unmarshal(data, (*alias)(r))
}

type receiveOrderRequest struct {
	Lines []receiveOrderLineRequest `json:"lines"`
	// AllowPartialShipment defaults to false — ship-complete (BR3).
	AllowPartialShipment bool `json:"allowPartialShipment,omitempty"`
	// ReleaseOnAllocation is a POINTER so an absent field is
	// distinguishable from an explicit false. ADR 0020 §1 makes the
	// default true, which is the opposite of Go's zero value: a plain
	// bool would silently HOLD every order from every existing caller
	// that never sends the field. nil means true.
	ReleaseOnAllocation *bool `json:"releaseOnAllocation,omitempty"`
	// RequiredShipBy is an externally-dictated deadline (ADR 0020 §2).
	// When present, the promise is CONSTRAINED to a window at or before
	// it rather than chosen as the earliest this service can manage, and
	// an order that cannot make it comes back with NO promiseDate —
	// which is the answer a caller holding a fill-or-kill commitment
	// actually needs.
	RequiredShipBy *time.Time `json:"requiredShipBy,omitempty"`
}

// UnmarshalJSON rejects an explicit JSON null on any of this DTO's known
// fields, mirroring receiveOrderLineRequest's rationale: none of them is
// nullable in apis/openapi.yaml, and the zero-value coercion Go would
// otherwise perform (a null allowPartialShipment silently becoming false,
// a null releaseOnAllocation being indistinguishable from an omitted one)
// accepts schema-violating requests as if they were valid.
func (r *receiveOrderRequest) UnmarshalJSON(data []byte) error {
	if err := rejectNullFields(data, "lines", "allowPartialShipment", "releaseOnAllocation", "requiredShipBy"); err != nil {
		return err
	}
	type alias receiveOrderRequest
	return json.Unmarshal(data, (*alias)(r))
}

// rejectNullFields reports an error when any of the named fields is
// explicitly JSON null in the raw object. It is deliberately limited to
// the DTO's own known fields: apis/openapi.yaml leaves
// additionalProperties at its default (allowed), so unknown properties —
// even null-valued ones — must stay ignored, and a null-valued UNKNOWN
// field must not fail decoding.
func rejectNullFields(data []byte, fields ...string) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for _, f := range fields {
		v, ok := raw[f]
		if !ok {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fmt.Errorf("json: field %q must not be null", f)
		}
	}
	return nil
}

type orderLineResponse struct {
	LineNo   int    `json:"lineNo"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	// PathID reflects an internally-assigned value (see
	// shared.DefaultPathId / shared.NewPathIdOrDefault) — a caller placing
	// an order has no business supplying wes-work-planning's process-path
	// vocabulary at intake, so this is read-only on the wire: it can be
	// seen in every response but never set on the request.
	PathID   string `json:"pathId"`
	GiftWrap bool   `json:"giftWrap"`
	Status   string `json:"status"`
	// ReservationID is inventory-storage's id, present only once the line
	// is allocated. Omitted rather than sent as "" so "not allocated" is
	// unambiguous on the wire.
	ReservationID *string `json:"reservationId,omitempty"`
}

type orderResponse struct {
	ID                   string              `json:"id"`
	Status               string              `json:"status"`
	AllowPartialShipment bool                `json:"allowPartialShipment"`
	ReleaseOnAllocation  bool                `json:"releaseOnAllocation"`
	RequiredShipBy       *time.Time          `json:"requiredShipBy,omitempty"`
	PromiseDate          *string             `json:"promiseDate,omitempty"`
	Lines                []orderLineResponse `json:"lines"`
}

// problemDetails is the RFC 7807 (Problem Details for HTTP APIs) response
// body used for every error response in this service — the same shape the
// other five services in this fleet emit.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}
