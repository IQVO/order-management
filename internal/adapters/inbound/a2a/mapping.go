package a2a

import (
	"fmt"

	"github.com/claudioed/order-management/internal/domain/order"
)

// problemTypeBase namespaces this context's problem types. It is the SAME base
// URI the REST adapter uses (internal/adapters/inbound/http/errors.go), so a
// delegating agent sees the same problem types a REST caller would.
const problemTypeBase = "https://errors.order-management.warehouse-systems.dev/"

// problem is the RFC 7807-shaped rejection an agent client receives instead of
// a result: domain rejections and request-shape failures are business answers,
// not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidArgument(detail string) *problem {
	return &problem{Type: problemTypeBase + "invalid-argument", Title: "Invalid skill argument", Detail: detail, Status: 400}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to (empty when the request named none).
type envelope struct {
	Skill   string    `json:"skill"`
	Data    *skillDTO `json:"data,omitempty"`
	Problem *problem  `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field, keeping the envelope
// flat and versioned as one contract.
type skillDTO struct {
	OrderStatus   *orderDTO         `json:"orderStatus,omitempty"`
	PromiseHealth *promiseHealthDTO `json:"promiseHealth,omitempty"`
}

// orderLineDTO is one line of an orderDTO. It is this adapter's OWN copy of
// the read-model projection (the MCP adapter has its own too): this package
// must never import the MCP or HTTP adapters.
type orderLineDTO struct {
	LineNo   int    `json:"lineNo"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	PathID   string `json:"pathId"`
	GiftWrap bool   `json:"giftWrap"`
	Status   string `json:"status"`
	// ReservationID is inventory-storage's id, present only once the line is
	// allocated. Omitted rather than sent as "" so "not allocated" is
	// unambiguous to a calling agent.
	ReservationID *string `json:"reservationId,omitempty"`
}

// orderDTO is the order_status skill's data.
type orderDTO struct {
	ID                   string         `json:"id"`
	Status               string         `json:"status"`
	AllowPartialShipment bool           `json:"allowPartialShipment"`
	PromiseDate          *string        `json:"promiseDate,omitempty"`
	Lines                []orderLineDTO `json:"lines"`
}

// toOrderDTO maps the aggregate into the DTO; the domain type never crosses
// the boundary.
func toOrderDTO(o *order.Order) orderDTO {
	lines := o.Lines()
	dtoLines := make([]orderLineDTO, 0, len(lines))
	for _, l := range lines {
		dtoLines = append(dtoLines, orderLineDTO{
			LineNo:        l.LineNo(),
			SKU:           string(l.SKU()),
			Quantity:      l.Quantity(),
			PathID:        string(l.PathID()),
			GiftWrap:      l.GiftWrap(),
			Status:        string(l.Status()),
			ReservationID: l.ReservationID(),
		})
	}

	dto := orderDTO{
		ID:                   string(o.ID()),
		Status:               string(o.Status()),
		AllowPartialShipment: o.AllowPartialShipment(),
		Lines:                dtoLines,
	}
	if pd := o.PromiseDate(); pd != nil {
		formatted := pd.UTC().Format("2006-01-02T15:04:05Z07:00")
		dto.PromiseDate = &formatted
	}
	return dto
}

// orderSummary is the one-line text form of an order_status answer.
func orderSummary(d orderDTO) string {
	s := fmt.Sprintf("order %s is %s with %d line(s)", d.ID, d.Status, len(d.Lines))
	if d.PromiseDate != nil {
		s += ", promised " + *d.PromiseDate
	}
	return s
}
