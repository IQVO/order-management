package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/order-management/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card). Both are reads.
const (
	skillOrderStatus   = "order_status"
	skillPromiseHealth = "promise_health"
)

var (
	// errReportStore marks a failure of the analytics read port.
	errReportStore = errors.New("analytics report store failed")
	// errPromiseHealthNotWired marks a Deps built without the analytics port.
	errPromiseHealthNotWired = errors.New("promise_health is not wired")
)

// skillRequest is the wire contract of every skill call: the first meaningful
// message part carries (as a Data part, or a Text part holding the same JSON)
// the skill id and its arguments. Strict on purpose -- an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored: a
// misspelled name like "pathid" must not silently drop the filter. (Go's
// encoding/json matches field names case-insensitively, so "pathid" still
// reaches pathId; only genuinely unknown names are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// orderStatusArgs is the order_status argument shape.
type orderStatusArgs struct {
	OrderID string `json:"orderId"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR-0046).
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, p := parseSkillRequest(ec)
	if p != nil {
		return reply(ec, envelope{Problem: p}, p.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, summary, p := run(ctx, deps, req)
	if p != nil {
		span.SetStatus(codes.Error, p.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: p}, p.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, summary)
}

// run dispatches one validated request to its skill and returns the data plus
// its one-line summary.
func run(ctx context.Context, deps Deps, req skillRequest) (*skillDTO, string, *problem) {
	switch req.Skill {
	case skillOrderStatus:
		data, p := deps.orderStatus(ctx, req.Arguments)
		if p != nil {
			return nil, "", p
		}
		return &skillDTO{OrderStatus: &data}, orderSummary(data), nil
	case skillPromiseHealth:
		data, p := deps.promiseHealthSkill(ctx, req.Arguments)
		if p != nil {
			return nil, "", p
		}
		return &skillDTO{PromiseHealth: &data}, promiseHealthSummary(data), nil
	default:
		return nil, "", problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first
// meaningful part. A Data part (or a Text part holding the same JSON object) is
// accepted; anything else is a request-shape problem, answered -- not raised --
// because a malformed delegation is a business answer for the calling agent,
// not a transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields and
// trailing data. Absent arguments decode as the empty object so each skill's
// own required-field checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// orderStatus is the single-order read skill: it runs the SAME GetOrder use
// case the REST and MCP surfaces use.
func (d Deps) orderStatus(ctx context.Context, rawArgs json.RawMessage) (orderDTO, *problem) {
	var args orderStatusArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return orderDTO{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	orderID, err := shared.NewOrderId(args.OrderID)
	if err != nil {
		return orderDTO{}, problemFromUseCase(err)
	}
	o, err := d.GetOrder.Execute(ctx, orderID)
	if err != nil {
		return orderDTO{}, problemFromUseCase(err)
	}
	return toOrderDTO(o), nil
}

// promiseHealthSkill decodes the arguments strictly and runs promiseHealth.
func (d Deps) promiseHealthSkill(ctx context.Context, rawArgs json.RawMessage) (promiseHealthDTO, *problem) {
	var args promiseHealthArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return promiseHealthDTO{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	return d.promiseHealth(ctx, args)
}

// problemFromUseCase maps an error to a problem answer, using the SAME
// classification and problem types as the REST adapter
// (internal/adapters/inbound/http statusFor/problemCatalog) so a delegating
// agent sees the outcome a REST caller would:
//
//   - usecases.ErrOrderNotFound -> 404 order-not-found
//   - shared.ErrEmptyOrderID -> 400 empty-order-id
//   - anything else -> 500 internal-error
//
// The 409/422/503 classes of the REST mapping belong to write commands and the
// downstream Suppliers (inventory-storage); a read skill cannot reach them, so
// they are deliberately not copied here (dead, untestable branches). Unlike
// the REST reports route, the 500 detail is a fixed sentence and the cause is
// logged: a delegating agent must not receive raw database text.
func problemFromUseCase(err error) *problem {
	switch {
	case errors.Is(err, usecases.ErrOrderNotFound):
		return &problem{Type: problemTypeBase + "order-not-found", Title: "Order not found", Detail: err.Error(), Status: 404}
	case errors.Is(err, shared.ErrEmptyOrderID):
		return &problem{Type: problemTypeBase + "empty-order-id", Title: "Order id must not be empty", Detail: "orderId is required", Status: 400}
	default:
		slog.Error("a2a skill failed", "error", err)
		return &problem{Type: problemTypeBase + "internal-error", Title: "An unexpected internal error occurred", Detail: "the skill failed; see server logs (trace id in the response metadata)", Status: 500}
	}
}
