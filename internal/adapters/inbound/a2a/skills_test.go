package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/analytics/report"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

var base = time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)

// reportStoreTestAdapter adapts the in-memory report store into this
// package's PromiseHealthStore port, mirroring cmd/a2a's real
// reportStoreAdapter (the production package must not import
// internal/analytics/report -- see PromiseHealthStore).
type reportStoreTestAdapter struct{ store *analyticsstore.MemoryStore }

func (a reportStoreTestAdapter) QueryPromiseHealth(ctx context.Context, from, to time.Time, pathID string) ([]PromiseHealthRow, error) {
	rep, err := a.store.Query(ctx, report.ReportQuery{From: from, To: to, PathId: pathID, Granularity: report.GranularityHour})
	if err != nil {
		return nil, err
	}
	rows := make([]PromiseHealthRow, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, PromiseHealthRow{
			PathID:                    row.Key.PathId,
			HourBucket:                row.Key.HourBucket,
			PromiseBasisCapability:    row.PromiseBasisCapability,
			PromiseBasisLeadTime:      row.PromiseBasisLeadTime,
			PromiseBasisNetwork:       row.PromiseBasisNetwork,
			OrdersRepromised:          row.OrdersRepromised,
			OrdersSplitShipment:       row.OrdersSplitShipment,
			PromiseToCutoffGapSeconds: row.PromiseToCutoffGapSeconds,
			PromiseToCutoffGapSamples: row.PromiseToCutoffGapSamples,
		})
	}
	return rows, nil
}

// harness builds Deps over in-memory repos, seeding directly through the
// domain's Rehydrate constructor -- the same approach as the MCP adapter's
// harness (ReceiveOrder needs real inventory/lead-time collaborators that are
// out of scope for an adapter test).
type harness struct {
	orders  *memory.OrderRepo
	reports *analyticsstore.MemoryStore
	deps    Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	orders := memory.NewOrderRepo()
	reports := analyticsstore.NewMemoryStore()
	return &harness{
		orders:  orders,
		reports: reports,
		deps: Deps{
			GetOrder:      &usecases.GetOrder{Orders: orders},
			PromiseHealth: reportStoreTestAdapter{reports},
		},
	}
}

// seedOrder stores ORD-1: one Allocated line carrying RES-1, one Backordered
// line, and a promise date.
func (h *harness) seedOrder(t *testing.T) {
	t.Helper()
	resID := "RES-1"
	promise := time.Date(2026, 9, 15, 17, 0, 0, 0, time.UTC)
	o := order.Rehydrate(order.OrderSnapshot{
		ID: "ORD-1",
		Lines: []*order.OrderLine{
			order.RehydrateOrderLine(1, "SKU-1", 2, "pick", false, order.LineAllocated, &resID),
			order.RehydrateOrderLine(2, "SKU-2", 1, "pick", true, order.LineBackordered, nil),
		},
		AllowPartialShipment: true,
		PromiseDate:          &promise,
	})
	if err := h.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("seed order: %v", err)
	}
}

func (h *harness) seedFunnel(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	cutoff := base.Add(4 * time.Hour)
	for _, err := range []error{
		h.reports.ApplyOrderAllocated(ctx, "e1", "pick", base, "Capability", &cutoff, true),
		h.reports.ApplyOrderAllocated(ctx, "e2", "pick", base, "LeadTime", nil, false),
		h.reports.ApplyOrderRepromised(ctx, "e3", base),
	} {
		if err != nil {
			t.Fatalf("seed funnel: %v", err)
		}
	}
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  *struct {
		OrderStatus *struct {
			ID                   string  `json:"id"`
			Status               string  `json:"status"`
			AllowPartialShipment bool    `json:"allowPartialShipment"`
			PromiseDate          *string `json:"promiseDate,omitempty"`
			Lines                []struct {
				LineNo        int     `json:"lineNo"`
				SKU           string  `json:"sku"`
				Quantity      int     `json:"quantity"`
				PathID        string  `json:"pathId"`
				GiftWrap      bool    `json:"giftWrap"`
				Status        string  `json:"status"`
				ReservationID *string `json:"reservationId,omitempty"`
			} `json:"lines"`
		} `json:"orderStatus"`
		PromiseHealth *struct {
			PromiseBasisCapability    int     `json:"promiseBasisCapability"`
			PromiseBasisLeadTime      int     `json:"promiseBasisLeadTime"`
			PromiseBasisNetwork       int     `json:"promiseBasisNetwork"`
			OrdersAllocatedTotal      int     `json:"ordersAllocatedTotal"`
			OrdersSplitShipment       int     `json:"ordersSplitShipment"`
			SplitShipmentRate         float64 `json:"splitShipmentRate"`
			OrdersRepromised          int     `json:"ordersRepromised"`
			RepromiseRate             float64 `json:"repromiseRate"`
			PromiseToCutoffGapSeconds float64 `json:"promiseToCutoffGapSeconds"`
		} `json:"promiseHealth"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct{ client *a2aclient.Client }

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope and the
// one-line text summary of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
	if env.Data != nil {
		t.Fatalf("a problem answer must carry no data, got %+v", env.Data)
	}
}

// wantJSON asserts that got marshals to the same JSON document as want
// (key order and whitespace ignored). It compares the whole answer at once, so
// a dropped, renamed or newly added field fails the suite.
func wantJSON(t *testing.T, got any, want string) {
	t.Helper()
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var g, w any
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("answer = %s\nwant   = %s", raw, want)
	}
}

func TestWireOrderStatusReadsTheOrder(t *testing.T) {
	h := newHarness(t)
	h.seedOrder(t)
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillOrderStatus, map[string]any{"orderId": "ORD-1"})
	if env.Problem != nil || env.Data == nil || env.Skill != skillOrderStatus {
		t.Fatalf("envelope = %+v", env)
	}
	// reservationId is present only on the allocated line: "not allocated" is
	// omitted rather than sent as "".
	wantJSON(t, env.Data.OrderStatus, `{
		"id": "ORD-1", "status": "PartiallyAllocated", "allowPartialShipment": true,
		"promiseDate": "2026-09-15T17:00:00Z",
		"lines": [
			{"lineNo": 1, "sku": "SKU-1", "quantity": 2, "pathId": "pick", "giftWrap": false, "status": "Allocated", "reservationId": "RES-1"},
			{"lineNo": 2, "sku": "SKU-2", "quantity": 1, "pathId": "pick", "giftWrap": true, "status": "Backordered"}
		]}`)
	if text != "order ORD-1 is PartiallyAllocated with 2 line(s), promised 2026-09-15T17:00:00Z" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWirePromiseHealthAggregatesTheWindow(t *testing.T) {
	h := newHarness(t)
	h.seedFunnel(t)
	w := newWire(t, h.deps)
	window := map[string]any{"from": base.Add(-time.Hour).Format(time.RFC3339), "to": base.Add(time.Hour).Format(time.RFC3339)}

	env, text := w.skill(t, skillPromiseHealth, window)
	if env.Problem != nil || env.Data == nil || env.Skill != skillPromiseHealth {
		t.Fatalf("envelope = %+v", env)
	}
	wantJSON(t, env.Data.PromiseHealth, `{
		"promiseBasisCapability": 1, "promiseBasisLeadTime": 1, "promiseBasisNetwork": 0, "ordersAllocatedTotal": 2,
		"ordersSplitShipment": 1, "splitShipmentRate": 0.5, "ordersRepromised": 1, "repromiseRate": 0.5,
		"promiseToCutoffGapSeconds": 14400}`)
	if want := "2 order(s) allocated in the window (capability 1, lead-time 1, network 0); split-shipment rate 0.500, re-promise rate 0.500, mean promise-to-cutoff gap 14400s"; text != want {
		t.Fatalf("summary = %q, want %q", text, want)
	}

	// The documented caveat: a pathId filter zeroes the (path-less) re-promise KPI.
	window["pathId"] = "pick"
	env, _ = w.skill(t, skillPromiseHealth, window)
	wantJSON(t, env.Data.PromiseHealth, `{
		"promiseBasisCapability": 1, "promiseBasisLeadTime": 1, "promiseBasisNetwork": 0, "ordersAllocatedTotal": 2,
		"ordersSplitShipment": 1, "splitShipmentRate": 0.5, "ordersRepromised": 0, "repromiseRate": 0,
		"promiseToCutoffGapSeconds": 14400}`)
}

func TestWireDomainRejectionsAreAnsweredNotRaised(t *testing.T) {
	w := newWire(t, newHarness(t).deps)

	env, text := w.skill(t, skillOrderStatus, map[string]any{"orderId": "ORD-NOPE"})
	wantProblem(t, env, 404, "order-not-found")
	if env.Problem.Detail != "order not found" || text != "Order not found (404): order not found" {
		t.Fatalf("detail = %q, summary = %q", env.Problem.Detail, text)
	}

	env, _ = w.skill(t, skillOrderStatus, map[string]any{"orderId": ""})
	wantProblem(t, env, 400, "empty-order-id")
	if env.Problem.Detail != "orderId is required" {
		t.Fatalf("detail = %q", env.Problem.Detail)
	}
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	const from, to = "2026-09-14T08:00:00Z", "2026-09-14T10:00:00Z"
	cases := map[string]struct {
		skill  string
		args   map[string]any
		slug   string
		detail string
	}{
		"order: missing orderId":     {skillOrderStatus, map[string]any{}, "empty-order-id", "orderId is required"},
		"order: unknown argument":    {skillOrderStatus, map[string]any{"orderId": "ORD-1", "includeLines": true}, "invalid-argument", "unknown field"},
		"order: typo'd argument":     {skillOrderStatus, map[string]any{"orderID": "ORD-1", "order_id": "x"}, "invalid-argument", "unknown field"},
		"order: wrong type":          {skillOrderStatus, map[string]any{"orderId": 7}, "invalid-argument", "cannot unmarshal number"},
		"promise: missing from":      {skillPromiseHealth, map[string]any{"to": to}, "invalid-argument", "from is required (RFC3339)"},
		"promise: missing to":        {skillPromiseHealth, map[string]any{"from": from}, "invalid-argument", "to is required (RFC3339)"},
		"promise: blank from":        {skillPromiseHealth, map[string]any{"from": "  ", "to": to}, "invalid-argument", "from is required (RFC3339)"},
		"promise: bad from":          {skillPromiseHealth, map[string]any{"from": "yesterday", "to": to}, "invalid-argument", "from must be an RFC3339 timestamp"},
		"promise: bad to":            {skillPromiseHealth, map[string]any{"from": from, "to": "2026-09-14"}, "invalid-argument", "to must be an RFC3339 timestamp"},
		"promise: unknown argument":  {skillPromiseHealth, map[string]any{"from": from, "to": to, "granularity": "hour"}, "invalid-argument", "unknown field"},
		"promise: typo'd pathId key": {skillPromiseHealth, map[string]any{"from": from, "to": to, "pathIds": "pick"}, "invalid-argument", "unknown field"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, text := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 400, tc.slug)
			if !strings.Contains(env.Problem.Detail, tc.detail) {
				t.Fatalf("detail = %q, want it to contain %q", env.Problem.Detail, tc.detail)
			}
			if env.Skill != tc.skill || !strings.HasPrefix(text, env.Problem.Title+" (400): ") {
				t.Fatalf("skill = %q, summary = %q", env.Skill, text)
			}
		})
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	w := newWire(t, newHarness(t).deps)

	env, _ := w.call(t, a2a.NewTextPart("not json at all"))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillOrderStatus, "arguments": map[string]any{}, "extra": 1}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"order_status","arguments":{}} {"trailing":true}`))
	wantProblem(t, env, 400, "malformed-skill-request")
	if !strings.Contains(env.Problem.Detail, "unexpected data after the JSON document") {
		t.Fatalf("trailing data must be named: %q", env.Problem.Detail)
	}

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillOrderStatus}))
	wantProblem(t, env, 400, "empty-order-id") // absent arguments decode as {}

	// Every state-changing verb of this context is unknown here: read-only agent.
	for _, verb := range []string{"place_order", "receive_order", "cancel_order", "retry_allocation", "release_order", "launch_rocket"} {
		env, _ = w.skill(t, verb, map[string]any{})
		wantProblem(t, env, 400, "unknown-skill")
	}

	// A text part holding the same JSON is accepted (documented contract).
	h := newHarness(t)
	h.seedOrder(t)
	tw := newWire(t, h.deps)
	env, _ = tw.call(t, a2a.NewTextPart(`{"skill":"order_status","arguments":{"orderId":"ORD-1"}}`))
	if env.Problem != nil || env.Data == nil || env.Data.OrderStatus == nil {
		t.Fatalf("a text part holding the skill JSON must be accepted, got %+v", env)
	}
}

// failingOrders makes every read fail with a database-flavoured error.
type failingOrders struct{ ports.OrderRepo }

func (failingOrders) FindByID(context.Context, shared.OrderId) (*order.Order, error) {
	return nil, errors.New("pq: password authentication failed for user secret-db-user")
}

type failingStore struct{}

func (failingStore) QueryPromiseHealth(context.Context, time.Time, time.Time, string) ([]PromiseHealthRow, error) {
	return nil, errors.New("dial tcp 10.0.0.9:5432: connect: connection refused")
}

// TestWireInternalFailuresAreAnswered500WithoutLeakingCauses covers the 500
// class of the REST mapping: a store failure is answered in-band, and the raw
// cause (which may name hosts or users) never reaches the calling agent.
func TestWireInternalFailuresAreAnswered500WithoutLeakingCauses(t *testing.T) {
	h := newHarness(t)
	cases := map[string]struct {
		deps  Deps
		skill string
		args  map[string]any
	}{
		"orders repo":     {Deps{GetOrder: &usecases.GetOrder{Orders: failingOrders{}}}, skillOrderStatus, map[string]any{"orderId": "ORD-1"}},
		"analytics store": {Deps{PromiseHealth: failingStore{}}, skillPromiseHealth, map[string]any{"from": "2026-09-14T08:00:00Z", "to": "2026-09-14T10:00:00Z"}},
		"port not wired":  {Deps{GetOrder: h.deps.GetOrder}, skillPromiseHealth, map[string]any{"from": "2026-09-14T08:00:00Z", "to": "2026-09-14T10:00:00Z"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, text := newWire(t, tc.deps).skill(t, tc.skill, tc.args)
			wantProblem(t, env, 500, "internal-error")
			for _, s := range []string{env.Problem.Detail, env.Problem.Title, text} {
				for _, leak := range []string{"secret-db-user", "10.0.0.9", "pq:", "dial tcp"} {
					if strings.Contains(s, leak) {
						t.Fatalf("an answer leaks %q: %q", leak, s)
					}
				}
			}
		})
	}
}

// TestSkillSpanCarriesTheSkillAttribute pins the OTel contract: one span per
// skill call named "a2a.skill <id>" with an a2a.skill attribute, mirroring
// the MCP adapter's "mcp.tool <name>" span.
func TestSkillSpanCarriesTheSkillAttribute(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	h := newHarness(t)
	h.seedOrder(t)
	newWire(t, h.deps).skill(t, skillOrderStatus, map[string]any{"orderId": "ORD-1"})

	var found bool
	for _, s := range rec.Ended() {
		if s.Name() != "a2a.skill "+skillOrderStatus {
			continue
		}
		found = true
		attrs := map[string]string{}
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.AsString()
		}
		if attrs["a2a.skill"] != skillOrderStatus {
			t.Errorf("span attributes = %v, want a2a.skill=%s", attrs, skillOrderStatus)
		}
	}
	if !found {
		t.Fatal("no \"a2a.skill order_status\" span recorded")
	}
}

func TestAggregatePromiseHealth(t *testing.T) {
	bucket1 := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
	bucket2 := time.Date(2026, 9, 14, 11, 0, 0, 0, time.UTC)

	got := aggregatePromiseHealth([]PromiseHealthRow{
		{PathID: "pick", HourBucket: bucket1, PromiseBasisCapability: 3, PromiseBasisLeadTime: 1, OrdersSplitShipment: 1, PromiseToCutoffGapSeconds: 3600, PromiseToCutoffGapSamples: 2},
		{PathID: "pick", HourBucket: bucket2, PromiseBasisCapability: 1, PromiseBasisNetwork: 2, PromiseToCutoffGapSeconds: 7200, PromiseToCutoffGapSamples: 1},
		{PathID: "", HourBucket: bucket1, OrdersRepromised: 2}, // the fleet-wide, path-less re-promise row
	})
	want := promiseHealthDTO{
		PromiseBasisCapability: 4, PromiseBasisLeadTime: 1, PromiseBasisNetwork: 2, OrdersAllocatedTotal: 7,
		OrdersSplitShipment: 1, SplitShipmentRate: 1.0 / 7.0, OrdersRepromised: 2, RepromiseRate: 2.0 / 7.0,
		PromiseToCutoffGapSeconds: 4800, // (3600*2 + 7200*1) / 3
	}
	if got != want {
		t.Fatalf("aggregate = %+v, want %+v", got, want)
	}

	if empty := aggregatePromiseHealth(nil); empty != (promiseHealthDTO{}) {
		t.Errorf("empty aggregate = %+v, want all zero", empty)
	}
}

// TestCardIsServedAndDialable proves discovery end to end: the card handler
// serves the card at the well-known path and a client built from the FETCHED
// card reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	h := newHarness(t)
	h.seedOrder(t)
	mux := http.NewServeMux()
	mux.Handle("/", Handler(NewExecutor(h.deps)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(Config{AgentURL: srv.URL + "/", Version: "test"})))

	res, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("card fetch = %v / %v", res, err)
	}
	defer func() { _ = res.Body.Close() }()
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatal(err)
	}
	env, _ := (&wire{client: client}).skill(t, skillOrderStatus, map[string]any{"orderId": "ORD-1"})
	if env.Data == nil || env.Data.OrderStatus == nil {
		t.Fatalf("a client built from the served card could not call the agent: %+v", env)
	}
}
