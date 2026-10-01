package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// repromiseFakeProcessed is a scripted ports.RepromiseProcessedEvents.
type repromiseFakeProcessed struct {
	seen map[string]bool
	err  error
}

func newRepromiseFakeProcessed() *repromiseFakeProcessed {
	return &repromiseFakeProcessed{seen: map[string]bool{}}
}

func (p *repromiseFakeProcessed) MarkProcessed(_ context.Context, eventID string) (bool, error) {
	if p.err != nil {
		return false, p.err
	}
	if p.seen[eventID] {
		return false, nil
	}
	p.seen[eventID] = true
	return true, nil
}

// repromiseFixture wires the real in-memory OrderRepo + a real
// RepromiseOrder use case, so the Kafka adapter's decode/routing logic
// is exercised end to end without a live broker.
type repromiseFixture struct {
	orders    *memory.OrderRepo
	processed *repromiseFakeProcessed
	events    *repromiseCapturingPublisher
	clock     *memory.FixedClock
	consumer  *RepromiseConsumer
}

type repromiseCapturingPublisher struct {
	published []shared.DomainEvent
}

func (p *repromiseCapturingPublisher) Publish(_ context.Context, e shared.DomainEvent) error {
	p.published = append(p.published, e)
	return nil
}

var _ ports.EventPublisher = (*repromiseCapturingPublisher)(nil)

func newRepromiseFixture(promise order.PromisePolicy) *repromiseFixture {
	orders := memory.NewOrderRepo()
	processed := newRepromiseFakeProcessed()
	events := &repromiseCapturingPublisher{}
	clock := memory.NewFixedClock(time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC))

	uc := &usecases.RepromiseOrder{
		Orders: orders, Promise: promise, Events: events, Clock: clock, Processed: processed,
	}
	return &repromiseFixture{
		orders: orders, processed: processed, events: events, clock: clock,
		consumer: &RepromiseConsumer{repromiseOrder: uc},
	}
}

// seedOrder persists an order with one allocated+released line and a
// real PromiseGroup breakdown (built directly rather than through
// ReceiveOrder, since this package cannot import the usecases_test
// fixture helpers), covering line 1 at initialCutoff.
func (rf *repromiseFixture) seedOrder(t *testing.T, orderID shared.OrderId, initialCutoff time.Time) *order.Order {
	t.Helper()
	l, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	o, err := order.New(orderID, []*order.OrderLine{l}, false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := o.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	o.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1}, Promise: order.Promise{CutoffAt: initialCutoff, Basis: order.BasisLeadTime}},
	})
	if err := rf.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return o
}

// fulfillmentEvent builds a CloudEvents 1.0 event exactly as
// fulfillment-execution publishes it on warehouse.fulfillment.events.
func fulfillmentEvent(t *testing.T, eventID, ceType string, data any) ce.Event {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(eventID)
	e.SetSource("/warehouse/fulfillment-execution")
	e.SetType(ceType)
	e.SetSubject("task-1")
	e.SetTime(time.Now())
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return e
}

func taskCPTMissedEnvelope(t *testing.T, eventID, orderRef string) ce.Event {
	t.Helper()
	return fulfillmentEvent(t, eventID, ceTypeTaskCPTMissed, taskCPTMissedData{
		TaskId: "task-1", OrderRef: orderRef, TaskType: "PICK", Cpt: time.Now(),
	})
}

func packageManifestedEnvelope(t *testing.T, eventID, orderRef string) ce.Event {
	t.Helper()
	return fulfillmentEvent(t, eventID, ceTypePackageManifested, packageManifestedData{PackageId: "pkg-1", OrderRef: orderRef})
}

func TestHandleFulfillmentEvent_TaskCPTMissed_DrivesRepromise(t *testing.T) {
	initialCutoff := time.Date(2026, 9, 14, 9+24, 0, 0, 0, time.UTC)
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	rf := newRepromiseFixture(movedPromise)
	o := rf.seedOrder(t, "ord-1", initialCutoff)

	env := taskCPTMissedEnvelope(t, "evt-1", usecases.WorkUnitID(o.ID(), 1))
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	if len(rf.events.published) != 1 {
		t.Fatalf("published = %d events, want 1", len(rf.events.published))
	}
	repromised, ok := rf.events.published[0].(shared.OrderRepromised)
	if !ok {
		t.Fatalf("published event = %T, want shared.OrderRepromised", rf.events.published[0])
	}
	if repromised.OrderID != o.ID() {
		t.Errorf("OrderRepromised.OrderID = %q, want %q", repromised.OrderID, o.ID())
	}
	if repromised.Reason != reasonTaskCPTMissed {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, reasonTaskCPTMissed)
	}
}

func TestHandleFulfillmentEvent_PackageManifested_DrivesRepromise(t *testing.T) {
	initialCutoff := time.Date(2026, 9, 14, 9+24, 0, 0, 0, time.UTC)
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	rf := newRepromiseFixture(movedPromise)
	o := rf.seedOrder(t, "ord-1", initialCutoff)

	env := packageManifestedEnvelope(t, "evt-1", usecases.WorkUnitID(o.ID(), 1))
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	if len(rf.events.published) != 1 {
		t.Fatalf("published = %d events, want 1", len(rf.events.published))
	}
	repromised := rf.events.published[0].(shared.OrderRepromised)
	if repromised.Reason != reasonPackageManifested {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, reasonPackageManifested)
	}
}

func TestHandleFulfillmentEvent_IgnoresOtherEventTypes(t *testing.T) {
	rf := newRepromiseFixture(order.PromisePolicy{})
	env := taskCPTMissedEnvelope(t, "evt-1", "ord-1-line-1")
	env.SetType("com.warehouse.wes.fulfillment-execution.task.SomethingElse")

	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}
	if len(rf.events.published) != 0 {
		t.Fatalf("published = %d events, want 0 for an unrecognized event type", len(rf.events.published))
	}
}

func TestHandleFulfillmentEvent_MalformedOrderRef_SkipsWithoutError(t *testing.T) {
	rf := newRepromiseFixture(order.PromisePolicy{})
	env := taskCPTMissedEnvelope(t, "evt-1", "not-a-work-unit-id")

	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v, want nil (skip malformed order_ref)", err)
	}
	if len(rf.events.published) != 0 {
		t.Fatalf("published = %d events, want 0", len(rf.events.published))
	}
}

func TestHandleFulfillmentEvent_MalformedJSON_ReturnsError(t *testing.T) {
	rf := newRepromiseFixture(order.PromisePolicy{})
	env := taskCPTMissedEnvelope(t, "evt-1", "ord-1-line-1")
	env.DataEncoded = []byte(`{"task_id":`)
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err == nil {
		t.Fatal("handleFulfillmentEvent: want error for malformed data JSON")
	}
}

func TestHandleFulfillmentEvent_RedeliveryIsIdempotent(t *testing.T) {
	initialCutoff := time.Date(2026, 9, 14, 9+24, 0, 0, 0, time.UTC)
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	rf := newRepromiseFixture(movedPromise)
	o := rf.seedOrder(t, "ord-1", initialCutoff)

	env := taskCPTMissedEnvelope(t, "evt-dup", usecases.WorkUnitID(o.ID(), 1))
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("first handleFulfillmentEvent: %v", err)
	}
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("redelivered handleFulfillmentEvent: %v", err)
	}
	if len(rf.events.published) != 1 {
		t.Fatalf("published = %d events, want 1 (no double-repromise on redelivery)", len(rf.events.published))
	}
}

// TestHandleFulfillmentEvent_MimicsFulfillmentExecutionSweepReemission
// covers ADR 0025 §4's real, documented behaviour: fulfillment-
// execution's TaskCPTMissed sweep re-fires on EVERY sweep pass for as
// long as a task stays overdue — a genuinely DIFFERENT event_id each
// time, for the SAME task/order_ref. Each such re-fire must be evaluated
// (not deduped, since the event_id differs), but once the promise has
// already moved once, a second sweep pass over the SAME already-moved
// promise must not move it again.
func TestHandleFulfillmentEvent_MimicsFulfillmentExecutionSweepReemission(t *testing.T) {
	initialCutoff := time.Date(2026, 9, 14, 9+24, 0, 0, 0, time.UTC)
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	rf := newRepromiseFixture(movedPromise)
	o := rf.seedOrder(t, "ord-1", initialCutoff)

	first := taskCPTMissedEnvelope(t, "evt-sweep-1", usecases.WorkUnitID(o.ID(), 1))
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), first); err != nil {
		t.Fatalf("first sweep pass: %v", err)
	}
	second := taskCPTMissedEnvelope(t, "evt-sweep-2", usecases.WorkUnitID(o.ID(), 1))
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), second); err != nil {
		t.Fatalf("second sweep pass: %v", err)
	}

	if len(rf.events.published) != 1 {
		t.Fatalf("published = %d events, want 1 (promise moved once, second pass sees no further movement)", len(rf.events.published))
	}
}

func TestHandleFulfillmentEvent_InfrastructureFailurePropagates(t *testing.T) {
	rf := newRepromiseFixture(order.PromisePolicy{})
	rf.processed.err = errors.New("boom")
	env := taskCPTMissedEnvelope(t, "evt-1", "ord-1-line-1")

	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err == nil {
		t.Fatal("handleFulfillmentEvent: want the infrastructure error propagated")
	}
}

// TestHandleFulfillmentEvent_ShortTypeNameIgnored proves dispatch is on
// the FULL CloudEvents type: a bare "TaskCPTMissed" type is unknown and
// skipped, never acted on.
func TestHandleFulfillmentEvent_ShortTypeNameIgnored(t *testing.T) {
	rf := newRepromiseFixture(order.PromisePolicy{})
	env := taskCPTMissedEnvelope(t, "evt-1", "ord-1-line-1")
	env.SetType("TaskCPTMissed")
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}
	if len(rf.events.published) != 0 || len(rf.processed.seen) != 0 {
		t.Fatalf("short type must be ignored: published=%d seen=%v", len(rf.events.published), rf.processed.seen)
	}
}

// TestRepromise_LegacyFlatEnvelopeRejected proves the retired flat
// fulfillment envelope fails the CloudEvents gate handleMessage applies
// before any dispatch (handleMessage then dead-letters and commits it —
// covered end to end by the testcontainers DLQ integration test).
func TestRepromise_LegacyFlatEnvelopeRejected(t *testing.T) {
	flat := legacyFlat(t, "evt-legacy", "TaskCPTMissed", taskCPTMissedData{TaskId: "task-1", OrderRef: "ord-1-line-1"})
	if _, err := cloudevents.Decode(flat); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("Decode(flat) err = %v, want ErrNotCloudEvent", err)
	}
}

func legacyFlat(t *testing.T, eventID, eventType string, data any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": eventType, "occurred_at": time.Now(),
		"source": "fulfillment-execution", "data": json.RawMessage(raw),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
