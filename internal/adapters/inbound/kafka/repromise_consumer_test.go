package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

func taskCPTMissedEnvelope(t *testing.T, eventID, orderRef string) fulfillmentEnvelope {
	t.Helper()
	data, err := json.Marshal(taskCPTMissedData{
		TaskId: "task-1", OrderRef: orderRef, TaskType: "PICK", Cpt: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	return fulfillmentEnvelope{
		EventID: eventID, EventType: eventTypeTaskCPTMissed,
		OccurredAt: time.Now(), Source: "fulfillment-execution", Data: data,
	}
}

func packageManifestedEnvelope(t *testing.T, eventID, orderRef string) fulfillmentEnvelope {
	t.Helper()
	data, err := json.Marshal(packageManifestedData{PackageId: "pkg-1", OrderRef: orderRef})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	return fulfillmentEnvelope{
		EventID: eventID, EventType: eventTypePackageManifested,
		OccurredAt: time.Now(), Source: "fulfillment-execution", Data: data,
	}
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
	if repromised.Reason != eventTypeTaskCPTMissed {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, eventTypeTaskCPTMissed)
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
	if repromised.Reason != eventTypePackageManifested {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, eventTypePackageManifested)
	}
}

func TestHandleFulfillmentEvent_IgnoresOtherEventTypes(t *testing.T) {
	rf := newRepromiseFixture(order.PromisePolicy{})
	env := taskCPTMissedEnvelope(t, "evt-1", "ord-1-line-1")
	env.EventType = "SomethingElse"

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
	env := fulfillmentEnvelope{
		EventID: "evt-1", EventType: eventTypeTaskCPTMissed,
		OccurredAt: time.Now(), Source: "fulfillment-execution", Data: json.RawMessage(`{"task_id":`),
	}
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

// --- ADR-0027 / ADR-0021 Phase 2 Task 2e: CloudEvents dual-read tests ---

func cloudEventsTaskCPTMissedJSON(t *testing.T, eventID, orderRef string) []byte {
	t.Helper()
	data, err := json.Marshal(taskCPTMissedData{
		TaskId: "task-8a1f", OrderRef: orderRef, TaskType: "PICK", Cpt: time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	ce := map[string]any{
		"specversion":     "1.0",
		"id":              eventID,
		"type":            cloudEventsTypeTaskCPTMissed,
		"source":          "/warehouse/fulfillment-execution",
		"subject":         "task-8a1f",
		"time":            "2026-08-22T18:05:00Z",
		"datacontenttype": "application/json",
		"data":            json.RawMessage(data),
	}
	b, err := json.Marshal(ce)
	if err != nil {
		t.Fatalf("marshal CloudEvents envelope: %v", err)
	}
	return b
}

func cloudEventsPackageManifestedJSON(t *testing.T, eventID, orderRef string) []byte {
	t.Helper()
	data, err := json.Marshal(packageManifestedData{PackageId: "pkg-1029", OrderRef: orderRef})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	ce := map[string]any{
		"specversion":     "1.0",
		"id":              eventID,
		"type":            cloudEventsTypePackageManifested,
		"source":          "/warehouse/fulfillment-execution",
		"subject":         "pkg-1029",
		"time":            "2026-08-22T14:12:00Z",
		"datacontenttype": "application/json",
		"data":            json.RawMessage(data),
	}
	b, err := json.Marshal(ce)
	if err != nil {
		t.Fatalf("marshal CloudEvents envelope: %v", err)
	}
	return b
}

func TestDecodeFulfillmentEnvelope_CloudEventsTaskCPTMissed_SameResultAsFlat(t *testing.T) {
	orderRef := "ord-1-line-1"
	cpt := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	flatData := taskCPTMissedData{TaskId: "task-8a1f", OrderRef: orderRef, TaskType: "PICK", Cpt: cpt}
	flatDataRaw, err := json.Marshal(flatData)
	if err != nil {
		t.Fatalf("marshal flat data: %v", err)
	}
	flatEnv := fulfillmentEnvelope{
		EventID: "evt-1", EventType: eventTypeTaskCPTMissed,
		OccurredAt: time.Now(), Source: "fulfillment-execution", Data: flatDataRaw,
	}

	ceRaw := cloudEventsTaskCPTMissedJSON(t, "evt-1", orderRef)
	ceEnv, err := decodeFulfillmentEnvelope(ceRaw)
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope: %v", err)
	}

	if ceEnv.EventID != flatEnv.EventID {
		t.Errorf("EventID = %q, want %q", ceEnv.EventID, flatEnv.EventID)
	}
	if ceEnv.EventType != eventTypeTaskCPTMissed {
		t.Errorf("EventType = %q, want %q (bare name stripped from reverse-DNS)", ceEnv.EventType, eventTypeTaskCPTMissed)
	}

	var ceData, wantData taskCPTMissedData
	if err := json.Unmarshal(ceEnv.Data, &ceData); err != nil {
		t.Fatalf("unmarshal CloudEvents data: %v", err)
	}
	if err := json.Unmarshal(flatEnv.Data, &wantData); err != nil {
		t.Fatalf("unmarshal flat data: %v", err)
	}
	if ceData != wantData {
		t.Errorf("CloudEvents-decoded data = %+v, want byte-identical flat data %+v", ceData, wantData)
	}
}

func TestDecodeFulfillmentEnvelope_CloudEventsPackageManifested_SameResultAsFlat(t *testing.T) {
	orderRef := "ord-1-line-1"
	flatData := packageManifestedData{PackageId: "pkg-1029", OrderRef: orderRef}
	flatDataRaw, err := json.Marshal(flatData)
	if err != nil {
		t.Fatalf("marshal flat data: %v", err)
	}
	flatEnv := fulfillmentEnvelope{
		EventID: "evt-1", EventType: eventTypePackageManifested,
		OccurredAt: time.Now(), Source: "fulfillment-execution", Data: flatDataRaw,
	}

	ceRaw := cloudEventsPackageManifestedJSON(t, "evt-1", orderRef)
	ceEnv, err := decodeFulfillmentEnvelope(ceRaw)
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope: %v", err)
	}

	if ceEnv.EventType != eventTypePackageManifested {
		t.Errorf("EventType = %q, want %q (bare name stripped from reverse-DNS)", ceEnv.EventType, eventTypePackageManifested)
	}

	var ceData, wantData packageManifestedData
	if err := json.Unmarshal(ceEnv.Data, &ceData); err != nil {
		t.Fatalf("unmarshal CloudEvents data: %v", err)
	}
	if err := json.Unmarshal(flatEnv.Data, &wantData); err != nil {
		t.Fatalf("unmarshal flat data: %v", err)
	}
	if ceData != wantData {
		t.Errorf("CloudEvents-decoded data = %+v, want byte-identical flat data %+v", ceData, wantData)
	}
}

func TestHandleFulfillmentEvent_CloudEventsShapedTaskCPTMissed_DrivesRepromise(t *testing.T) {
	initialCutoff := time.Date(2026, 9, 14, 9+24, 0, 0, 0, time.UTC)
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	rf := newRepromiseFixture(movedPromise)
	o := rf.seedOrder(t, "ord-1", initialCutoff)

	orderRef := usecases.WorkUnitID(o.ID(), 1)
	ceRaw := cloudEventsTaskCPTMissedJSON(t, "evt-ce-1", orderRef)
	env, err := decodeFulfillmentEnvelope(ceRaw)
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope: %v", err)
	}

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
	if repromised.Reason != eventTypeTaskCPTMissed {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, eventTypeTaskCPTMissed)
	}
}

func TestDecodeFulfillmentEnvelope_UnrecognizedSpecversion_FailsSoft(t *testing.T) {
	raw := []byte(`{"specversion":"2.0","type":"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed","data":{"task_id":"task-1","order_ref":"ord-1-line-1"}}`)
	if _, err := decodeFulfillmentEnvelope(raw); err == nil {
		t.Fatal("decodeFulfillmentEnvelope: want an error for an unrecognized specversion, mirroring this repo's malformed-message handling posture")
	}
}

func TestHandleMessage_UnrecognizedSpecversion_SkippedNotFatal(t *testing.T) {
	// A malformed/unrecognized specversion message must be skipped
	// (fail soft) rather than wedging the consume loop -- mirrored via
	// decodeFulfillmentEnvelope directly, since handleMessage itself
	// needs a live *kafkago.Reader for CommitMessages.
	raw := []byte(`{"specversion":"9.9","type":"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed","data":{}}`)
	_, err := decodeFulfillmentEnvelope(raw)
	if err == nil {
		t.Fatal("expected decodeFulfillmentEnvelope to fail soft (return an error, not panic) on an unrecognized specversion")
	}
}
