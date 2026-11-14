package kafka

// Event consumer contract tests (E1-E4) for the fulfillment-execution
// edge: RepromiseConsumer subscribes to FulfillmentEventsTopic
// (warehouse.fulfillment.events, wired in NewRepromiseConsumer's
// kafka.ReaderConfig) and dispatches on the full CloudEvents types
// TaskCPTMissed / PackageManifested. The contract is the PINNED copy of
// fulfillment-execution's own apis/asyncapi.yaml
// (testdata/fulfillment-execution.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/fulfillment-execution@develop).
//
// Golden fixtures are the producer spec's OWN examples. Their order_ref
// values ("wu-8a1f", a bare work-unit id; "order-88421", a bare order id)
// are not WorkUnitId-shaped "{order}-line-{n}" references, so the
// verbatim documents exercise the consumer's documented skip path; each
// test ALSO drives an order_ref-adapted clone of the same golden document
// through the full RepromiseOrder path against a seeded order.

import (
	"context"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// fulfillmentGoldenExamples returns the pinned producer spec's examples
// for one message on the subscribed channel.
func fulfillmentGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	return doc.goldenMessage(t, message).Examples
}

// newFulfillmentContractFixture seeds one released line under a promise
// whose fallback moves, so an adapted golden repromises it (mirrors
// repromise_consumer_test.go's newRepromiseFixture).
func newFulfillmentContractFixture(t *testing.T) *repromiseFixture {
	t.Helper()
	initialCutoff := time.Date(2026, 9, 30, 15, 30, 0, 0, time.UTC)
	moved := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	rf := newRepromiseFixture(moved)
	rf.seedOrder(t, "ord-ct-1", initialCutoff)
	return rf
}

// E1: the topic this repo's reader subscribes to (FulfillmentEventsTopic,
// wired by NewRepromiseConsumer) is documented by the producer's pinned
// spec as a 'subscribe:' channel — fulfillment-execution's file is
// channel-perspective, its own topics sit under 'subscribe:' — and the
// channel's message list documents both types handleFulfillmentEvent
// dispatches on.
func TestPinnedAsyncAPICoversSubscriptions_FulfillmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	names := doc.channelMessageRefs(t, FulfillmentEventsTopic, "subscribe")

	documented := map[string]bool{}
	for _, name := range names {
		documented[doc.documentedType(t, doc.message(t, name))] = true
	}
	for _, routed := range []string{ceTypeTaskCPTMissed, ceTypePackageManifested} {
		if !documented[routed] {
			t.Errorf("pinned fulfillment-execution spec does not document %s on %s (documented: %v)", routed, FulfillmentEventsTopic, documented)
		}
	}
}

// E2: every routed-type example decodes through the REAL
// cloudevents.Decode and routes through the REAL handleFulfillmentEvent.
// The verbatim document skips (its order_ref is not a WorkUnitId —
// fulfillment-execution documents task-scoped refs); an order_ref-adapted
// clone of the same document drives RepromiseOrder and publishes exactly
// one OrderRepromised, and the replayed CloudEvents id records nothing
// more (RepromiseOrder's Processed gate is keyed on the event id).
func TestGoldenEventsDecode_Fulfillment(t *testing.T) {
	for _, c := range []struct {
		message string
		reason  string
	}{
		{message: "TaskCPTMissed", reason: reasonTaskCPTMissed},
		{message: "PackageManifested", reason: reasonPackageManifested},
	} {
		for _, ex := range fulfillmentGoldenExamples(t, c.message) {
			t.Run(ex.Name, func(t *testing.T) { runFulfillmentGoldenCase(t, c.reason, ex) })
		}
	}
}

// runFulfillmentGoldenCase drives one golden fulfillment example through the
// real decode + handle path: verbatim (unparsable order_ref -> documented
// skip, nothing published), an adapted clone naming the seeded order's line
// (exactly one OrderRepromised with the case's reason), and the replayed
// CloudEvents id (Processed gate keyed on the event id -> nothing more).
func runFulfillmentGoldenCase(t *testing.T, reason string, ex pinnedExample) {
	t.Helper()
	{
		evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
		if err != nil {
			t.Fatalf("decode golden example: %v", err)
		}
		rf := newFulfillmentContractFixture(t)

		// Verbatim: an unparsable order_ref is a documented skip.
		if err := rf.consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
			t.Fatalf("verbatim golden must skip cleanly: %v", err)
		}
		if len(rf.events.published) != 0 {
			t.Fatalf("verbatim golden repromised %d orders, want the documented skip", len(rf.events.published))
		}

		// Adapted clone: same golden document, order_ref pointing
		// at the seeded order's line 1.
		adapted := clonePayload(ex.Payload)
		data := adapted["data"].(map[string]any)
		data["order_ref"] = usecases.WorkUnitID(shared.OrderId("ord-ct-1"), 1)
		driven, err := cloudevents.Decode(goldenJSON(t, adapted))
		if err != nil {
			t.Fatalf("decode adapted golden: %v", err)
		}
		if err := rf.consumer.handleFulfillmentEvent(context.Background(), driven); err != nil {
			t.Fatalf("handleFulfillmentEvent: %v", err)
		}
		if len(rf.events.published) != 1 {
			t.Fatalf("adapted golden published %d events, want 1 OrderRepromised", len(rf.events.published))
		}
		repromised, ok := rf.events.published[0].(shared.OrderRepromised)
		if !ok {
			t.Fatalf("published event = %T, want shared.OrderRepromised", rf.events.published[0])
		}
		if repromised.OrderID != shared.OrderId("ord-ct-1") {
			t.Errorf("OrderRepromised.OrderID = %q, want the seeded ord-ct-1", repromised.OrderID)
		}
		if repromised.Reason != reason {
			t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, reason)
		}

		// Dedupe: the same CloudEvents id never repromises twice.
		if err := rf.consumer.handleFulfillmentEvent(context.Background(), driven); err != nil {
			t.Fatalf("replay: %v", err)
		}
		if len(rf.events.published) != 1 {
			t.Errorf("replayed CloudEvents id %q repromised again (%d events)", driven.ID(), len(rf.events.published))
		}
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/fulfillment-execution,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:fulfillment-execution:events:<Name>:v1 — and the pinned
// spec's envelope schema requires exactly those attributes. The
// structured-mode Kafka content-type header the examples carry must match
// this repo's cloudevents.MediaType constant.
func TestCloudEventsEnvelopeInvariants_Fulfillment(t *testing.T) {
	for _, message := range []string{"TaskCPTMissed", "PackageManifested"} {
		for _, ex := range fulfillmentGoldenExamples(t, message) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example %s: %v", ex.Name, err)
			}
			assertCloudEventsEnvelope(t, ex.Name, evt, "/warehouse/fulfillment-execution",
				"urn:warehouse:fulfillment-execution:events:"+message+":v1")
			if got := ex.Headers["content-type"]; got != cloudevents.MediaType {
				t.Errorf("%s: Kafka content-type header %q, want this repo's constant %q", ex.Name, got, cloudevents.MediaType)
			}
		}
	}
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	assertEnvelopeRequired(t, doc, "TaskCPTMissed")
}

// E4: this consumer is a Conformist on a shared fan-out topic — unknown
// ADDITIVE attributes in data must not fail the decode or the handling,
// an unknown event TYPE is silently skipped (not an error, no DLQ, no
// repromise), and a message that is not a CloudEvent at all is a
// deterministic decode error wrapping ErrNotCloudEvent — the exact
// signal handleMessage dead-letters once and never retries.
func TestUnknownAttributesTolerated_Fulfillment(t *testing.T) {
	golden := fulfillmentGoldenExamples(t, "TaskCPTMissed")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	rf := newFulfillmentContractFixture(t)
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(rf.events.published) != 0 {
		t.Errorf("unknown data attributes must not change the skip outcome (%d events)", len(rf.events.published))
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := rf.consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(rf.events.published) != 0 {
		t.Errorf("unknown event type was acted on (%d events)", len(rf.events.published))
	}

	_, err = cloudevents.Decode(goldenJSON(t, map[string]any{"task_id": "task-legacy", "order_ref": "wu-1"}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}

// assertCloudEventsEnvelope is the shared E3 body: one decoded golden
// fixture against the fleet envelope invariants.
func assertCloudEventsEnvelope(t *testing.T, name string, evt ce.Event, source, dataschema string) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/subject/time must all be present (got %q/%q/%v)", name, evt.ID(), evt.Subject(), evt.Time())
	}
	if evt.Source() != source {
		t.Errorf("%s: source %q, want %q", name, evt.Source(), source)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if evt.DataSchema() != dataschema {
		t.Errorf("%s: dataschema %q, want %q", name, evt.DataSchema(), dataschema)
	}
}

// assertEnvelopeRequired is the shared E3 spec-side body: the pinned
// message's envelope schema must require every fleet-mandatory attribute.
func assertEnvelopeRequired(t *testing.T, doc *pinnedAsyncAPI, message string) {
	t.Helper()
	required := doc.envelopeRequired(t, doc.message(t, message).Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's %s envelope does not require %q (required: %v)", message, attr, required)
		}
	}
}
