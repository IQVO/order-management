package kafka

// Event consumer contract tests (E1-E4) for the outbound-shipping edge:
// OutboundShippingConsumer subscribes to OutboundShippingEventsTopic
// (warehouse.outbound-shipping.events, wired in
// NewOutboundShippingConsumer's kafka.ReaderConfig) and dispatches on the
// full CloudEvents types ShipmentConfirmed / PackageMissedLoad. The
// contract is the PINNED copy of outbound-shipping's own apis/asyncapi.yaml
// (testdata/outbound-shipping.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/outbound-shipping@develop).
//
// Golden fixtures are the producer spec's OWN examples. Their order ids
// (order-88421...) name no order in this repo's store, so the verbatim
// documents exercise the consumer's documented fail-soft outcomes (ADR
// 0051: unknown order -> skipped, nil error); each test ALSO drives an
// order-id-adapted clone of the same golden document through the full
// ConfirmShipment / RepromiseOrder path against kosFixture's seeded
// orders (mirrors outbound_shipping_consumer_test.go's newKOSFixture).

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// kafkagoMessage aliases the fetched-message type the consumer's
// handleMessage takes; osContractMsg wraps golden payload bytes as one.
type kafkagoMessage = kafkago.Message

// snapshot returns the events published so far (a deep-enough copy for
// assertions outside the publisher's lock).
func (p *osPublisher) snapshot() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]shared.DomainEvent(nil), p.events...)
}

// errIsNotCloudEvent reports the DLQ-trigger signal: a decode failure
// wrapping cloudevents.ErrNotCloudEvent.
func errIsNotCloudEvent(err error) bool {
	return errors.Is(err, cloudevents.ErrNotCloudEvent)
}

// outboundShippingGolden returns the pinned spec's examples for one
// message on the subscribed channel.
func outboundShippingGolden(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShipping)
	return doc.goldenMessage(t, message).Examples
}

// osContractMsg wraps golden payload bytes as one fetched Kafka message.
func osContractMsg(payload []byte) (msg kafkagoMessage) {
	return kafkagoMessage{Offset: 1, Value: payload}
}

// E1: the topic this repo's reader subscribes to is documented by the
// producer's pinned spec as a 'subscribe:' channel (outbound-shipping's
// file is channel-perspective), and the channel's message list documents
// both types outboundShippingRequestFrom dispatches on.
func TestPinnedAsyncAPICoversSubscriptions_OutboundShippingEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShipping)
	names := doc.channelMessageRefs(t, OutboundShippingEventsTopic, "subscribe")

	documented := map[string]bool{}
	for _, name := range names {
		documented[doc.documentedType(t, doc.message(t, name))] = true
	}
	for _, routed := range []string{ceTypeShipmentConfirmed, ceTypePackageMissedLoad} {
		if !documented[routed] {
			t.Errorf("pinned outbound-shipping spec does not document %s on %s (documented: %v)", routed, OutboundShippingEventsTopic, documented)
		}
	}
}

// E2 (ShipmentConfirmed): the golden document decodes through the REAL
// decode path and routes through the REAL consumer handle. Verbatim, its
// orders name no known order — ADR 0051's ignored_unknown_order outcome,
// nil error, nothing published. The same document naming the fixture's
// released order ships it: OrderLineShipped per released line, each
// carrying the golden's own load_id.
func TestGoldenEventsDecode_ShipmentConfirmed(t *testing.T) {
	ex := outboundShippingGolden(t, "ShipmentConfirmed")[0]
	f := newKOSFixture(t)

	// Verbatim: unknown orders are a documented fail-soft skip.
	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, ex.Payload))); err != nil {
		t.Fatalf("verbatim golden must be fail-soft: %v", err)
	}
	if got := f.events.names(); len(got) != 0 {
		t.Fatalf("unknown orders must ship nothing, published %v", got)
	}

	// Adapted clone: the same golden document naming the seeded order.
	adapted := clonePayload(ex.Payload)
	// mint a FRESH CloudEvents id: the verbatim run above already claimed
	// the golden's own id (fail-soft skip still marks it processed), and the
	// id is the idempotency key
	adapted["id"] = "contract-shipment-confirmed-adapted-0001"
	data := adapted["data"].(map[string]any)
	data["orders"] = []any{map[string]any{"order_id": "ord-os-1"}}
	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, adapted))); err != nil {
		t.Fatalf("handleMessage adapted golden: %v", err)
	}
	shipped := 0
	for _, e := range f.events.snapshot() {
		line, ok := e.(shared.OrderLineShipped)
		if !ok {
			continue
		}
		shipped++
		if line.OrderID != shared.OrderId("ord-os-1") {
			t.Errorf("OrderLineShipped.OrderID = %q, want ord-os-1", line.OrderID)
		}
		if want := ex.Payload["data"].(map[string]any)["load_id"]; line.LoadID != want {
			t.Errorf("OrderLineShipped.LoadID = %q, the golden's load_id is %q", line.LoadID, want)
		}
	}
	if shipped != 2 {
		t.Errorf("the golden confirmation shipped %d lines of the released two-line order, want 2", shipped)
	}

	// Dedupe: the same CloudEvents id never ships twice — the replay must
	// leave the published set EXACTLY as the first adapted run left it
	// (two lines plus the OrderShipped order event).
	before := len(f.events.snapshot())
	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, adapted))); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := len(f.events.snapshot()); got != before {
		t.Errorf("replayed CloudEvents id published again: %d events before replay, %d after", before, got)
	}
}

// E2 (PackageMissedLoad): verbatim, the golden's source_order_id names no
// known order — the documented missed-load skip. The same document naming
// the seeded released order drives the order-wide re-promise and
// publishes OrderRepromised.
func TestGoldenEventsDecode_PackageMissedLoad(t *testing.T) {
	ex := outboundShippingGolden(t, "PackageMissedLoad")[0]
	f := newKOSFixture(t)

	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, ex.Payload))); err != nil {
		t.Fatalf("verbatim golden must be fail-soft: %v", err)
	}
	if got := f.events.names(); len(got) != 0 {
		t.Fatalf("an unknown missed-load order must repromise nothing, published %v", got)
	}

	adapted := clonePayload(ex.Payload)
	// fresh CloudEvents id: the verbatim run claimed the golden's own
	// (fail-soft still marks it processed) and the id is the idempotency key
	adapted["id"] = "contract-package-missed-load-adapted-0001"
	data := adapted["data"].(map[string]any)
	data["source_order_id"] = "ord-os-1"
	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, adapted))); err != nil {
		t.Fatalf("handleMessage adapted golden: %v", err)
	}
	repromised := 0
	for _, e := range f.events.snapshot() {
		if _, ok := e.(shared.OrderRepromised); ok {
			repromised++
		}
	}
	if repromised == 0 {
		t.Error("the adapted PackageMissedLoad repromised nothing; the seeded released order must re-promise")
	}
}

// E3: every golden fixture carries the fleet CloudEvents 1.0 envelope
// with outbound-shipping's own source and dataschema consts, and the
// pinned spec's envelope schema requires every mandatory attribute.
func TestCloudEventsEnvelopeInvariants_OutboundShipping(t *testing.T) {
	for _, message := range []string{"ShipmentConfirmed", "PackageMissedLoad"} {
		for _, ex := range outboundShippingGolden(t, message) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example %s: %v", ex.Name, err)
			}
			assertCloudEventsEnvelope(t, ex.Name, evt, "/warehouse/outbound-shipping",
				"urn:warehouse:outbound-shipping:events:"+message+":v1")
		}
	}
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShipping)
	assertEnvelopeRequired(t, doc, "ShipmentConfirmed")
}

// E4: unknown ADDITIVE data attributes (the producer's site_id, carrier,
// trailer_id, seal_number, version... are already extra to this
// consumer's DTO — the live proof) must not fail decode or handling; an
// unknown event type on the shared topic is skipped, not an error; a
// non-CloudEvent message is the deterministic ErrNotCloudEvent the
// consumer dead-letters.
func TestUnknownAttributesTolerated_OutboundShipping(t *testing.T) {
	golden := outboundShippingGolden(t, "ShipmentConfirmed")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = map[string]any{"nested": []any{1, 2}}
	f := newKOSFixture(t)
	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, tolerant))); err != nil {
		t.Fatalf("unknown data attributes must not fail handling: %v", err)
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.outbound-shipping.outboundload.LoadClosed"
	if err := f.consumer.handleMessage(context.Background(), osContractMsg(goldenJSON(t, otherType))); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := f.events.names(); len(got) != 0 {
		t.Errorf("an unknown type was acted on: %v", got)
	}

	if _, err := cloudevents.Decode(goldenJSON(t, map[string]any{"load_id": "load-legacy"})); !errIsNotCloudEvent(err) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
