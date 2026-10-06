package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// fakeWriter captures every message it's asked to write, so tests can assert
// on the exact envelope shape without a real broker.
type fakeWriter struct {
	messages []kafkago.Message
	err      error
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if w.err != nil {
		return w.err
	}
	w.messages = append(w.messages, msgs...)
	return nil
}

// envelope is the test's flattened view of a decoded CloudEvent: every
// context attribute plus the raw data payload.
type envelope struct {
	SpecVersion     string
	ID              string
	Source          string
	Type            string
	Subject         string
	Time            time.Time
	DataContentType string
	DataSchema      string
	Data            json.RawMessage
}

// typeOf is the full CloudEvents type this service publishes for name.
// SiteSkuDemandChanged is the one event raised outside the Order entity:
// its entity segment is "siteskudemand" (see publisher.go's eventEntity).
func typeOf(name string) string {
	entity := "order"
	if name == "SiteSkuDemandChanged" {
		entity = "siteskudemand"
	}
	return "com.warehouse.wes.order-management." + entity + "." + name
}

type releasedLineData struct {
	LineNo           int    `json:"line_no"`
	SKU              string `json:"sku"`
	PathID           string `json:"path_id"`
	GiftWrap         bool   `json:"gift_wrap"`
	FulfillmentClass string `json:"fulfillment_class"`
	PromiseCptId     string `json:"promise_cpt_id,omitempty"`
	PromiseBasis     string `json:"promise_basis,omitempty"`
	PromiseCutoffAt  string `json:"promise_cutoff_at,omitempty"`
}

type allocationData struct {
	OrderID     string             `json:"order_id"`
	PromiseDate string             `json:"promise_date"`
	Lines       []releasedLineData `json:"lines"`
}

// publishOne publishes event through pub over writer and asserts
// exactly one message was written, returning it.
func publishOne(t *testing.T, writer *fakeWriter, pub *kafka.Publisher, event shared.DomainEvent) kafkago.Message {
	t.Helper()
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}
	return writer.messages[0]
}

// decodeEnvelope decodes and validates a published message value as a
// CloudEvents 1.0 event via the service's own cloudevents.Decode.
func decodeEnvelope(t *testing.T, msg kafkago.Message) envelope {
	t.Helper()
	return decodeCE(t, msg.Value)
}

func decodeCE(t *testing.T, raw []byte) envelope {
	t.Helper()
	e, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("cloudevents.Decode: %v (%s)", err, raw)
	}
	return envelope{
		SpecVersion: e.SpecVersion(), ID: e.ID(), Source: e.Source(), Type: e.Type(),
		Subject: e.Subject(), Time: e.Time(), DataContentType: e.DataContentType(),
		DataSchema: e.DataSchema(), Data: e.Data(),
	}
}

// decodeAllocationData unmarshals an envelope's data payload into the
// typed allocation shape.
func decodeAllocationData(t *testing.T, env envelope) allocationData {
	t.Helper()
	var data allocationData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	return data
}

// assertReleasedLine pins one decoded released-line entry against its
// expected wire shape.
func assertReleasedLine(t *testing.T, got, want releasedLineData) {
	t.Helper()
	if got != want {
		t.Errorf("released line = %+v, want %+v", got, want)
	}
}

func TestPublisher_OrderAllocated_EnvelopeShape(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	occurredAt := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	promiseDate := occurredAt.Add(24 * time.Hour)
	lines := []shared.ReleasedLine{
		{LineNo: 1, SKU: "SKU-1", PathID: "pick", GiftWrap: true, FulfillmentClass: "MULTI_LINE_MULTI"},
		{LineNo: 2, SKU: "SKU-2", PathID: "singles", GiftWrap: false, FulfillmentClass: "MULTI_LINE_MULTI"},
	}
	event := shared.NewOrderAllocated(occurredAt, "ord-42", promiseDate, lines)

	env := decodeEnvelope(t, publishOne(t, writer, pub, event))

	t.Run("envelope", func(t *testing.T) {
		assertAllocatedEnvelopeHeader(t, env, occurredAt)
	})

	t.Run("data", func(t *testing.T) {
		assertAllocatedEnvelopeData(t, decodeAllocationData(t, env), promiseDate)
	})
}

// assertAllocatedEnvelopeHeader pins the envelope-level fields of a
// published OrderAllocated event.
func assertAllocatedEnvelopeHeader(t *testing.T, env envelope, occurredAt time.Time) {
	t.Helper()
	if env.Type != typeOf("OrderAllocated") {
		t.Errorf("EventType = %q, want OrderAllocated", env.Type)
	}
	if env.Source != cloudevents.Source {
		t.Errorf("Source = %q, want %q", env.Source, cloudevents.Source)
	}
	if !env.Time.Equal(occurredAt) {
		t.Errorf("OccurredAt = %v, want %v", env.Time, occurredAt)
	}
	if env.ID == "" {
		t.Error("EventID must not be empty")
	}
}

// assertAllocatedEnvelopeData pins the payload-level fields of a
// published OrderAllocated event carrying two released lines.
func assertAllocatedEnvelopeData(t *testing.T, data allocationData, promiseDate time.Time) {
	t.Helper()
	if data.OrderID != "ord-42" {
		t.Errorf("data.order_id = %q, want ord-42", data.OrderID)
	}
	if data.PromiseDate != promiseDate.Format(time.RFC3339) {
		t.Errorf("data.promise_date = %q, want %q", data.PromiseDate, promiseDate.Format(time.RFC3339))
	}
	if len(data.Lines) != 2 {
		t.Fatalf("data.lines = %v, want 2 entries", data.Lines)
	}
	assertReleasedLine(t, data.Lines[0], releasedLineData{LineNo: 1, SKU: "SKU-1", PathID: "pick", GiftWrap: true, FulfillmentClass: "MULTI_LINE_MULTI"})
	assertReleasedLine(t, data.Lines[1], releasedLineData{LineNo: 2, SKU: "SKU-2", PathID: "singles", FulfillmentClass: "MULTI_LINE_MULTI"})
}

func TestPublisher_OrderPartiallyAllocated_EnvelopeShape(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	occurredAt := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	promiseDate := occurredAt.Add(6 * time.Hour)
	lines := []shared.ReleasedLine{{LineNo: 1, SKU: "SKU-1", PathID: "pick", GiftWrap: false}}
	event := shared.NewOrderPartiallyAllocated(occurredAt, "ord-7", 1, 1, promiseDate, lines)

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	env := decodeEnvelope(t, writer.messages[0])
	if env.Type != typeOf("OrderPartiallyAllocated") {
		t.Errorf("EventType = %q, want OrderPartiallyAllocated", env.Type)
	}

	var data allocationData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if data.OrderID != "ord-7" || len(data.Lines) != 1 {
		t.Errorf("data = %+v, want order_id ord-7 with 1 line", data)
	}
}

func TestPublisher_OrderAllocated_EmptyLines(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	event := shared.NewOrderAllocated(time.Now(), "ord-1", time.Now(), nil)
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	env := decodeEnvelope(t, writer.messages[0])
	var data allocationData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if len(data.Lines) != 0 {
		t.Errorf("data.lines = %v, want empty", data.Lines)
	}
}

func TestPublisher_IgnoresOtherDomainEvents(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	event := shared.NewOrderReceived(time.Now(), "ord-1", 2)

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(writer.messages) != 0 {
		t.Errorf("expected no message written for a non-integration event, got %d", len(writer.messages))
	}
}

type repromisedData struct {
	OrderID  string `json:"order_id"`
	CptIdOld string `json:"cpt_id_old,omitempty"`
	CptIdNew string `json:"cpt_id_new,omitempty"`
	Reason   string `json:"reason"`
}

// TestPublisher_OrderRepromised_EnvelopeShape covers ADR 0014 §5 / ADR
// 0018's new integration event: the fleet's "your delivery is delayed"
// trigger.
func TestPublisher_OrderRepromised_EnvelopeShape(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	occurredAt := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	event := shared.NewOrderRepromised(occurredAt, "ord-42", "sp1-1200", "sp1-1800", "TaskCPTMissed")

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	env := decodeEnvelope(t, writer.messages[0])
	if env.Type != typeOf("OrderRepromised") {
		t.Errorf("EventType = %q, want OrderRepromised", env.Type)
	}
	if env.Source != cloudevents.Source {
		t.Errorf("Source = %q, want %q", env.Source, cloudevents.Source)
	}

	var data repromisedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if data.OrderID != "ord-42" || data.CptIdOld != "sp1-1200" || data.CptIdNew != "sp1-1800" || data.Reason != "TaskCPTMissed" {
		t.Errorf("data = %+v, want {ord-42 sp1-1200 sp1-1800 TaskCPTMissed}", data)
	}
}

// TestPublisher_OrderRepromised_EmptyCptIdsOmitted covers a LeadTime-basis
// re-promise: neither cpt_id_old nor cpt_id_new should appear on the wire
// at all (omitempty), not present-and-empty.
func TestPublisher_OrderRepromised_EmptyCptIdsOmitted(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	event := shared.NewOrderRepromised(time.Now(), "ord-1", "", "", "PackageManifested")
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	env := decodeEnvelope(t, writer.messages[0])
	var raw map[string]any
	if err := json.Unmarshal(env.Data, &raw); err != nil {
		t.Fatalf("failed to unmarshal raw data: %v", err)
	}
	for _, key := range []string{"cpt_id_old", "cpt_id_new"} {
		if _, present := raw[key]; present {
			t.Errorf("raw data has key %q, want it entirely absent (omitempty) for a LeadTime-basis promise", key)
		}
	}
}

// TestPublisher_InjectsTraceContextIntoHeaders proves the published message
// carries the W3C traceparent for the publish span, which is what lets
// wes-work-planning's consumer parent its span onto this one. It also pins
// the span's name and messaging attributes, since those are the fleet-wide
// convention the other services follow.
func TestPublisher_InjectsTraceContextIntoHeaders(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	installTracing(t, sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))

	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	event := shared.NewOrderAllocated(time.Now(), "ord-1", time.Now(), nil)

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))

	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	traceparent := traceparentHeader(t, writer.messages[0])

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	published := spans[0]

	t.Run("span", func(t *testing.T) {
		assertPublishSpan(t, published, spanID)
	})

	t.Run("traceparent", func(t *testing.T) {
		assertTraceparentHeader(t, traceparent, traceID, published)
	})

	t.Run("messaging attributes", func(t *testing.T) {
		assertPublishSpanAttributes(t, published)
	})
}

// traceparentHeader returns the traceparent header value carried by a
// published message, failing the test when the header is absent.
func traceparentHeader(t *testing.T, msg kafkago.Message) string {
	t.Helper()
	for _, h := range msg.Headers {
		if h.Key == "traceparent" {
			return string(h.Value)
		}
	}
	t.Fatalf("no traceparent header on the published message: %+v", msg.Headers)
	return ""
}

// assertPublishSpan pins the publish span's name, kind and parent — the
// fleet-wide convention the other services follow.
func assertPublishSpan(t *testing.T, published sdktrace.ReadOnlySpan, parentSpanID trace.SpanID) {
	t.Helper()
	if published.Name() != "kafka.publish "+kafka.Topic {
		t.Errorf("span name = %q, want %q", published.Name(), "kafka.publish "+kafka.Topic)
	}
	if published.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want producer", published.SpanKind())
	}
	if published.Parent().SpanID() != parentSpanID {
		t.Errorf("publish span parent = %s, want the caller's span %s", published.Parent().SpanID(), parentSpanID)
	}
}

// assertTraceparentHeader pins the W3C traceparent the published message
// carries for the publish span.
func assertTraceparentHeader(t *testing.T, traceparent string, traceID trace.TraceID, published sdktrace.ReadOnlySpan) {
	t.Helper()
	want := "00-" + traceID.String() + "-" + published.SpanContext().SpanID().String() + "-01"
	if traceparent != want {
		t.Errorf("traceparent = %q, want %q", traceparent, want)
	}
}

// assertPublishSpanAttributes pins the publish span's messaging
// attributes.
func assertPublishSpanAttributes(t *testing.T, published sdktrace.ReadOnlySpan) {
	t.Helper()
	attrs := map[string]string{}
	for _, attr := range published.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	if attrs["messaging.system"] != "kafka" {
		t.Errorf("messaging.system = %q, want kafka", attrs["messaging.system"])
	}
	if attrs["messaging.destination.name"] != kafka.Topic {
		t.Errorf("messaging.destination.name = %q, want %q", attrs["messaging.destination.name"], kafka.Topic)
	}
	if attrs["cloudevents.event_type"] != typeOf("OrderAllocated") {
		t.Errorf("cloudevents.event_type = %q, want %q", attrs["cloudevents.event_type"], typeOf("OrderAllocated"))
	}
}

// errWriterBoom stands in for a broker write failure.
var errWriterBoom = errors.New("writer boom")

func installTracing(t *testing.T, tp trace.TracerProvider) {
	t.Helper()

	previousTracer := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Cleanup(func() {
		otel.SetTracerProvider(previousTracer)
		otel.SetTextMapPropagator(previousPropagator)
	})
}

// TestPublisher_NoTraceContextWithoutASpan covers the un-instrumented case —
// no Setup, so the global provider is the no-op one. Publishing must still
// work and must leave the headers clean rather than stamping on an all-zero
// traceparent that a consumer would try to parent onto.
func TestPublisher_NoTraceContextWithoutASpan(t *testing.T) {
	installTracing(t, noop.NewTracerProvider())

	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	event := shared.NewOrderAllocated(time.Now(), "ord-1", time.Now(), nil)

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	for _, h := range writer.messages[0].Headers {
		if h.Key == "traceparent" {
			t.Errorf("unexpected traceparent header with no active span: %q", h.Value)
		}
	}
}

// TestPublisher_KafkaMessageKey_MatchesOrderID_AcrossEventTypes covers the
// partition-scaleup fix (ADR-0027 / warehouse-infra PR #42's 1->8
// partition change): the Kafka message Key must be the Order's aggregate
// id for every published integration event type, and — critically — must
// be IDENTICAL across OrderAllocated, OrderPartiallyAllocated, and
// OrderRepromised for the SAME order, since Kafka's default partitioner
// only guarantees same-partition routing when the key bytes are equal.
// Before this fix Key was always nil (round-robin), which silently broke
// per-order event ordering the moment the topic gained more than one
// partition.
func TestPublisher_KafkaMessageKey_MatchesOrderID_AcrossEventTypes(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	occurredAt := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	const orderID = "ord-partition-key-42"

	events := []shared.DomainEvent{
		shared.NewOrderAllocated(occurredAt, orderID, occurredAt.Add(24*time.Hour), nil),
		shared.NewOrderPartiallyAllocated(occurredAt, orderID, 1, 1, occurredAt.Add(6*time.Hour), nil),
		shared.NewOrderRepromised(occurredAt, orderID, "sp1-1200", "sp1-1800", "TaskCPTMissed"),
	}
	for _, event := range events {
		if err := pub.Publish(context.Background(), event); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}
	}

	if len(writer.messages) != len(events) {
		t.Fatalf("expected %d messages, got %d", len(events), len(writer.messages))
	}

	for i, msg := range writer.messages {
		if msg.Key == nil {
			t.Fatalf("messages[%d].Key is nil, want %q — every message must carry a partition key", i, orderID)
		}
		if string(msg.Key) != orderID {
			t.Errorf("messages[%d].Key = %q, want %q", i, msg.Key, orderID)
		}
	}
	// Every message for this order must carry the exact same key bytes —
	// this is what Kafka's default partitioner needs to route them all
	// onto the same partition regardless of partition count.
	for i := 1; i < len(writer.messages); i++ {
		if string(writer.messages[i].Key) != string(writer.messages[0].Key) {
			t.Errorf("messages[%d].Key = %q, want identical to messages[0].Key = %q", i, writer.messages[i].Key, writer.messages[0].Key)
		}
	}
}

// TestPublisher_KafkaMessageKey_DiffersAcrossOrders proves the key is
// actually derived from the event's own OrderID rather than some fixed or
// accidental constant — two different orders must get two different keys.
func TestPublisher_KafkaMessageKey_DiffersAcrossOrders(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	event1 := shared.NewOrderAllocated(time.Now(), "ord-A", time.Now(), nil)
	event2 := shared.NewOrderAllocated(time.Now(), "ord-B", time.Now(), nil)

	if err := pub.Publish(context.Background(), event1); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if err := pub.Publish(context.Background(), event2); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(writer.messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(writer.messages))
	}
	if string(writer.messages[0].Key) != "ord-A" {
		t.Errorf("messages[0].Key = %q, want ord-A", writer.messages[0].Key)
	}
	if string(writer.messages[1].Key) != "ord-B" {
		t.Errorf("messages[1].Key = %q, want ord-B", writer.messages[1].Key)
	}
}

// TestPublisher_Encode_SetsKafkaMessageKey covers the outbox path
// (Publisher.Encode, what postgres.OutboxPublisher calls inside the use
// case's transaction) — it must stamp the same OrderID-derived Key onto
// the Encoded row as the direct Publish path, so a message relayed later
// by postgres.OutboxRelay still routes deterministically by order.
func TestPublisher_Encode_SetsKafkaMessageKey(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{})

	event := shared.NewOrderAllocated(time.Now(), "ord-encode-1", time.Now(), nil)
	enc, ok, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	if !ok {
		t.Fatal("Encode: ok = false, want true for OrderAllocated")
	}
	if string(enc.Key) != "ord-encode-1" {
		t.Errorf("Encode Key = %q, want ord-encode-1", enc.Key)
	}
}

func TestPublisher_MarshalErrorPropagates(t *testing.T) {
	writer := &fakeWriter{err: errWriterBoom}
	pub := kafka.NewPublisher(writer)

	event := shared.NewOrderAllocated(time.Now(), "ord-1", time.Now(), nil)
	if err := pub.Publish(context.Background(), event); err == nil {
		t.Fatal("Publish: want error from a failing writer, got nil")
	}
}

// TestPublisher_PerLinePromiseFields_ADR0017 covers the new additive
// per-line promise fields (ADR 0014 §3 / ADR 0017): present, and
// correctly per-line, when ReleasedLine carries them; entirely absent
// (omitempty) when it does not — proving a pre-ADR-0017 event's wire
// shape is completely unaffected, which is what keeps
// wes-work-planning's current consumer (which reads neither the
// existing order-level promise_cpt_id/promise_basis nor these new
// per-line fields) working with zero changes on its side.
func TestPublisher_PerLinePromiseFields_ADR0017(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)

	occurredAt := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	earlyCutoff := occurredAt.Add(2 * time.Hour)
	lateCutoff := occurredAt.Add(8 * time.Hour)
	earlyCpt := "sp1-1200"
	lateCpt := "sp1-1800"
	capabilityBasis := "Capability"
	leadTimeBasis := "LeadTime"

	lines := []shared.ReleasedLine{
		{
			LineNo: 1, SKU: "SKU-1", PathID: "pick", FulfillmentClass: "MULTI_LINE_MULTI",
			PromiseCptId: &earlyCpt, PromiseBasis: &capabilityBasis, PromiseCutoffAt: &earlyCutoff,
		},
		{
			LineNo: 2, SKU: "SKU-2", PathID: "multis", FulfillmentClass: "MULTI_LINE_MULTI",
			PromiseCptId: &lateCpt, PromiseBasis: &capabilityBasis, PromiseCutoffAt: &lateCutoff,
		},
		{
			// No group breakdown for this line (e.g. the legacy
			// SetPromise path) -- every promise field must be absent
			// from the wire, not present-and-empty.
			LineNo: 3, SKU: "SKU-3", PathID: "singles", FulfillmentClass: "MULTI_LINE_MULTI",
		},
	}
	event := shared.NewOrderAllocatedWithPromise(occurredAt, "ord-77", lateCutoff, lateCpt, leadTimeBasis, lines)

	env := decodeEnvelope(t, publishOne(t, writer, pub, event))

	// Assert against the raw JSON too, not just the typed struct, so an
	// accidental "present but empty string" regression (which the typed
	// struct's omitempty tag would silently absorb on decode) is caught.
	t.Run("absent promise fields stay entirely off the wire (omitempty)", func(t *testing.T) {
		assertRawLineOmitsPromiseFields(t, env)
	})

	t.Run("present promise fields decode per line", func(t *testing.T) {
		assertPerLinePromiseDecode(t, decodeAllocationData(t, env), earlyCutoff, earlyCpt, lateCpt, capabilityBasis)
	})
}

// assertRawLineOmitsPromiseFields checks the raw JSON lines[] entry for
// a line with no group breakdown: every promise key must be entirely
// absent (omitempty), not present-and-empty.
func assertRawLineOmitsPromiseFields(t *testing.T, env envelope) {
	t.Helper()
	var raw map[string]any
	if err := json.Unmarshal(env.Data, &raw); err != nil {
		t.Fatalf("failed to unmarshal raw data: %v", err)
	}
	rawLines, ok := raw["lines"].([]any)
	if !ok || len(rawLines) != 3 {
		t.Fatalf("raw lines = %v, want 3 entries", raw["lines"])
	}
	line3, ok := rawLines[2].(map[string]any)
	if !ok {
		t.Fatalf("raw lines[2] = %v, want an object", rawLines[2])
	}
	for _, key := range []string{"promise_cpt_id", "promise_basis", "promise_cutoff_at"} {
		if _, present := line3[key]; present {
			t.Errorf("raw lines[2] has key %q, want it entirely absent (omitempty) when no group applies", key)
		}
	}
}

// assertPerLinePromiseDecode checks the typed decode of the per-line
// promise fields: present and correct on lines 1 and 2, empty on line 3.
func assertPerLinePromiseDecode(t *testing.T, data allocationData, earlyCutoff time.Time, earlyCpt, lateCpt, capabilityBasis string) {
	t.Helper()
	if len(data.Lines) != 3 {
		t.Fatalf("data.Lines = %d entries, want 3", len(data.Lines))
	}
	if data.Lines[0].PromiseCptId != earlyCpt || data.Lines[0].PromiseBasis != capabilityBasis {
		t.Errorf("data.Lines[0] = %+v, want CptId=%q Basis=%q", data.Lines[0], earlyCpt, capabilityBasis)
	}
	if data.Lines[0].PromiseCutoffAt != earlyCutoff.UTC().Format(time.RFC3339) {
		t.Errorf("data.Lines[0].PromiseCutoffAt = %q, want %q", data.Lines[0].PromiseCutoffAt, earlyCutoff.UTC().Format(time.RFC3339))
	}
	if data.Lines[1].PromiseCptId != lateCpt || data.Lines[1].PromiseBasis != capabilityBasis {
		t.Errorf("data.Lines[1] = %+v, want CptId=%q Basis=%q", data.Lines[1], lateCpt, capabilityBasis)
	}
	if data.Lines[2].PromiseCptId != "" || data.Lines[2].PromiseBasis != "" || data.Lines[2].PromiseCutoffAt != "" {
		t.Errorf("data.Lines[2] = %+v, want every promise field empty (no group)", data.Lines[2])
	}
}

func TestPublisher_SiteSkuDemandChanged_UsesLineScopedCloudEventAndKey(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer)
	dueAt := time.Date(2026, 10, 8, 18, 0, 0, 0, time.UTC)
	event := shared.NewSiteSkuDemandChanged(
		time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC),
		"ord-42", 3, "SIM1", "SKU-42", 7, dueAt, shared.SiteSkuDemandActive, "static-site-v1",
	)

	msg := publishOne(t, writer, pub, event)
	env := decodeEnvelope(t, msg)
	if env.Source != "/warehouse/order-management" {
		t.Fatalf("source = %q", env.Source)
	}
	if env.Type != "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged" {
		t.Fatalf("type = %q", env.Type)
	}
	if env.DataSchema != "urn:warehouse:order-management:events:SiteSkuDemandChanged:v1" {
		t.Fatalf("dataschema = %q", env.DataSchema)
	}
	if env.Subject != "ord-42/line/3" {
		t.Fatalf("subject = %q", env.Subject)
	}
	if string(msg.Key) != env.Subject {
		t.Fatalf("key = %q, want %q", msg.Key, env.Subject)
	}

	var data struct {
		SourceOrderID     string `json:"source_order_id"`
		LineNo            int    `json:"line_no"`
		SiteID            string `json:"site_id"`
		SKU               string `json:"sku"`
		DemandedUnits     int    `json:"demanded_units"`
		DueAt             string `json:"due_at"`
		State             string `json:"state"`
		AssignmentVersion string `json:"assignment_version"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("unmarshal demand data: %v", err)
	}
	if data.SourceOrderID != "ord-42" || data.LineNo != 3 || data.SiteID != "SIM1" || data.SKU != "SKU-42" || data.DemandedUnits != 7 || data.DueAt != dueAt.Format(time.RFC3339) || data.State != "ACTIVE" || data.AssignmentVersion != "static-site-v1" {
		t.Fatalf("data = %+v", data)
	}
}
