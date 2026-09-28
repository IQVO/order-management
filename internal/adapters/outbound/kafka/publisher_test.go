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

type envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
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

// decodeEnvelope unmarshals a published message value into the shared
// envelope shape.
func decodeEnvelope(t *testing.T, msg kafkago.Message) envelope {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
	return env
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
	if env.EventType != "OrderAllocated" {
		t.Errorf("EventType = %q, want OrderAllocated", env.EventType)
	}
	if env.Source != kafka.Source {
		t.Errorf("Source = %q, want %q", env.Source, kafka.Source)
	}
	if !env.OccurredAt.Equal(occurredAt) {
		t.Errorf("OccurredAt = %v, want %v", env.OccurredAt, occurredAt)
	}
	if env.EventID == "" {
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

	var env envelope
	if err := json.Unmarshal(writer.messages[0].Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
	if env.EventType != "OrderPartiallyAllocated" {
		t.Errorf("EventType = %q, want OrderPartiallyAllocated", env.EventType)
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

	var env envelope
	if err := json.Unmarshal(writer.messages[0].Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
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

	var env envelope
	if err := json.Unmarshal(writer.messages[0].Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
	if env.EventType != "OrderRepromised" {
		t.Errorf("EventType = %q, want OrderRepromised", env.EventType)
	}
	if env.Source != kafka.Source {
		t.Errorf("Source = %q, want %q", env.Source, kafka.Source)
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

	var env envelope
	if err := json.Unmarshal(writer.messages[0].Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
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
	if attrs["messaging.message.event_type"] != "OrderAllocated" {
		t.Errorf("messaging.message.event_type = %q, want OrderAllocated", attrs["messaging.message.event_type"])
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
