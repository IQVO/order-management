//go:build integration

// D27 reference (ADR 0041): the W3C trace context of the transaction that
// wrote an outbox row crosses the outbox to Kafka. Real Postgres (the
// package's shared testcontainer) and a real Kafka broker (testcontainers),
// never KAFKA_BROKERS or localhost.
package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what telemetry.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// startKafka boots a disposable broker and pre-creates topics so the
// relay's first write does not race topic auto-creation.
func startKafka(t *testing.T, ctx context.Context, topics ...string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("order-management-outbox-trace-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, topic := range topics {
		if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	return brokers
}

func readOne(t *testing.T, ctx context.Context, brokers []string, topic string) kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", topic, err)
	}
	return msg
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// storedHeaders returns the decoded outbox_events.headers of the only row
// for eventType.
func storedHeaders(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventType string) []map[string]string {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT headers FROM outbox_events WHERE event_type = $1`, eventType).Scan(&raw); err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	var out []map[string]string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode stored headers %s: %v", raw, err)
	}
	return out
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka is the D27 reference
// test. A use case runs inside a request span; the outbox row stores that
// span's traceparent; the relay (a later pass, a different goroutine, no
// span of its own) sends it as a Kafka header; and a consumer extracting it
// with the global propagator lands on the request's trace and span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	brokers := startKafka(t, ctx, outboundkafka.AnalyticsTopic)

	orders := postgres.NewOrderRepo(pool)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt-trace-1" }}
	outbox := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisher(nil), analytics)
	receive := &usecases.ReceiveOrder{
		Orders: orders, Events: outbox, Clock: fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		Inventory: noopInventory{}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	reqCtx, reqSpan := tracer.Start(ctx, "POST /orders", trace.WithSpanKind(trace.SpanKindServer))
	if _, err := receive.Execute(reqCtx, []usecases.NewLine{{SKU: "sku-1", Quantity: 1, PathID: "pick"}}, true); err != nil {
		t.Fatalf("receive: %v", err)
	}
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())

	const received = "com.warehouse.wes.order-management.order.OrderReceived"
	var stored string
	for _, h := range storedHeaders(t, ctx, pool, received) {
		if h["key"] == "traceparent" {
			stored = h["value"]
		}
	}
	if stored != wantTraceparent {
		t.Fatalf("outbox row traceparent = %q, want the request span's %q", stored, wantTraceparent)
	}

	sink := outboundkafka.NewRelaySink(brokers...)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOne(t, ctx, brokers, outboundkafka.AnalyticsTopic)
	got, ok := headerValue(msg.Headers, "traceparent")
	if !ok || got != wantTraceparent {
		t.Fatalf("Kafka traceparent = %q (present=%v), want %q", got, ok, wantTraceparent)
	}
	if ct, _ := headerValue(msg.Headers, "content-type"); ct == "" {
		t.Fatalf("content-type header lost on the outbox path: %+v", msg.Headers)
	}

	consumerCtx := otel.GetTextMapPropagator().Extract(context.Background(), kafkaHeaderCarrier(msg.Headers))
	parent := trace.SpanContextFromContext(consumerCtx)
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged pins the other half
// of D27: a row written with no trace context (no active span, or a row
// that predates trace capture) relays with exactly its stored headers and
// value, and carries no traceparent a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const topic = "warehouse.order-management.itest-no-trace"
	brokers := startKafka(t, ctx, topic)

	insertOutboxRow(t, pool, topic, "com.example.Untraced", "order-untraced")

	sink := outboundkafka.NewRelaySink(brokers...)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOne(t, ctx, brokers, topic)
	if string(msg.Key) != "order-untraced" || string(msg.Value) != `{}` {
		t.Fatalf("row relayed changed: key=%q value=%q", msg.Key, msg.Value)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("a row without stored headers must relay with none, got %+v", msg.Headers)
	}
}

// kafkaHeaderCarrier adapts kafka-go headers to the OTel text-map carrier,
// as every inbound consumer of this repo does (headerCarrier in
// internal/adapters/inbound/kafka).
type kafkaHeaderCarrier []kafkago.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	v, _ := headerValue(c, key)
	return v
}

func (c kafkaHeaderCarrier) Set(string, string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
