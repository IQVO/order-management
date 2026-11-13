//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/domain/shared"
	"github.com/claudioed/order-management/internal/testing/pgtest"
)

// Postgres here is booted once per test binary by internal/testing/pgtest,
// which starts it through testcontainers-go/modules/postgres (fleet rule:
// never an external DSN, never a skip gate).
//
// Kafka is started inline through testcontainers-go/modules/kafka, the same
// recipe as internal/adapters/inbound/kafka's consumer integration tests.

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// freeAddr reserves an ephemeral loopback port for a server to listen on.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// waitUntil polls cond every 200ms until it holds or the 90s deadline passes.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// sigtermDrain stops the binary the way the pod's orchestrator does. The
// test registers its OWN SIGTERM handler first, so a signal landing before
// run() reaches its signal.Notify can never fall back to the default action
// and kill the whole test process; os/signal broadcasts to every registered
// channel, so a signal only our handler saw means run() missed it and the
// signal is retried until run() returns.
func sigtermDrain(t *testing.T, done <-chan error, timeout time.Duration) {
	t.Helper()
	relay := make(chan os.Signal, 1)
	signal.Notify(relay, syscall.SIGTERM)
	defer signal.Stop(relay)
	deadline := time.Now().Add(timeout)
	for {
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Fatalf("send SIGTERM: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a signalled projector must drain cleanly, got %v", err)
			}
			return
		case <-relay:
			// run() had not armed its own handler yet; retry shortly.
			time.Sleep(100 * time.Millisecond)
		case <-time.After(2 * time.Second):
			if time.Now().After(deadline) {
				t.Fatal("the projector did not stop")
			}
		}
	}
}

// createAnalyticsTopic creates topic on the test broker and waits for its
// partition leader, the recipe from the inbound kafka adapter's consumer
// integration tests.
func createAnalyticsTopic(ctx context.Context, brokers []string, topic string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial Kafka: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("Kafka controller: %w", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial Kafka controller: %w", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		return fmt.Errorf("create Kafka topic: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("Kafka topic %q leader was not ready: %w", topic, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestProjectorBootsMigratesProjectsAndStopsCleanly proves the WRITER half of
// the analytical data product (ADR-0006) end to end: the real run() boots
// against an EMPTY testcontainers Postgres and runs its own analytics
// migrations (the projector owns the schema), serves /healthz on its admin
// port, consumes the analytics topic the repo's own AnalyticsPublisher wrote
// real CloudEvents 1.0 envelopes to (ADR-0030), and projects exactly the
// documented funnel facts (docs/docs/analytics/order-funnel-report.md: each
// metric counts its source event per (path_id, hour_bucket); promise KPI
// columns come from OrderAllocated with a Capability basis whose
// promise_cutoff_at contributes one gap sample, ADR-0019; OrderRepromised is
// not path-dimensioned, ADR-0018). A SIGTERM drains to a clean nil return.
func TestProjectorBootsMigratesProjectsAndStopsCleanly(t *testing.T) {
	ctx := context.Background()

	// EMPTY analytical database: the projector must migrate it itself.
	url := pgtest.EmptyURL(t)

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-projector-itest-%d", time.Now().UnixNano())),
	)
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
	if err := createAnalyticsTopic(ctx, brokers, outboundkafka.AnalyticsTopic); err != nil {
		t.Fatalf("create analytics topic: %v", err)
	}

	// The repo's OWN producer: the real AnalyticsPublisher writes the real
	// CloudEvents 1.0 envelopes to the projector's input topic. Every event
	// below carries its path on the event itself (or none), the shapes the
	// publisher documents as needing no OrderRepo enrichment: the allocation
	// path is its first released line's, the line release carries its own
	// line's path, the re-promise carries none.
	publisher := outboundkafka.NewAnalyticsPublisher(brokers, nil, nil)
	t.Cleanup(func() { _ = publisher.Close() })

	hour := time.Date(2026, 11, 12, 14, 0, 0, 0, time.UTC)
	at := hour.Add(5 * time.Minute)
	cutoff := at.Add(90 * time.Minute) // gap operand distinct from every other number here
	orderID := shared.OrderId("ord-itcov-1")
	events := []shared.DomainEvent{
		shared.NewOrderAllocatedWithPromise(at, orderID, cutoff, "cpt-itcov-1", "Capability",
			[]shared.ReleasedLine{{LineNo: 1, SKU: shared.SKU("SKU-100"), PathID: shared.PathId("pick")}}),
		shared.NewOrderLineReleased(hour.Add(20*time.Minute), orderID, 2, shared.PathId("pack"), string(orderID)+"-line-2"),
		shared.NewOrderRepromised(hour.Add(40*time.Minute), orderID, "cpt-itcov-1", "cpt-itcov-2", "capacity"),
	}
	for _, ev := range events {
		if err := publisher.Publish(ctx, ev); err != nil {
			t.Fatalf("publish %T: %v", ev, err)
		}
	}

	// The documented env knobs of cmd/order-projector (main.go): the
	// migrations path is relative to the deployment's working directory, so
	// the test points it at the repo's own analytics migration set.
	addr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("KAFKA_BROKERS", brokers[0])
	t.Setenv("ADMIN_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", "../../migrations/analytics")

	done := make(chan error, 1)
	go func() { done <- run() }()

	// /healthz answers only after the migrations, pool ping and consumer
	// wiring succeeded — the admin probe is the boot proof.
	waitUntil(t, "/healthz 200", func() bool {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// The documented projection: OrderAllocated on ('pick', hour) with its
	// promise KPIs, OrderLineReleased on its own line's path ('pack'),
	// OrderRepromised on the path-less repromise_rollup.
	waitUntil(t, "the consumed events projected onto the funnel rollup", func() bool {
		var n int
		err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM funnel_rollup
			         WHERE path_id = 'pick' AND hour_bucket = $1
			           AND orders_allocated = 1 AND promise_basis_capability = 1
			           AND promise_to_cutoff_gap_seconds_sum = 5400 AND promise_to_cutoff_gap_samples = 1)
			     + (SELECT count(*) FROM funnel_rollup
			         WHERE path_id = 'pack' AND hour_bucket = $1 AND lines_released = 1)
			     + (SELECT count(*) FROM repromise_rollup
			         WHERE hour_bucket = $1 AND orders_repromised = 1)`, hour).Scan(&n)
		return err == nil && n == 3
	})

	// The process's own stop path: SIGTERM is what the pod receives; run()
	// must drain the consumer and the admin server and return nil.
	sigtermDrain(t, done, 30*time.Second)
}
