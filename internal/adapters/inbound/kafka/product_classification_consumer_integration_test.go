//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

const clsItType = "com.warehouse.wms.product-master.product.ProductClassified"

func clsItEvent(t *testing.T, ceType, id, sku string, data map[string]any) kafkago.Message {
	t.Helper()
	return kafkago.Message{
		Key: []byte(sku),
		Value: ceEvent(t, ceSpec{
			id: id, source: "/warehouse/product-master", ceType: ceType, subject: sku,
			at: time.Date(2026, 10, 6, 21, 2, 0, 0, time.UTC), dataschema: "urn:warehouse:product-master:events:ProductClassified:v1",
		}, data),
	}
}

func clsItData(sku string, version int64, tags ...string) map[string]any {
	return map[string]any{"sku": sku, "handling_tags": tags, "classification_source": "native", "version": version}
}

// clsItCatalogue is a one-path ports.ProcessPathCatalogue whose only active
// path EXCLUDES Hazmat, so a line's classification decides intake.
type clsItCatalogue struct{ eligibility shared.Eligibility }

func (c clsItCatalogue) IsActive(id shared.PathId) bool { return id == shared.DefaultPathId }
func (c clsItCatalogue) CycleTimeP95(shared.PathId) (time.Duration, bool) {
	return 0, false
}
func (c clsItCatalogue) Eligibility(id shared.PathId) (shared.Eligibility, bool) {
	return c.eligibility, id == shared.DefaultPathId
}
func (c clsItCatalogue) ListActive() []shared.ActivePathCandidate {
	return []shared.ActivePathCandidate{{PathId: shared.DefaultPathId, Eligibility: c.eligibility}}
}

// TestProductClassificationConsumer_RealBrokerAndPostgres_ReachesIntake
// proves ADR 0036 end to end against a REAL Kafka broker and a REAL
// Postgres (testcontainers only): product-master's ProductClassified
// messages are consumed into the Postgres copy (stale versions, other
// types, a replayed id and a non-CloudEvent are all handled), and
// ReceiveOrder — the use case that consumes classification — sees the
// copied tags through ports.ProductClassificationLookup: a SKU accepted
// before the event is rejected as ineligible after it.
func TestProductClassificationConsumer_RealBrokerAndPostgres_ReachesIntake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-classification-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kafkaC) })
	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}

	pgC, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_management"), tcpostgres.WithUsername("order_management"),
		tcpostgres.WithPassword("order_management"), tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start Postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pgC) })
	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(dsn, "../../../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	topic := fmt.Sprintf("warehouse.product-master.events.itest-%d", time.Now().UnixNano())
	createRepromiseTopic(t, ctx, brokers, topic)

	// Wired like cmd/order does for PRODUCT_CLASSIFICATION_MODE=kafka with Postgres.
	store := productclassificationcopy.NewPostgresStore(pool, nil)
	apply := &usecases.ApplyProductClassification{
		Copy: store, Processed: productclassificationcopy.NewPostgresProcessedEvents(pool), UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	groupID := fmt.Sprintf("om-product-classification-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewProductClassificationConsumerForTopic(brokers, groupID, topic, apply, nil)
	defer func() { _ = consumer.Close() }()
	runCtx, runCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	// The use case that consumes classification, reading the SAME copy.
	receive := &usecases.ReceiveOrder{
		Orders:         memory.NewOrderRepo(),
		Events:         &capturingPublisher{},
		Clock:          memory.NewFixedClock(time.Date(2026, 10, 6, 22, 0, 0, 0, time.UTC)),
		Inventory:      inventorystorage.NewPermissiveClient(),
		Promise:        order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)},
		Catalogue:      clsItCatalogue{eligibility: shared.NewEligibility(nil, nil, []string{"Hazmat"}, false)},
		Classification: store,
	}
	intake := func(sku shared.SKU) error {
		_, err := receive.Execute(ctx, []usecases.NewLine{{SKU: sku, Quantity: 1}}, false)
		return err
	}

	// Before any event: SKU-HZ is unknown to the copy, so intake fails open
	// and the order is accepted on the hazmat-excluding path.
	if err := intake("SKU-HZ"); err != nil {
		t.Fatalf("intake before any classification: %v, want accepted (fail-open)", err)
	}

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}}
	defer func() { _ = writer.Close() }()
	msgs := []kafkago.Message{
		// A non-CloudEvent first: it must not block what follows.
		{Key: []byte("legacy"), Value: legacyFlatMessage("legacy-1", "ProductClassified", `{"sku":"SKU-HZ"}`)},
		clsItEvent(t, "com.warehouse.wms.product-master.product.ProductRegistered", "evt-reg", "SKU-HZ",
			map[string]any{"sku": "SKU-HZ", "description": "Lithium battery", "version": 1}),
		clsItEvent(t, clsItType, "evt-hz-v3", "SKU-HZ", clsItData("SKU-HZ", 3, "Hazmat", "TemperatureSensitive")),
		// Stale (older version, arrived late) and a replayed id with new content.
		clsItEvent(t, clsItType, "evt-hz-v2", "SKU-HZ", clsItData("SKU-HZ", 2, "Fragile")),
		clsItEvent(t, clsItType, "evt-hz-v3", "SKU-HZ", clsItData("SKU-HZ", 9, "Oversized")),
		// Sentinel for ANOTHER SKU, published last on the single partition.
		clsItEvent(t, clsItType, "evt-sentinel", "SKU-SENTINEL", clsItData("SKU-SENTINEL", 1, "Fragile")),
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, ctx, "the sentinel SKU to reach the copy", func() bool {
		got, _ := store.GetClassification(ctx, "SKU-SENTINEL")
		return got.Known
	})

	// 1. The copy holds version 3's tags: the stale v2 and the replayed id
	//    were no-ops.
	got, err := store.GetClassification(ctx, "SKU-HZ")
	if err != nil || !got.Known || len(got.HandlingTags) != 2 || got.HandlingTags[0] != "Hazmat" || got.HandlingTags[1] != "TemperatureSensitive" {
		t.Fatalf("copy for SKU-HZ = %+v, %v; want version 3's [Hazmat TemperatureSensitive]", got, err)
	}
	var version int64
	if err := pool.QueryRow(ctx, `SELECT version FROM product_classification_copy WHERE sku = 'SKU-HZ'`).Scan(&version); err != nil || version != 3 {
		t.Fatalf("stored version = %d, %v; want 3", version, err)
	}

	// 2. Intake now sees Hazmat through the port and rejects the line on
	//    the hazmat-excluding path; an unclassified SKU still fails open.
	if err := intake("SKU-HZ"); !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
		t.Fatalf("intake after ProductClassified = %v, want ErrLineIneligibleForResolvedPath", err)
	}
	if err := intake("SKU-PLAIN"); err != nil {
		t.Fatalf("intake of an unclassified SKU: %v, want accepted", err)
	}

	// 3. One claim per applied-or-stale ProductClassified id: v3, v2,
	//    sentinel. Not the ProductRegistered, not the legacy message, not the
	//    replay twice.
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM product_classification_processed_events`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 3 {
		t.Fatalf("claimed ids = %d, want 3", claims)
	}

	runCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("consumer did not stop after cancellation")
	}
}
