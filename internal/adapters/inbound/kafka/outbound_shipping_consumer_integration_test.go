//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	logpub "github.com/claudioed/order-management/internal/adapters/outbound/events"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

const (
	osItTypeConfirmed = "com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed"
	osItTypeMissed    = "com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad"
	osItTypeBol       = "com.warehouse.wms.outbound-shipping.billoflading.BillOfLadingIssued"
	osItSchemaRoot    = "urn:warehouse:outbound-shipping:events:"
)

// osItConfirmed builds one ShipmentConfirmed exactly as
// outbound-shipping's encoder emits it: structured-mode CloudEvents 1.0,
// subject = the load id, the REAL payload's full field set (including
// every member this consumer deliberately ignores).
func osItConfirmed(t *testing.T, id, loadID string, orderIDs ...string) kafkago.Message {
	t.Helper()
	orders := make([]map[string]any, 0, len(orderIDs))
	for _, oid := range orderIDs {
		orders = append(orders, map[string]any{"order_id": oid})
	}
	data := map[string]any{
		"load_id": loadID, "site_id": "SITE1", "cpt_id": "sp1-1800", "carrier": "CARRIER-X",
		"trailer_id": "trailer-9", "seal_number": "seal-4",
		"departed_at": "2026-11-06T17:05:00Z", "orders": orders, "version": 3,
	}
	return kafkago.Message{
		Key: []byte(loadID),
		Value: ceEvent(t, ceSpec{
			id: id, source: "/warehouse/outbound-shipping", ceType: osItTypeConfirmed, subject: loadID,
			at:         time.Date(2026, 11, 6, 17, 5, 0, 0, time.UTC),
			dataschema: osItSchemaRoot + "ShipmentConfirmed:v1",
		}, data),
	}
}

func osItMissed(t *testing.T, id, packageID, orderID string) kafkago.Message {
	t.Helper()
	data := map[string]any{
		"package_id": packageID, "load_id": "load-os-it-2", "site_id": "SITE1",
		"source_order_id": orderID, "cutoff": "2026-11-06T16:00:00Z",
		"detected_at": "2026-11-06T16:05:00Z", "version": 2,
	}
	return kafkago.Message{
		Key: []byte(packageID),
		Value: ceEvent(t, ceSpec{
			id: id, source: "/warehouse/outbound-shipping", ceType: osItTypeMissed, subject: packageID,
			at:         time.Date(2026, 11, 6, 16, 5, 0, 0, time.UTC),
			dataschema: osItSchemaRoot + "PackageMissedLoad:v1",
		}, data),
	}
}

// osItOrder persists one two-line order with every line driven to the
// given status through the domain's own transitions, plus one promise
// group stamped the way a real allocation leaves it (24h lead time, so
// the missed-load recompute's 6h policy has something to move to).
func osItOrder(t *testing.T, ctx context.Context, repo *postgres.OrderRepo, id string, status order.LineStatus) *order.Order {
	t.Helper()
	l1, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := order.NewOrderLine(2, "SKU-2", 2, "pick", false)
	if err != nil {
		t.Fatal(err)
	}
	o, err := order.New(shared.OrderId(id), []*order.OrderLine{l1, l2}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range o.Lines() {
		switch status {
		case order.LineReleased:
			if err := o.Allocate(l.LineNo(), fmt.Sprintf("res-%s-%d", id, l.LineNo())); err != nil {
				t.Fatal(err)
			}
			if err := o.Release(l.LineNo()); err != nil {
				t.Fatal(err)
			}
		case order.LinePending:
			// nothing
		default:
			t.Fatalf("unsupported fixture status %q", status)
		}
	}
	if status == order.LineReleased {
		now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
		o.SetPromiseGroups([]order.PromiseGroup{{
			LineNos: []int{1, 2},
			Promise: order.Promise{CutoffAt: now.Add(24 * time.Hour), Basis: order.BasisLeadTime},
		}})
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("save %s: %v", id, err)
	}
	return o
}

// TestOutboundShippingConsumer_RealBrokerAndPostgres_EndToEnd proves ADR
// 0051 against a REAL Kafka broker and a REAL Postgres (testcontainers
// only). One ordered batch with a sentinel at the end:
//
//   - poison FIRST (a legacy flat envelope): dead-lettered byte for
//     byte, and it must not block anything behind it;
//   - a ShipmentConfirmed ships a Released order through the full round
//     trip consumer -> aggregate -> read side (GetOrder), and its
//     redelivery under the SAME CloudEvents id changes nothing;
//   - a ShipmentConfirmed naming an UNKNOWN order is skipped-and-logged,
//     committed, never dead-lettered;
//   - a PackageMissedLoad re-promises the named order's whole group
//     (24h -> 6h) without shipping anything;
//   - a foreign type on the topic is committed past;
//   - a malformed ShipmentConfirmed is dead-lettered;
//   - the SENTINEL ShipmentConfirmed ships a second Released order:
//     once it is Shipped, everything before it was handled.
func TestOutboundShippingConsumer_RealBrokerAndPostgres_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-outbound-itest-%d", time.Now().UnixNano())))
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

	topic := fmt.Sprintf("warehouse.outbound-shipping.events.itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	createRepromiseTopic(t, ctx, brokers, dlqTopic)

	orders := postgres.NewOrderRepo(pool)
	shipped := osItOrder(t, ctx, orders, "ord-os-it-1", order.LineReleased)
	pending := osItOrder(t, ctx, orders, "ord-os-it-2", order.LinePending)
	missed := osItOrder(t, ctx, orders, "ord-os-it-3", order.LineReleased)
	sentinel := osItOrder(t, ctx, orders, "ord-os-it-4", order.LineReleased)
	originalCutoff := missed.PromiseGroups()[0].Promise.CutoffAt

	publisher := logpub.NewLogPublisher(slog.Default())
	confirm := &usecases.ConfirmShipment{
		Orders: orders, Events: publisher, Clock: memory.SystemClock{},
		Processed:  postgres.NewShipmentProcessedEventsRepo(pool),
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	// 6h fallback vs the stored 24h group: the missed-load recompute has
	// a genuinely different commitment to move to.
	repromise := &usecases.RepromiseOrder{
		Orders: orders, Events: publisher, Clock: memory.SystemClock{},
		Promise:    order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)},
		Processed:  postgres.NewRepromiseProcessedEventsRepo(pool),
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	groupID := fmt.Sprintf("om-outbound-shipping-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewOutboundShippingConsumerForTopic(brokers, groupID, topic, confirm, repromise, nil)
	defer func() { _ = consumer.Close() }()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	poison := legacyFlatMessage("legacy-os-1", osItTypeConfirmed, `{"load_id":"load-os-it-1"}`)
	malformed := osItConfirmed(t, "evt-os-malformed", "load-os-it-9", "ord-os-it-1")
	malformed.Value = ceEvent(t, ceSpec{
		id: "evt-os-malformed", source: "/warehouse/outbound-shipping", ceType: osItTypeConfirmed, subject: "load-os-it-9",
		at: time.Date(2026, 11, 6, 17, 5, 0, 0, time.UTC), dataschema: osItSchemaRoot + "ShipmentConfirmed:v1",
	}, map[string]any{"load_id": "load-os-it-9", "orders": "not-a-list"})
	bol := kafkago.Message{
		Key: []byte("bol-os-it-1"),
		Value: ceEvent(t, ceSpec{
			id: "evt-os-bol", source: "/warehouse/outbound-shipping", ceType: osItTypeBol, subject: "bol-os-it-1",
			at: time.Date(2026, 11, 6, 17, 6, 0, 0, time.UTC), dataschema: osItSchemaRoot + "BillOfLadingIssued:v1",
		}, map[string]any{"bol_number": "BOL-1", "load_id": "load-os-it", "package_count": 3}),
	}

	msgs := []kafkago.Message{
		{Key: []byte("legacy"), Value: poison}, // poison FIRST: must not block what follows
		osItConfirmed(t, "evt-os-1", "load-os-it-1", shipped.ID().String()),
		osItConfirmed(t, "evt-os-1", "load-os-it-1", shipped.ID().String()),  // redelivery, same id
		osItConfirmed(t, "evt-os-2", "load-os-it-1", "ord-os-never"),         // unknown order
		osItConfirmed(t, "evt-os-2b", "load-os-it-1", pending.ID().String()), // not shippable
		osItMissed(t, "evt-os-3", "pkg-os-it-1", missed.ID().String()),
		bol, // foreign type
		{Key: malformed.Key, Value: malformed.Value}, // malformed data
		osItConfirmed(t, "evt-os-sentinel", "load-os-it-8", sentinel.ID().String()),
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}}
	defer func() { _ = writer.Close() }()
	publishDeadline := time.Now().Add(30 * time.Second)
	for {
		if err := writer.WriteMessages(ctx, msgs...); err == nil {
			break
		}
		if time.Now().After(publishDeadline) {
			t.Fatalf("publish: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	getOrder := &usecases.GetOrder{Orders: orders}
	waitFor(t, ctx, "the sentinel order to reach Shipped (everything before it handled)", func() bool {
		o, err := getOrder.Execute(ctx, sentinel.ID())
		return err == nil && o != nil && o.Status() == order.StatusShipped
	})

	// 1. The round trip: ShipmentConfirmed -> aggregate -> read side.
	shippedRead, err := getOrder.Execute(ctx, shipped.ID())
	if err != nil {
		t.Fatalf("read side: %v", err)
	}
	if shippedRead.Status() != order.StatusShipped {
		t.Fatalf("ord-os-it-1 status = %q, want Shipped", shippedRead.Status())
	}
	for _, l := range shippedRead.Lines() {
		if l.Status() != order.LineShipped {
			t.Fatalf("ord-os-it-1 line %d = %q, want Shipped", l.LineNo(), l.Status())
		}
	}

	// 2. Idempotency: evt-os-1 claimed ONCE despite the redelivery; the
	// unknown order, the not-shippable order and the sentinel each claim
	// once. The foreign type, the missed load and the malformed/poison
	// messages claim nothing here.
	var gateRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM shipment_processed_events`).Scan(&gateRows); err != nil {
		t.Fatal(err)
	}
	if gateRows != 4 {
		t.Fatalf("shipment_processed_events rows = %d, want 4 (evt-os-1 once despite the redelivery, evt-os-2, evt-os-2b, the sentinel; the foreign type, missed load and poison claim nothing)", gateRows)
	}

	// 3. The pending order never transitioned.
	pendingRead, _ := getOrder.Execute(ctx, pending.ID())
	if pendingRead.Status() == order.StatusShipped || pendingRead.Status() == order.StatusPartiallyShipped {
		t.Fatalf("ord-os-it-2 status = %q, a not-shippable order must not transition", pendingRead.Status())
	}

	// 4. The missed load re-promised the WHOLE group (24h -> 6h) without
	// shipping anything, and its id sits in the repromise gate.
	missedRead, _ := orders.FindByID(ctx, missed.ID())
	groups := missedRead.PromiseGroups()
	if len(groups) != 1 {
		t.Fatalf("ord-os-it-3 groups = %d, want 1", len(groups))
	}
	if groups[0].Promise.CutoffAt.Equal(originalCutoff) {
		t.Fatalf("ord-os-it-3 cutoff = %v, want it moved from %v", groups[0].Promise.CutoffAt, originalCutoff)
	}
	if missedRead.Status() != order.StatusReleased {
		t.Fatalf("ord-os-it-3 status = %q, want Released (a missed load ships nothing)", missedRead.Status())
	}
	var repromiseRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repromise_processed_events`).Scan(&repromiseRows); err != nil {
		t.Fatal(err)
	}
	if repromiseRows != 1 {
		t.Fatalf("repromise_processed_events rows = %d, want 1", repromiseRows)
	}

	// 5. Exactly the two poison messages reached the dead-letter topic,
	// byte for byte, in order.
	dlq := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer func() { _ = dlq.Close() }()
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	for i, want := range [][]byte{poison, malformed.Value} {
		dead, err := dlq.ReadMessage(dlqCtx)
		if err != nil {
			t.Fatalf("read dead-letter message %d: %v", i, err)
		}
		if string(dead.Value) != string(want) {
			t.Fatalf("dead-lettered message %d = %s, want the original poison bytes", i, dead.Value)
		}
	}
	extraCtx, extraCancel := context.WithTimeout(ctx, 3*time.Second)
	defer extraCancel()
	if extra, err := dlq.ReadMessage(extraCtx); err == nil {
		t.Fatalf("an unexpected third dead-letter message: key=%q value=%s", extra.Key, extra.Value)
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
