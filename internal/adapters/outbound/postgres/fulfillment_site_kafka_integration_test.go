//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

type demandWire struct {
	SourceOrderID     string `json:"source_order_id"`
	SiteID            string `json:"site_id"`
	State             string `json:"state"`
	AssignmentVersion string `json:"assignment_version"`
}

type publishedEvent struct {
	Type    string
	Subject string
	Data    json.RawMessage
}

// createTopics creates the topics up front so the first relayed message
// does not race the broker's auto-creation, and waits until each has a
// leader.
func createTopics(ctx context.Context, t *testing.T, broker string, topics ...string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, topic := range topics {
		if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID >= 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic %s never became ready", topic)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// readIntegrationTopic reads the order-management integration topic until
// want messages have been seen (or the context expires).
func readIntegrationTopic(ctx context.Context, t *testing.T, brokers []string, want int) []publishedEvent {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: outboundkafka.Topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	var out []publishedEvent
	for len(out) < want {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read integration topic (got %d of %d): %v", len(out), want, err)
		}
		e, err := cloudevents.Decode(msg.Value)
		if err != nil {
			t.Fatalf("not a CloudEvent: %v (%s)", err, msg.Value)
		}
		out = append(out, publishedEvent{Type: e.Type(), Subject: e.Subject(), Data: e.Data()})
	}
	return out
}

// TestFulfillmentSite_ProjectsThroughRealPostgresOutboxToRealKafka drives
// the real use cases over the real OrderRepo / UnitOfWork / outbox, relays
// the outbox to a real Kafka broker and reads the wire back: an order with
// a caller-assigned site yields order-site-v1 at THAT site, one without
// falls back to the static site, and the frozen OrderAllocated payload is
// shaped identically for both (the site never leaks into it).
func TestFulfillmentSite_ProjectsThroughRealPostgresOutboxToRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := outboxDB(t)

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-fulfillment-site-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}

	createTopics(ctx, t, brokers[0], outboundkafka.Topic, outboundkafka.AnalyticsTopic)

	orders := postgres.NewOrderRepo(pool)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt" }}
	outbox := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisher(nil), analytics)
	uow := postgres.NewUnitOfWork(pool)
	static := usecases.DemandProjectionPolicy{SiteID: "SIM1", AssignmentVersion: usecases.StaticDemandAssignmentVersion}
	receive := &usecases.ReceiveOrder{
		Orders: orders, Events: outbox, Clock: fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		Inventory: allocatingInventory{}, UnitOfWork: uow, DemandProjection: static,
	}
	cancelUC := &usecases.CancelOrder{
		Orders: orders, Inventory: allocatingInventory{}, Events: outbox, Clock: fixedClock{t: time.Now().UTC()},
		DemandProjection: static, UnitOfWork: uow,
	}
	lines := []usecases.NewLine{{SKU: "sku-1", Quantity: 3, PathID: "pick"}}
	site := func(code string) *shared.FulfillmentSiteID { s := shared.FulfillmentSiteID(code); return &s }

	withSite, err := receive.ExecuteWithSite(ctx, lines, true, true, nil, site("WH2"))
	if err != nil {
		t.Fatalf("receive with site: %v", err)
	}
	without, err := receive.ExecuteWithSite(ctx, lines, true, true, nil, nil)
	if err != nil {
		t.Fatalf("receive without site: %v", err)
	}
	held, err := receive.ExecuteWithSite(ctx, lines, false, false, nil, site("DC7"))
	if err != nil {
		t.Fatalf("receive held with site: %v", err)
	}
	if _, err := cancelUC.Execute(ctx, held.ID()); err != nil {
		t.Fatalf("cancel held: %v", err)
	}

	sink := outboundkafka.NewRelaySink(brokers...)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, nil)
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}

	var integrationRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE topic = $1`, outboundkafka.Topic).Scan(&integrationRows); err != nil {
		t.Fatalf("count: %v", err)
	}
	events := readIntegrationTopic(ctx, t, brokers, integrationRows)

	demand := map[string][]demandWire{}
	allocatedKeys := map[string][]string{}
	for _, e := range events {
		switch e.Type {
		case "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged":
			var d demandWire
			if err := json.Unmarshal(e.Data, &d); err != nil {
				t.Fatalf("demand data: %v", err)
			}
			demand[d.SourceOrderID] = append(demand[d.SourceOrderID], d)
		case "com.warehouse.wes.order-management.order.OrderAllocated":
			var m map[string]json.RawMessage
			if err := json.Unmarshal(e.Data, &m); err != nil {
				t.Fatalf("allocated data: %v", err)
			}
			for k := range m {
				allocatedKeys[e.Subject] = append(allocatedKeys[e.Subject], k)
			}
			sort.Strings(allocatedKeys[e.Subject])
		}
	}

	if got := demand[withSite.ID().String()]; len(got) != 1 || got[0].SiteID != "WH2" || got[0].AssignmentVersion != "order-site-v1" || got[0].State != "ACTIVE" {
		t.Fatalf("order with site: demand = %+v, want one ACTIVE WH2 order-site-v1", got)
	}
	if got := demand[without.ID().String()]; len(got) != 1 || got[0].SiteID != "SIM1" || got[0].AssignmentVersion != "static-site-v1" {
		t.Fatalf("order without site: demand = %+v, want one SIM1 static-site-v1", got)
	}
	if got := demand[held.ID().String()]; len(got) != 2 || got[0].State != "ACTIVE" || got[1].State != "REMOVED" ||
		got[0].SiteID != "DC7" || got[1].SiteID != "DC7" || got[1].AssignmentVersion != "order-site-v1" {
		t.Fatalf("held order: demand = %+v, want ACTIVE then REMOVED at DC7 order-site-v1", got)
	}
	a, b := allocatedKeys[withSite.ID().String()], allocatedKeys[without.ID().String()]
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("OrderAllocated payload keys differ: with site %v, without %v", a, b)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("OrderAllocated payload keys differ: with site %v, without %v", a, b)
		}
	}
}
