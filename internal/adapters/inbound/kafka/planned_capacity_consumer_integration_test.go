//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

const (
	pcItPlanID   = "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10"
	pcItTypeNew  = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated"
	pcItTypePub  = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished"
	pcItTypeShrt = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected"
	pcItTypeBott = "com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected"
)

func pcItEvent(t *testing.T, ceType, id, planID string, at time.Time, data map[string]any) kafkago.Message {
	t.Helper()
	return kafkago.Message{
		Key: []byte(planID),
		Value: ceEvent(t, ceSpec{
			id: id, source: "/warehouse/warehouse-planning", ceType: ceType, subject: planID, at: at,
			dataschema: "urn:warehouse:warehouse-planning:events:" + ceType[strings.LastIndex(ceType, ".")+1:] + ":v1",
		}, data),
	}
}

func pcItData(planID string, shortage float64) map[string]any {
	return map[string]any{
		"plan_id": planID, "warehouse_id": "WH-1", "location": "SIM1", "path_id": "pick-rebin-pack",
		"window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z",
		"assigned_demand": 12000, "capacity_over_window": 8000, "shortage": shortage, "bottleneck_step": "REBIN",
	}
}

// TestPlannedCapacityConsumer_RealBrokerAndPostgres_EndToEnd proves ADR 0031
// against a REAL Kafka broker and a REAL Postgres (testcontainers only):
// real CloudEvents messages in warehouse-planning's exact published shape
// are consumed into the Postgres read model through the transactional
// claim, a legacy-flat poison message is dead-lettered without blocking the
// partition, a replayed id is a no-op, and the real HTTP surface then shows
// the capacity-constrained annotation on an order whose promise overlaps the
// published shortage.
func TestPlannedCapacityConsumer_RealBrokerAndPostgres_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-plancap-itest-%d", time.Now().UnixNano())))
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

	topic := fmt.Sprintf("warehouse.warehouse-planning.events.itest-%d", time.Now().UnixNano())
	createRepromiseTopic(t, ctx, brokers, topic)

	// The consumer under test, wired like cmd/order does for Postgres.
	windows := postgres.NewPlannedCapacityRepo(pool)
	apply := &usecases.ApplyPlannedCapacity{
		Windows: windows, Processed: postgres.NewPlannedCapacityProcessedEventsRepo(pool), UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	groupID := fmt.Sprintf("om-planned-capacity-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewPlannedCapacityConsumerForTopic(brokers, groupID, topic, apply, nil)
	defer func() { _ = consumer.Close() }()
	runCtx, runCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	// The real HTTP surface over the real read model; an order promised
	// for 2026-10-06T09:00Z, i.e. AFTER the published shortage window.
	clock := memory.NewFixedClock(time.Date(2026, 10, 4, 21, 15, 30, 0, time.UTC))
	orders := memory.NewOrderRepo()
	line, err := order.NewOrderLine(1, "SKU-9", 3, "pick", false)
	if err != nil {
		t.Fatal(err)
	}
	o, err := order.New("ord-plancap-itest", []*order.OrderLine{line}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Allocate(1, "res-41"); err != nil {
		t.Fatal(err)
	}
	o.SetPromiseDate(time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC))
	if err := orders.Save(ctx, o); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		GetOrder:            &usecases.GetOrder{Orders: orders},
		CapacityConstraints: &usecases.OrderCapacityConstraints{Windows: windows, Clock: clock, SiteID: "SIM1"},
		PlannedCapacity:     &usecases.GetPlannedCapacity{Windows: windows, Clock: clock},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), ""))
	defer srv.Close()

	getBody := func(path string) string {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, resp.StatusCode, b)
		}
		return string(b)
	}

	// Before any event: the order has no annotation (behaviour as before).
	if body := getBody("/orders/ord-plancap-itest"); strings.Contains(body, "capacityConstraint") {
		t.Fatalf("no events consumed yet, but the order is annotated: %s", body)
	}

	// Publish: a legacy-flat poison message FIRST (must not block what
	// follows), then the real plan lifecycle, a replay and a bottleneck.
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}}
	defer func() { _ = writer.Close() }()
	t0 := time.Date(2026, 10, 4, 21, 15, 30, 0, time.UTC)
	t1 := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
	created := pcItData(pcItPlanID, 4000)
	created["path_capacity"], created["status"] = 1000, "DRAFT"
	published := pcItData(pcItPlanID, 4000)
	published["path_capacity"], published["published_at"] = 1000, t1.Format(time.RFC3339)
	replayChanged := pcItData(pcItPlanID, 1)

	poison := []byte(`{"event_id":"legacy-1","event_type":"CapacityShortageDetected","occurred_at":"2026-09-01T00:00:00Z","source":"legacy","data":{}}`)
	msgs := []kafkago.Message{
		{Key: []byte("legacy"), Value: poison},
		pcItEvent(t, pcItTypeNew, "evt-created", pcItPlanID, t0, created),
		pcItEvent(t, pcItTypePub, "evt-published", pcItPlanID, t1, published),
		pcItEvent(t, pcItTypeShrt, "evt-shortage", pcItPlanID, t1, pcItData(pcItPlanID, 4000)),
		pcItEvent(t, pcItTypeBott, "evt-bottleneck", pcItPlanID, t1, map[string]any{"plan_id": pcItPlanID, "bottleneck_step": "REBIN", "path_capacity": 1000}),
		// Same id as the shortage above, different content: a redelivery.
		pcItEvent(t, pcItTypeShrt, "evt-shortage", pcItPlanID, t1.Add(time.Hour), replayChanged),
		// A sentinel for ANOTHER plan, published last: once it is visible,
		// everything before it on the (single) partition has been handled.
		pcItEvent(t, pcItTypeShrt, "evt-sentinel", "sentinel-plan", t1, func() map[string]any {
			d := pcItData("sentinel-plan", 7)
			d["location"] = "SIM9"
			return d
		}()),
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, ctx, "the sentinel plan to reach the read model", func() bool {
		return strings.Contains(getBody("/planned-capacity?site=SIM9&from=2026-10-01T00:00:00Z"), "sentinel-plan")
	})

	// 1. The order response shows the annotation, explained by the plan.
	body := getBody("/orders/ord-plancap-itest")
	for _, want := range []string{
		`"capacityConstraint":{"constrained":true,"site":"SIM1"`, `"planId":"` + pcItPlanID + `"`,
		`"windowStart":"2026-10-05T08:00:00Z"`, `"windowEnd":"2026-10-05T16:00:00Z"`, `"shortage":4000`, `"bottleneckStep":"REBIN"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("order response lacks %s: %s", want, body)
		}
	}

	// 2. The read endpoint shows the plan PUBLISHED with the ORIGINAL shortage
	//    (the replayed id with shortage=1 and a later time was a no-op).
	var listing struct {
		Windows []struct {
			PlanID   string  `json:"planId"`
			Status   string  `json:"status"`
			Shortage float64 `json:"shortage"`
		} `json:"windows"`
	}
	if err := json.Unmarshal([]byte(getBody("/planned-capacity?site=SIM1")), &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Windows) != 1 || listing.Windows[0].PlanID != pcItPlanID ||
		listing.Windows[0].Status != "PUBLISHED" || listing.Windows[0].Shortage != 4000 {
		t.Fatalf("listing = %+v, want exactly the published plan with shortage 4000", listing)
	}

	// 3. The idempotency table holds one claim per applied id: Created,
	//    Published, Shortage (once), sentinel. NOT the bottleneck (ignored),
	//    NOT the legacy-flat message, NOT the replay twice.
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM planned_capacity_processed_events`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 4 {
		t.Fatalf("claimed ids = %d, want 4", claims)
	}

	// 4. The poison message reached the dead-letter topic, byte-identical.
	dlq := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic + ".dlq", Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer func() { _ = dlq.Close() }()
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	dead, err := dlq.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read dead-letter topic: %v", err)
	}
	if string(dead.Value) != string(poison) {
		t.Fatalf("dead-lettered value = %s, want the original poison message", dead.Value)
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

func waitFor(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context done waiting for %s: %v", what, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
