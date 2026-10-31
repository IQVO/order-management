//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/kafkacptschedule"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR 0047 end to end on real infrastructure: process-path-management's
// CPTScheduleChanged for TWO sites goes through a real Kafka broker into the
// real kafkacptschedule cache; orders are received and re-promised through
// the real Postgres repository and outbox. An order carrying
// fulfillmentSiteId SIM1 is promised (and re-promised) on SIM1's schedule,
// an order without one on DEFAULT_SITE_ID's (site-1), and a schedule change
// at one site never moves the other site's orders.

const cptScheduleChangedType = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"

// fixedPathCapability answers one cycle time for every path.
type fixedPathCapability time.Duration

func (c fixedPathCapability) CycleTimeP95(shared.PathId) (time.Duration, bool) {
	return time.Duration(c), true
}

func cptScheduleMessage(t *testing.T, id, site, cptID, localTime string) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/process-path-management")
	e.SetType(cptScheduleChangedType)
	e.SetSubject(site)
	e.SetTime(time.Now().UTC())
	data := map[string]any{
		"site_id":  site,
		"timezone": "UTC",
		"cutoffs":  []map[string]any{{"cpt_id": cptID, "local_time": localTime, "eligible_path_ids": []string{"pick"}}},
	}
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal CloudEvent: %v", err)
	}
	return kafkago.Message{Key: []byte(site), Value: raw}
}

// waitForCutoff polls the cache until site's first window is cptID.
func waitForCutoff(ctx context.Context, t *testing.T, cache *kafkacptschedule.Consumer, site, cptID string, from time.Time) {
	t.Helper()
	for {
		if w, ok := cache.NextCutoffs(site, from, 1); ok && len(w) == 1 && w[0].CptId == cptID {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("CPT cache never showed %s at %s", cptID, site)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestPromise_CPTCacheKeyedByTwoSites_RealKafkaAndPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := outboxDB(t)

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-promise-site-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest-%d", time.Now().UnixNano())
	createTopics(ctx, t, brokers[0], topic)

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = writer.Close() })
	if err := writer.WriteMessages(ctx,
		cptScheduleMessage(t, "evt-site1-v1", "site-1", "site1-1000", "10:00"),
		cptScheduleMessage(t, "evt-sim1-v1", "SIM1", "sim1-1400", "14:00"),
	); err != nil {
		t.Fatalf("publish schedules: %v", err)
	}

	cache, err := kafkacptschedule.NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("NewConsumerForTopic: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- cache.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done; _ = cache.Close() })
	if err := cache.WaitReady(ctx); err != nil {
		t.Fatalf("CPT cache never became ready: %v", err)
	}

	// 08:00 UTC: site-1 cuts off at 10:00, SIM1 at 14:00 — different answers.
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	clock := fixedClock{t: now}
	policy := order.PromisePolicy{
		Schedule:      cache,
		Capability:    fixedPathCapability(30 * time.Minute),
		Fallback:      order.NewLeadTimePolicy(48*time.Hour, nil),
		DefaultSiteId: "site-1",
	}
	orders := postgres.NewOrderRepo(pool)
	outbox := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisher(nil))
	uow := postgres.NewUnitOfWork(pool)
	receive := &usecases.ReceiveOrder{Orders: orders, Events: outbox, Clock: clock, Inventory: allocatingInventory{}, Promise: policy, UnitOfWork: uow}
	lines := []usecases.NewLine{{SKU: "sku-1", Quantity: 1, PathID: "pick"}}
	sim1 := shared.FulfillmentSiteID("SIM1")

	atSIM1, err := receive.ExecuteWithSite(ctx, lines, false, true, nil, &sim1)
	if err != nil {
		t.Fatalf("receive at SIM1: %v", err)
	}
	atDefault, err := receive.ExecuteWithSite(ctx, lines, false, true, nil, nil)
	if err != nil {
		t.Fatalf("receive without site: %v", err)
	}
	assertStoredCpt(ctx, t, orders, atSIM1.ID(), "sim1-1400", now.Add(6*time.Hour))
	assertStoredCpt(ctx, t, orders, atDefault.ID(), "site1-1000", now.Add(2*time.Hour))

	// SIM1's schedule changes on the wire; site-1's does not.
	if err := writer.WriteMessages(ctx, cptScheduleMessage(t, "evt-sim1-v2", "SIM1", "sim1-1600", "16:00")); err != nil {
		t.Fatalf("publish SIM1 update: %v", err)
	}
	waitForCutoff(ctx, t, cache, "SIM1", "sim1-1600", now)
	waitForCutoff(ctx, t, cache, "site-1", "site1-1000", now)

	repromise := &usecases.RepromiseOrder{
		Orders: orders, Promise: policy, Events: outbox, Clock: clock,
		Processed: postgres.NewRepromiseProcessedEventsRepo(pool), UnitOfWork: uow,
	}
	for i, id := range []shared.OrderId{atSIM1.ID(), atDefault.ID()} {
		if err := repromise.Execute(ctx, usecases.RepromiseOrderRequest{
			SourceEventId: fmt.Sprintf("evt-missed-%d", i), OrderId: id, LineNo: 1, Reason: "TaskCPTMissed",
		}); err != nil {
			t.Fatalf("repromise %s: %v", id, err)
		}
	}
	assertStoredCpt(ctx, t, orders, atSIM1.ID(), "sim1-1600", now.Add(8*time.Hour))
	assertStoredCpt(ctx, t, orders, atDefault.ID(), "site1-1000", now.Add(2*time.Hour))

	var repromised int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE event_type = 'com.warehouse.wes.order-management.order.OrderRepromised'`).Scan(&repromised); err != nil {
		t.Fatalf("count OrderRepromised: %v", err)
	}
	if repromised != 1 {
		t.Fatalf("OrderRepromised rows = %d, want exactly 1 (the SIM1 order only)", repromised)
	}
}

// assertStoredCpt reloads id from Postgres and checks its promise.
func assertStoredCpt(ctx context.Context, t *testing.T, orders *postgres.OrderRepo, id shared.OrderId, wantCpt string, wantCutoff time.Time) {
	t.Helper()
	o, err := orders.FindByID(ctx, id)
	if err != nil || o == nil {
		t.Fatalf("FindByID(%s): %v, %v", id, o, err)
	}
	if c := o.PromiseCptId(); c == nil || *c != wantCpt {
		t.Fatalf("order %s PromiseCptId = %v, want %s", id, c, wantCpt)
	}
	if d := o.PromiseDate(); d == nil || !d.Equal(wantCutoff) {
		t.Fatalf("order %s PromiseDate = %v, want %v", id, d, wantCutoff)
	}
}
