//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

const (
	rcItTypeCreated  = "com.warehouse.wes.work-planning.workunit.WorkUnitCreated"
	rcItTypeReleased = "com.warehouse.wes.work-planning.workunit.WorkReleased"
)

// rcItEvent builds a WorkUnitCreated (or other work-planning) message exactly
// as wes-work-planning's encoder emits it: structured-mode CloudEvents 1.0,
// subject = the work unit id, dataschema v1.
func rcItEvent(t *testing.T, ceType, id, workUnitID string, data map[string]any) kafkago.Message {
	t.Helper()
	return kafkago.Message{
		Key: []byte(workUnitID),
		Value: ceEvent(t, ceSpec{
			id: id, source: "/warehouse/wes-work-planning", ceType: ceType, subject: workUnitID,
			at:         time.Date(2026, 10, 17, 9, 0, 5, 0, time.UTC),
			dataschema: "urn:warehouse:wes-work-planning:events:" + ceType[strings.LastIndex(ceType, ".")+1:] + ":v1",
		}, data),
	}
}

func rcItOrderData(orderID string, lineNo int) map[string]any {
	return map[string]any{
		"path_id": "pick-a", "work_unit_id": usecases.WorkUnitID(shared.OrderId(orderID), lineNo),
		"reference": orderID, "line_no": lineNo,
	}
}

// TestReleaseConfirmationConsumer_RealBrokerAndPostgres_EndToEnd proves ADR
// 0043 against a REAL Kafka broker and a REAL Postgres (testcontainers only):
// a WorkUnitCreated CloudEvent confirms the PENDING line it names; a replayed
// id is a no-op; work units that are not order work (today's payload without
// the new fields, a transfer reference, another event type) are ignored
// without blocking the partition; and poison -- a legacy-flat message and a
// malformed `data` -- is dead-lettered byte for byte on "<topic>.dlq".
func TestReleaseConfirmationConsumer_RealBrokerAndPostgres_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-relconf-itest-%d", time.Now().UnixNano())))
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

	topic := fmt.Sprintf("warehouse.work-planning.events.itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	createRepromiseTopic(t, ctx, brokers, dlqTopic)

	// Three released lines awaiting confirmation.
	repo := postgres.NewReleaseConfirmationRepo(pool)
	releasedAt := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	for _, l := range []struct {
		order string
		line  int
	}{{"ord-it-1", 1}, {"ord-it-1", 2}, {"ord-it-2", 1}} {
		c, err := releaseconfirmation.NewPending(shared.OrderId(l.order), l.line, usecases.WorkUnitID(shared.OrderId(l.order), l.line), "pick-a", releasedAt)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Open(ctx, c); err != nil {
			t.Fatal(err)
		}
	}

	confirm := &usecases.ConfirmRelease{
		Confirmations: repo, Processed: postgres.NewReleaseConfirmationProcessedEventsRepo(pool),
		Clock: memory.SystemClock{}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	groupID := fmt.Sprintf("om-release-confirmation-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewReleaseConfirmationConsumerForTopic(brokers, groupID, topic, confirm, nil)
	defer func() { _ = consumer.Close() }()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	poison := []byte(`{"event_id":"legacy-1","event_type":"WorkUnitCreated","occurred_at":"2026-09-01T00:00:00Z","source":"legacy","data":{}}`)
	malformed := rcItEvent(t, rcItTypeCreated, "evt-malformed", "ord-it-1-line-2",
		map[string]any{"path_id": "pick-a", "reference": "ord-it-1", "line_no": "two"})
	todaysPayload := rcItEvent(t, rcItTypeCreated, "evt-today", "wu-gold",
		map[string]any{"path_id": "pick-a", "work_unit_id": "wu-gold"})
	msgs := []kafkago.Message{
		{Key: []byte("legacy"), Value: poison}, // poison FIRST: it must not block what follows
		rcItEvent(t, rcItTypeCreated, "evt-line1", "ord-it-1-line-1", rcItOrderData("ord-it-1", 1)),
		rcItEvent(t, rcItTypeCreated, "evt-line1", "ord-it-1-line-1", rcItOrderData("ord-it-1", 1)), // a redelivery
		todaysPayload,
		rcItEvent(t, rcItTypeCreated, "evt-transfer", "tr-9-line-1", rcItOrderData("transfer-9", 1)),
		rcItEvent(t, rcItTypeReleased, "evt-released", "ord-it-1-line-2", rcItOrderData("ord-it-1", 2)), // another type
		malformed,
		// A sentinel for ANOTHER line, published last: once it is CONFIRMED,
		// everything before it on the (single) partition has been handled.
		rcItEvent(t, rcItTypeCreated, "evt-sentinel", "ord-it-2-line-1", rcItOrderData("ord-it-2", 1)),
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}}
	defer func() { _ = writer.Close() }()
	publishDeadline := time.Now().Add(30 * time.Second)
	for {
		err := writer.WriteMessages(ctx, msgs...)
		if err == nil {
			break
		}
		if time.Now().After(publishDeadline) {
			t.Fatalf("publish: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	state := func(orderID string, lineNo int) releaseconfirmation.State {
		c, found, err := repo.Find(ctx, shared.OrderId(orderID), lineNo)
		if err != nil || !found {
			t.Fatalf("Find(%s,%d) = found %v, err %v", orderID, lineNo, found, err)
		}
		return c.State()
	}
	waitFor(t, ctx, "the sentinel line to be CONFIRMED", func() bool {
		return state("ord-it-2", 1) == releaseconfirmation.StateConfirmed
	})

	// 1. The named line is CONFIRMED with the event's processing time.
	line1, _, _ := repo.Find(ctx, "ord-it-1", 1)
	if line1.State() != releaseconfirmation.StateConfirmed || line1.ConfirmedAt() == nil {
		t.Fatalf("ord-it-1 line 1 = %s %v, want CONFIRMED with a confirmation time", line1.State(), line1.ConfirmedAt())
	}
	// 2. The line only a foreign-type event and a malformed event named stays PENDING.
	if got := state("ord-it-1", 2); got != releaseconfirmation.StatePending {
		t.Fatalf("ord-it-1 line 2 = %s, want PENDING: neither WorkReleased nor a malformed WorkUnitCreated may confirm it", got)
	}
	// 3. One claim per applied id: evt-line1 (once, despite the redelivery)
	//    and the sentinel. Not today's payload, the transfer, the foreign
	//    type, the malformed event or the legacy-flat message.
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM release_confirmation_processed_events`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 2 {
		t.Fatalf("claimed ids = %d, want 2", claims)
	}

	// 4. Exactly the two poison messages reached the dead-letter topic, byte
	//    for byte, in order.
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
			t.Fatalf("dead-lettered message %d = %s, want the original poison message", i, dead.Value)
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
