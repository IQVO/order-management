package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// Fixtures lifted from warehouse-planning's published AsyncAPI examples.
const (
	pcPlanID    = "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10"
	pcEventID   = "3b2a1c0d-9e8f-4d7c-b6a5-4f3e2d1c0b9a"
	pcTypeShort = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected"
	pcTypePub   = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished"
	pcTypeNew   = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated"
	pcTypeBott  = "com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected"
)

var (
	pcEventAt = time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
	pcWinFrom = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	pcWinTo   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
)

func pcData() map[string]any {
	return map[string]any{
		"plan_id": pcPlanID, "warehouse_id": "WH-1", "location": "SIM1", "path_id": "pick-rebin-pack",
		"window_start": pcWinFrom.Format(time.RFC3339), "window_end": pcWinTo.Format(time.RFC3339),
		"assigned_demand": 12000, "capacity_over_window": 8000, "shortage": 4000, "bottleneck_step": "REBIN",
	}
}

// pcMsg builds a real CloudEvents 1.0 structured-mode Kafka message with the
// official SDK, exactly the wire format warehouse-planning's encoder emits.
func pcMsg(t *testing.T, offset int64, ceType, id, subject string, at time.Time, data any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/warehouse-planning")
	e.SetType(ceType)
	e.SetSubject(subject)
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:warehouse-planning:events:X:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Offset: offset, Key: []byte(subject), Value: b}
}

// kpcStore: one in-memory read model + idempotency gate + UnitOfWork with
// real rollback (see the usecases test twin for the rationale).
type kpcStore struct {
	mu        sync.Mutex
	windows   map[string]order.PlannedCapacityWindow
	claimed   map[string]bool
	upsertErr func(call int) error // nil => never fails
	upserts   int
}

func newKPCStore() *kpcStore {
	return &kpcStore{windows: map[string]order.PlannedCapacityWindow{}, claimed: map[string]bool{}}
}

func (s *kpcStore) Execute(ctx context.Context, fn func(context.Context) error) error {
	s.mu.Lock()
	winSnap := map[string]order.PlannedCapacityWindow{}
	for k, v := range s.windows {
		winSnap[k] = v
	}
	claimSnap := map[string]bool{}
	for k, v := range s.claimed {
		claimSnap[k] = v
	}
	s.mu.Unlock()
	if err := fn(ctx); err != nil {
		s.mu.Lock()
		s.windows, s.claimed = winSnap, claimSnap
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *kpcStore) MarkProcessed(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed[id] {
		return false, nil
	}
	s.claimed[id] = true
	return true, nil
}

func (s *kpcStore) Upsert(_ context.Context, w order.PlannedCapacityWindow) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts++
	if s.upsertErr != nil {
		if err := s.upsertErr(s.upserts); err != nil {
			return false, err
		}
	}
	if prev, ok := s.windows[w.PlanID]; ok && !w.Supersedes(prev) {
		return false, nil
	}
	s.windows[w.PlanID] = w
	return true, nil
}

func (s *kpcStore) ListByLocation(context.Context, string, time.Time) ([]order.PlannedCapacityWindow, error) {
	return nil, nil
}

func (s *kpcStore) get(id string) (order.PlannedCapacityWindow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.windows[id]
	return w, ok
}

func (s *kpcStore) isClaimed(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed[id]
}

var (
	_ ports.PlannedCapacityRepo            = (*kpcStore)(nil)
	_ ports.PlannedCapacityProcessedEvents = (*kpcStore)(nil)
	_ ports.UnitOfWork                     = (*kpcStore)(nil)
)

// captureDLQ records every dead-lettered message.
type captureDLQ struct {
	mu   sync.Mutex
	msgs []kafkago.Message
	err  error
}

func (d *captureDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.msgs = append(d.msgs, msgs...)
	return nil
}
func (d *captureDLQ) Close() error { return nil }

func (d *captureDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.msgs)
}

type kpcFixture struct {
	store    *kpcStore
	reader   *scriptedReader
	dlq      *captureDLQ
	consumer *PlannedCapacityConsumer
}

func newKPCFixture() *kpcFixture {
	store := newKPCStore()
	reader := &scriptedReader{}
	dlq := &captureDLQ{}
	return &kpcFixture{
		store: store, reader: reader, dlq: dlq,
		consumer: &PlannedCapacityConsumer{
			reader: reader, dlqWriter: dlq,
			apply: &usecases.ApplyPlannedCapacity{Windows: store, Processed: store, UnitOfWork: store},
		},
	}
}

func (f *kpcFixture) handle(t *testing.T, msg kafkago.Message) error {
	t.Helper()
	return f.consumer.handleMessage(context.Background(), msg)
}

func (f *kpcFixture) committed() []int64 {
	c, _, _ := f.reader.snapshot()
	return c
}

func TestPlannedCapacityConsumer_ValidShortageUpdatesTheReadModel(t *testing.T) {
	f := newKPCFixture()
	if err := f.handle(t, pcMsg(t, 41, pcTypeShort, pcEventID, pcPlanID, pcEventAt, pcData())); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	w, ok := f.store.get(pcPlanID)
	if !ok {
		t.Fatal("the read model must hold the plan")
	}
	want := order.PlannedCapacityWindow{
		PlanID: pcPlanID, WarehouseID: "WH-1", Location: "SIM1", PathID: "pick-rebin-pack",
		Start: pcWinFrom, End: pcWinTo, AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN", Status: order.PlannedCapacityPublished, AsOf: pcEventAt,
	}
	if w != want {
		t.Fatalf("window = %+v\nwant     %+v", w, want)
	}
	if got := f.committed(); len(got) != 1 || got[0] != 41 {
		t.Fatalf("committed = %v, want [41]", got)
	}
	if f.dlq.count() != 0 {
		t.Fatal("nothing may be dead-lettered")
	}
}

func TestPlannedCapacityConsumer_CreatedIsADraftAndPublishedUpgradesItNeverTheReverse(t *testing.T) {
	f := newKPCFixture()
	created := pcData()
	created["status"], created["path_capacity"] = "DRAFT", 1000
	published := pcData()
	published["published_at"] = pcEventAt.Format(time.RFC3339)

	steps := []struct {
		offset int64
		typ    string
		id     string
		at     time.Time
		data   map[string]any
		want   order.PlannedCapacityStatus
	}{
		{1, pcTypeNew, "evt-created", pcEventAt.Add(-time.Minute), created, order.PlannedCapacityDraft},
		{2, pcTypePub, "evt-published", pcEventAt, published, order.PlannedCapacityPublished},
		// A late Created (a different, later id) must not un-publish the plan.
		{3, pcTypeNew, "evt-created-late", pcEventAt.Add(time.Hour), created, order.PlannedCapacityPublished},
	}
	for _, s := range steps {
		if err := f.handle(t, pcMsg(t, s.offset, s.typ, s.id, pcPlanID, s.at, s.data)); err != nil {
			t.Fatalf("offset %d: %v", s.offset, err)
		}
		if w, _ := f.store.get(pcPlanID); w.Status != s.want {
			t.Fatalf("after offset %d status = %s, want %s", s.offset, w.Status, s.want)
		}
	}
	if got := f.committed(); len(got) != 3 {
		t.Fatalf("committed = %v, want all three", got)
	}
}

func TestPlannedCapacityConsumer_ReplayedIDIsANoOp(t *testing.T) {
	f := newKPCFixture()
	if err := f.handle(t, pcMsg(t, 1, pcTypeShort, pcEventID, pcPlanID, pcEventAt, pcData())); err != nil {
		t.Fatal(err)
	}
	changed := pcData()
	changed["shortage"] = 1
	if err := f.handle(t, pcMsg(t, 2, pcTypeShort, pcEventID, pcPlanID, pcEventAt.Add(time.Hour), changed)); err != nil {
		t.Fatal(err)
	}
	if w, _ := f.store.get(pcPlanID); w.Shortage != 4000 {
		t.Fatalf("shortage = %v: the replayed id must not be applied", w.Shortage)
	}
	if got := f.committed(); len(got) != 2 {
		t.Fatalf("the replay must still be committed, got %v", got)
	}
}

func TestPlannedCapacityConsumer_UnknownAndBottleneckTypesAreIgnoredAndCommitted(t *testing.T) {
	for _, typ := range []string{
		pcTypeBott,
		"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanArchived",
		"com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected.v2",
		"CapacityShortageDetected", // a short name is not the full type
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
	} {
		t.Run(typ, func(t *testing.T) {
			f := newKPCFixture()
			if err := f.handle(t, pcMsg(t, 9, typ, "evt-x", pcPlanID, pcEventAt, pcData())); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if _, ok := f.store.get(pcPlanID); ok || f.store.isClaimed("evt-x") {
				t.Fatal("an ignored type must touch nothing")
			}
			if f.dlq.count() != 0 {
				t.Fatal("an unknown type is forward compatibility, not poison: it must not be dead-lettered")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 9 {
				t.Fatalf("committed = %v, want [9]", got)
			}
		})
	}
}

func TestPlannedCapacityConsumer_LegacyFlatAndGarbageAreDeadLetteredWithoutError(t *testing.T) {
	flat := pcLegacyFlat("evt-legacy", "CapacityShortageDetected", `{"plan_id":"p1"}`)
	for name, value := range map[string][]byte{
		"legacy flat envelope":       flat,
		"garbage bytes":              []byte("\x00\xffnot json"),
		"json but not a cloud event": []byte(`{"hello":"world"}`),
		"empty value":                nil,
	} {
		t.Run(name, func(t *testing.T) {
			f := newKPCFixture()
			if err := f.handle(t, kafkago.Message{Offset: 5, Value: value}); err != nil {
				t.Fatalf("a poison message must be skipped without error, got %v", err)
			}
			if f.dlq.count() != 1 {
				t.Fatalf("dlq = %d messages, want 1", f.dlq.count())
			}
			if got := f.dlq.msgs[0].Value; string(got) != string(value) {
				t.Fatal("the DLQ copy must be byte-identical to the original")
			}
			if len(f.store.windows) != 0 || len(f.store.claimed) != 0 {
				t.Fatal("nothing may be written")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 5 {
				t.Fatalf("committed = %v, want [5]: the partition must not block", got)
			}
		})
	}
}

func pcLegacyFlat(eventID, eventType, data string) []byte {
	return []byte(`{"event_id":"` + eventID + `","event_type":"` + eventType + `","occurred_at":"2026-09-01T00:00:00Z","source":"legacy","data":` + data + `}`)
}

func TestPlannedCapacityConsumer_InvalidPayloadsAreDeadLetteredNeverApplied(t *testing.T) {
	mut := func(f func(map[string]any)) map[string]any {
		d := pcData()
		f(d)
		return d
	}
	tests := []struct {
		name    string
		subject string
		data    any
	}{
		{"missing plan id", pcPlanID, mut(func(d map[string]any) { delete(d, "plan_id") })},
		{"missing location", pcPlanID, mut(func(d map[string]any) { d["location"] = "" })},
		{"window end equal to start", pcPlanID, mut(func(d map[string]any) { d["window_end"] = d["window_start"] })},
		{"missing window", pcPlanID, mut(func(d map[string]any) { delete(d, "window_start"); delete(d, "window_end") })},
		{"negative shortage", pcPlanID, mut(func(d map[string]any) { d["shortage"] = -1 })},
		{"shortage of the wrong type", pcPlanID, mut(func(d map[string]any) { d["shortage"] = "lots" })},
		{"unparseable window", pcPlanID, mut(func(d map[string]any) { d["window_start"] = "tomorrow" })},
		{"subject disagrees with plan_id", "another-plan", pcData()},
		{"data is not an object", pcPlanID, []int{1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newKPCFixture()
			if err := f.handle(t, pcMsg(t, 7, pcTypeShort, pcEventID, tt.subject, pcEventAt, tt.data)); err != nil {
				t.Fatalf("an invalid payload must be skipped without error, got %v", err)
			}
			if f.dlq.count() != 1 {
				t.Fatalf("dlq = %d, want 1", f.dlq.count())
			}
			if len(f.store.windows) != 0 || f.store.isClaimed(pcEventID) {
				t.Fatal("an invalid payload must not write or claim anything")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 7 {
				t.Fatalf("committed = %v, want [7]", got)
			}
			if f.store.upserts != 0 {
				t.Fatal("validation must run before the transaction opens")
			}
		})
	}
}

// A transient repository failure: every attempt fails AFTER the claim, so
// the transaction rolls back each time — nothing written, claim un-recorded
// — and only once the bounded retries are exhausted does the message go to
// the DLQ (and only then is the offset committed).
func TestPlannedCapacityConsumer_PersistentRepositoryFailureRollsBackThenDeadLetters(t *testing.T) {
	f := newKPCFixture()
	f.store.upsertErr = func(int) error { return errors.New("connection reset by peer") }

	if err := f.handle(t, pcMsg(t, 3, pcTypeShort, pcEventID, pcPlanID, pcEventAt, pcData())); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.store.upserts != maxHandlerAttempts {
		t.Fatalf("upsert attempts = %d, want %d", f.store.upserts, maxHandlerAttempts)
	}
	if _, ok := f.store.get(pcPlanID); ok {
		t.Fatal("nothing may be written")
	}
	if f.store.isClaimed(pcEventID) {
		t.Fatal("the claim must be un-recorded, or a manual replay would be skipped as already handled")
	}
	if f.dlq.count() != 1 {
		t.Fatalf("dlq = %d, want 1", f.dlq.count())
	}
	if got := f.committed(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("committed = %v, want [3]", got)
	}
}

func TestPlannedCapacityConsumer_TransientFailureThatRecoversIsAppliedWithoutDeadLettering(t *testing.T) {
	f := newKPCFixture()
	f.store.upsertErr = func(call int) error {
		if call < 3 {
			return errors.New("deadlock detected")
		}
		return nil
	}
	if err := f.handle(t, pcMsg(t, 4, pcTypeShort, pcEventID, pcPlanID, pcEventAt, pcData())); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if _, ok := f.store.get(pcPlanID); !ok || !f.store.isClaimed(pcEventID) {
		t.Fatal("the third attempt must apply and claim")
	}
	if f.dlq.count() != 0 {
		t.Fatal("a recovered transient failure must not be dead-lettered")
	}
	if got := f.committed(); len(got) != 1 || got[0] != 4 {
		t.Fatalf("committed = %v, want [4]", got)
	}
}

// Never lose a message because the DLQ itself is unavailable: the offset is
// NOT committed, the error is returned, and the loop retries the message.
func TestPlannedCapacityConsumer_DLQPublishFailureDoesNotCommit(t *testing.T) {
	f := newKPCFixture()
	f.dlq.err = errors.New("dlq broker down")
	err := f.handle(t, kafkago.Message{Offset: 6, Value: []byte(`{"not":"a cloudevent"}`)})
	if err == nil {
		t.Fatal("a DLQ publish failure must be returned")
	}
	if got := f.committed(); len(got) != 0 {
		t.Fatalf("committed = %v, want nothing: the message was not safely parked", got)
	}
}

// The offset is committed only after the handling succeeded, in order, and a
// failed commit is retried rather than skipped.
func TestPlannedCapacityConsumer_Run_CommitsAfterSuccessAndSurvivesCommitFailure(t *testing.T) {
	f := newKPCFixture()
	f.reader.commitFailures = 1
	f.reader.msgs = []kafkago.Message{
		pcMsg(t, 20, pcTypeShort, "evt-20", pcPlanID, pcEventAt, pcData()),
		pcMsg(t, 21, pcTypeBott, "evt-21", pcPlanID, pcEventAt, map[string]any{"plan_id": pcPlanID}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := f.consumer.Run(ctx); err != nil {
		t.Fatalf("Run returned %v; it must only stop when ctx is done", err)
	}
	if got := f.committed(); len(got) != 2 || got[0] != 20 || got[1] != 21 {
		t.Fatalf("committed = %v, want [20 21]", got)
	}
	if _, ok := f.store.get(pcPlanID); !ok {
		t.Fatal("the shortage must have been applied")
	}
	if f.store.upserts != 1 {
		t.Fatalf("upserts = %d, want 1: message 20's failed commit is retried, and the retry hits the already-claimed id (a no-op, not a second write); message 21 is an ignored type", f.store.upserts)
	}
}

func TestPlannedCapacityConsumerForTopic_DerivesTheDLQTopicFromTheSource(t *testing.T) {
	c := NewPlannedCapacityConsumerForTopic([]string{"broker-1:9092"}, "grp-from-config", "warehouse.warehouse-planning.events.itest", nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	if got := c.reader.Config().Topic; got != "warehouse.warehouse-planning.events.itest" {
		t.Fatalf("topic = %q", got)
	}
	if got := c.reader.Config().GroupID; got != "grp-from-config" {
		t.Fatalf("group = %q, want the configured id verbatim (no default, no suffix)", got)
	}
	w, ok := c.dlqWriter.(*kafkago.Writer)
	if !ok || w.Topic != "warehouse.warehouse-planning.events.itest.dlq" || !w.AllowAutoTopicCreation {
		t.Fatalf("dlq writer = %+v", c.dlqWriter)
	}
}
