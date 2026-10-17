package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Fixtures follow wes-work-planning's WorkUnitCreated encoder (its golden
// ce_events_WorkUnitCreated.golden.json) plus the two OPTIONAL fields that
// producer's ADR 0041 adds to `data` on the integration stream: `reference`
// (string) and `line_no` (integer > 0, omitted when unknown).
const (
	rcTypeCreated = "com.warehouse.wes.work-planning.workunit.WorkUnitCreated"
	rcEventID     = "11111111-1111-4111-8111-111111111111"
	rcOrderID     = "ord-1"
)

var (
	rcReleasedAt  = time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	rcConfirmedAt = time.Date(2026, 10, 17, 9, 0, 5, 0, time.UTC)
)

func rcMsg(t *testing.T, offset int64, ceType, id, subject string, data any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/wes-work-planning")
	e.SetType(ceType)
	e.SetSubject(subject)
	e.SetTime(rcConfirmedAt)
	e.SetDataSchema("urn:warehouse:wes-work-planning:events:WorkUnitCreated:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Offset: offset, Key: []byte(subject), Value: b}
}

// rcData is the WorkUnitCreated payload with the new optional fields.
func rcData(orderID string, lineNo int) map[string]any {
	return map[string]any{
		"path_id": "pick-a", "work_unit_id": usecases.WorkUnitID(shared.OrderId(orderID), lineNo),
		"reference": orderID, "line_no": lineNo,
	}
}

// rcLegacyData is today's payload: no reference, no line_no.
func rcLegacyData() map[string]any {
	return map[string]any{"path_id": "pick-a", "work_unit_id": "wu-gold"}
}

// krcStore: one in-memory process-manager table + idempotency gate +
// UnitOfWork with real rollback.
type krcStore struct {
	mu      sync.Mutex
	rows    map[string]releaseconfirmation.Confirmation
	claimed map[string]bool
	saves   int
	saveErr func(call int) error
	// park, when non-nil, makes Execute block until the handler's ctx is
	// done and then report the cancellation (what the Postgres UnitOfWork
	// does when the pod is told to stop mid-transaction).
	park    chan struct{}
	entered int
}

func newKRCStore() *krcStore {
	return &krcStore{rows: map[string]releaseconfirmation.Confirmation{}, claimed: map[string]bool{}}
}

func rcKey(orderID shared.OrderId, lineNo int) string {
	return fmt.Sprintf("%s#%d", orderID, lineNo)
}

func (s *krcStore) Execute(ctx context.Context, fn func(context.Context) error) error {
	s.mu.Lock()
	s.entered++
	park := s.park
	rowSnap := map[string]releaseconfirmation.Confirmation{}
	for k, v := range s.rows {
		rowSnap[k] = v
	}
	claimSnap := map[string]bool{}
	for k, v := range s.claimed {
		claimSnap[k] = v
	}
	s.mu.Unlock()

	if park != nil {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := fn(ctx); err != nil {
		s.mu.Lock()
		s.rows, s.claimed = rowSnap, claimSnap
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *krcStore) MarkProcessed(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed[id] {
		return false, nil
	}
	s.claimed[id] = true
	return true, nil
}

func (s *krcStore) Open(_ context.Context, c releaseconfirmation.Confirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[rcKey(c.OrderID(), c.LineNo())] = c
	return nil
}

func (s *krcStore) Find(_ context.Context, orderID shared.OrderId, lineNo int) (releaseconfirmation.Confirmation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.rows[rcKey(orderID, lineNo)]
	return c, ok, nil
}

func (s *krcStore) Save(_ context.Context, c releaseconfirmation.Confirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saveErr != nil {
		if err := s.saveErr(s.saves); err != nil {
			return err
		}
	}
	s.rows[rcKey(c.OrderID(), c.LineNo())] = c
	return nil
}

func (s *krcStore) ClaimOverdue(context.Context, time.Time, int) ([]releaseconfirmation.Confirmation, error) {
	return nil, nil
}

func (s *krcStore) row(orderID shared.OrderId, lineNo int) releaseconfirmation.Confirmation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[rcKey(orderID, lineNo)]
}

func (s *krcStore) isClaimed(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed[id]
}

func (s *krcStore) claimCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.claimed)
}

var (
	_ ports.ReleaseConfirmations               = (*krcStore)(nil)
	_ ports.ReleaseConfirmationProcessedEvents = (*krcStore)(nil)
	_ ports.UnitOfWork                         = (*krcStore)(nil)
)

type krcMetrics struct {
	mu       sync.Mutex
	outcomes []ports.ReleaseConfirmationOutcome
}

func (m *krcMetrics) Handled(_ context.Context, o ports.ReleaseConfirmationOutcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outcomes = append(m.outcomes, o)
}
func (m *krcMetrics) LineUnconfirmed(context.Context) {}

func (m *krcMetrics) recorded() []ports.ReleaseConfirmationOutcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ports.ReleaseConfirmationOutcome(nil), m.outcomes...)
}

type rcClock struct{}

func (rcClock) Now() time.Time { return rcConfirmedAt }

type krcFixture struct {
	store    *krcStore
	metrics  *krcMetrics
	reader   *scriptedReader
	dlq      *captureDLQ
	consumer *ReleaseConfirmationConsumer
}

func newKRCFixture(t *testing.T) *krcFixture {
	t.Helper()
	store := newKRCStore()
	pending, err := releaseconfirmation.NewPending(rcOrderID, 2, usecases.WorkUnitID(rcOrderID, 2), "pick-a", rcReleasedAt)
	if err != nil {
		t.Fatalf("NewPending: %v", err)
	}
	store.rows[rcKey(rcOrderID, 2)] = pending
	metrics := &krcMetrics{}
	reader := &scriptedReader{}
	dlq := &captureDLQ{}
	return &krcFixture{
		store: store, metrics: metrics, reader: reader, dlq: dlq,
		consumer: &ReleaseConfirmationConsumer{
			reader: reader, dlqWriter: dlq,
			confirm: &usecases.ConfirmRelease{
				Confirmations: store, Processed: store, Clock: rcClock{}, UnitOfWork: store, Metrics: metrics,
			},
		},
	}
}

func (f *krcFixture) handle(t *testing.T, msg kafkago.Message) error {
	t.Helper()
	return f.consumer.handleMessage(context.Background(), msg)
}

func (f *krcFixture) committed() []int64 {
	c, _, _ := f.reader.snapshot()
	return c
}

func TestReleaseConfirmationConsumer_WorkUnitCreatedWithTheNewFieldsConfirmsTheLine(t *testing.T) {
	f := newKRCFixture(t)
	if err := f.handle(t, rcMsg(t, 41, rcTypeCreated, rcEventID, usecases.WorkUnitID(rcOrderID, 2), rcData(rcOrderID, 2))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	c := f.store.row(rcOrderID, 2)
	if c.State() != releaseconfirmation.StateConfirmed {
		t.Fatalf("State = %q, want CONFIRMED", c.State())
	}
	if at := c.ConfirmedAt(); at == nil || !at.Equal(rcConfirmedAt) {
		t.Fatalf("ConfirmedAt = %v, want the clock's now %v", at, rcConfirmedAt)
	}
	if !f.store.isClaimed(rcEventID) {
		t.Fatal("the CloudEvents id must be claimed")
	}
	if got := f.committed(); len(got) != 1 || got[0] != 41 {
		t.Fatalf("committed = %v, want [41]", got)
	}
	if f.dlq.count() != 0 {
		t.Fatal("nothing may be dead-lettered")
	}
	if got := f.metrics.recorded(); len(got) != 1 || got[0] != ports.ReleaseConfirmationConfirmed {
		t.Fatalf("metrics = %v, want [confirmed]", got)
	}
}

// Today's WorkUnitCreated (and any transfer / RELOCATE / legacy work unit)
// carries no reference and no line_no. It is IGNORED with a counter, never
// dead-lettered, and touches no state.
func TestReleaseConfirmationConsumer_WorkUnitCreatedWithoutTheNewFieldsIsIgnoredCountedAndCommitted(t *testing.T) {
	variants := map[string]map[string]any{
		"today's payload (no new fields)": rcLegacyData(),
		"reference only":                  {"path_id": "pick-a", "work_unit_id": "wu-1", "reference": rcOrderID},
		"line_no only":                    {"path_id": "pick-a", "work_unit_id": "wu-1", "line_no": 2},
		"empty reference":                 {"path_id": "pick-a", "work_unit_id": "wu-1", "reference": "", "line_no": 2},
		"line_no zero":                    {"path_id": "pick-a", "work_unit_id": "wu-1", "reference": rcOrderID, "line_no": 0},
		"line_no negative":                {"path_id": "pick-a", "work_unit_id": "wu-1", "reference": rcOrderID, "line_no": -3},
		"explicit nulls":                  {"path_id": "pick-a", "work_unit_id": "wu-1", "reference": nil, "line_no": nil},
		"empty object":                    {},
	}
	for name, data := range variants {
		t.Run(name, func(t *testing.T) {
			f := newKRCFixture(t)
			if err := f.handle(t, rcMsg(t, 9, rcTypeCreated, rcEventID, "wu-1", data)); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			assertIgnoredAsNotOrderWork(t, f, 9)
		})
	}
}

// assertIgnoredAsNotOrderWork checks a WorkUnitCreated without the new fields
// left every row untouched, claimed nothing, opened no transaction, was not
// dead-lettered, was committed past and was counted as ignored_no_reference.
func assertIgnoredAsNotOrderWork(t *testing.T, f *krcFixture, offset int64) {
	t.Helper()
	if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StatePending {
		t.Fatalf("State = %q, an event without the new fields must not confirm anything", c.State())
	}
	if f.store.claimCount() != 0 || f.store.entered != 0 {
		t.Fatalf("claims=%d transactions=%d: an event without the new fields must touch nothing", f.store.claimCount(), f.store.entered)
	}
	if f.dlq.count() != 0 {
		t.Fatal("a work unit that is not order work is not poison: it must not be dead-lettered")
	}
	if got := f.committed(); len(got) != 1 || got[0] != offset {
		t.Fatalf("committed = %v, want [%d]", got, offset)
	}
	if got := f.metrics.recorded(); len(got) != 1 || got[0] != ports.ReleaseConfirmationIgnoredNoReference {
		t.Fatalf("metrics = %v, want [ignored_no_reference]", got)
	}
}

// A reference this service never opened a row for (a transfer id, an order
// released before the migration) is ignored and counted, not dead-lettered.
func TestReleaseConfirmationConsumer_UnknownReferenceOrLineIsIgnoredCountedAndCommitted(t *testing.T) {
	for name, data := range map[string]map[string]any{
		"transfer reference":                rcData("transfer-77", 1),
		"known order, unknown line":         rcData(rcOrderID, 9),
		"order released before the feature": rcData("ord-old", 1),
	} {
		t.Run(name, func(t *testing.T) {
			f := newKRCFixture(t)
			if err := f.handle(t, rcMsg(t, 12, rcTypeCreated, "evt-u", "wu", data)); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if f.store.isClaimed("evt-u") || f.dlq.count() != 0 {
				t.Fatalf("claimed=%v dlq=%d, an unknown line is neither claimed nor dead-lettered", f.store.isClaimed("evt-u"), f.dlq.count())
			}
			if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StatePending {
				t.Fatalf("the known row changed to %q", c.State())
			}
			if got := f.committed(); len(got) != 1 || got[0] != 12 {
				t.Fatalf("committed = %v, want [12]", got)
			}
			if got := f.metrics.recorded(); len(got) != 1 || got[0] != ports.ReleaseConfirmationIgnoredUnknownLine {
				t.Fatalf("metrics = %v, want [ignored_unknown_line]", got)
			}
		})
	}
}

func TestReleaseConfirmationConsumer_ReplayedIDIsADuplicateNoOp(t *testing.T) {
	f := newKRCFixture(t)
	msg := rcMsg(t, 1, rcTypeCreated, rcEventID, "wu", rcData(rcOrderID, 2))
	if err := f.handle(t, msg); err != nil {
		t.Fatal(err)
	}
	first := *f.store.row(rcOrderID, 2).ConfirmedAt()
	f.consumer.confirm.Clock = laterClock{}
	msg.Offset = 2
	if err := f.handle(t, msg); err != nil {
		t.Fatal(err)
	}
	if at := f.store.row(rcOrderID, 2).ConfirmedAt(); !at.Equal(first) {
		t.Fatalf("ConfirmedAt moved to %v on a replay, want it to stay %v", at, first)
	}
	if got := f.committed(); len(got) != 2 {
		t.Fatalf("the replay must still be committed, got %v", got)
	}
	if got := f.metrics.recorded(); len(got) != 2 || got[1] != ports.ReleaseConfirmationDuplicate {
		t.Fatalf("metrics = %v, want [confirmed duplicate]", got)
	}
}

type laterClock struct{}

func (laterClock) Now() time.Time { return rcConfirmedAt.Add(time.Hour) }

func TestReleaseConfirmationConsumer_OtherTypesOnTheTopicAreIgnoredAndCommitted(t *testing.T) {
	for _, typ := range []string{
		"com.warehouse.wes.work-planning.workunit.WorkReleased",
		"com.warehouse.wes.work-planning.workunit.WorkUnitCompleted",
		"com.warehouse.wes.work-planning.workunit.WorkUnitCreated.v2",
		"com.warehouse.wes.work-planning.workunit.WorkUnitCreatedExtra",
		"WorkUnitCreated", // a short name is not the full type
		"com.warehouse.wes.order-management.order.OrderAllocated",
	} {
		t.Run(typ, func(t *testing.T) {
			f := newKRCFixture(t)
			if err := f.handle(t, rcMsg(t, 9, typ, "evt-x", "wu", rcData(rcOrderID, 2))); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StatePending || f.store.isClaimed("evt-x") {
				t.Fatal("an ignored type must touch nothing")
			}
			if f.dlq.count() != 0 {
				t.Fatal("an unknown type is forward compatibility, not poison: it must not be dead-lettered")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 9 {
				t.Fatalf("committed = %v, want [9]", got)
			}
			if len(f.metrics.recorded()) != 0 {
				t.Fatalf("metrics = %v, a foreign type is not a handled WorkUnitCreated", f.metrics.recorded())
			}
		})
	}
}

func TestReleaseConfirmationConsumer_LegacyFlatAndGarbageAreDeadLetteredWithoutError(t *testing.T) {
	flat := pcLegacyFlat("evt-legacy", "WorkUnitCreated", `{"path_id":"p"}`)
	for name, value := range map[string][]byte{
		"legacy flat envelope":       flat,
		"garbage bytes":              []byte("\x00\xffnot json"),
		"json but not a cloud event": []byte(`{"hello":"world"}`),
		"empty value":                nil,
	} {
		t.Run(name, func(t *testing.T) {
			f := newKRCFixture(t)
			if err := f.handle(t, kafkago.Message{Offset: 5, Value: value}); err != nil {
				t.Fatalf("a poison message must be skipped without error, got %v", err)
			}
			if f.dlq.count() != 1 {
				t.Fatalf("dlq = %d messages, want 1", f.dlq.count())
			}
			if got := f.dlq.msgs[0].Value; string(got) != string(value) {
				t.Fatal("the DLQ copy must be byte-identical to the original")
			}
			if f.store.claimCount() != 0 {
				t.Fatal("nothing may be written")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 5 {
				t.Fatalf("committed = %v, want [5]: the partition must not block", got)
			}
		})
	}
}

// data that cannot be decoded at all is malformed: dead-letter it, never
// apply it, never retry it.
func TestReleaseConfirmationConsumer_MalformedDataIsDeadLetteredNeverApplied(t *testing.T) {
	mut := func(k string, v any) map[string]any {
		d := rcData(rcOrderID, 2)
		d[k] = v
		return d
	}
	for name, data := range map[string]any{
		"line_no is a string":           mut("line_no", "2"),
		"line_no is a fraction":         mut("line_no", 2.5),
		"line_no is an object":          mut("line_no", map[string]any{"n": 2}),
		"reference is a number":         mut("reference", 42),
		"reference is an array":         mut("reference", []string{"a"}),
		"data is not an object":         []int{1, 2, 3},
		"data is a bare string":         "ord-1",
		"line_no beyond an int":         mut("line_no", 1e30),
		"line_no is a boolean":          mut("line_no", true),
		"reference is a nested object":  mut("reference", map[string]any{"id": rcOrderID}),
		"line_no is the string literal": mut("line_no", "two"),
	} {
		t.Run(name, func(t *testing.T) {
			f := newKRCFixture(t)
			if err := f.handle(t, rcMsg(t, 7, rcTypeCreated, rcEventID, "wu", data)); err != nil {
				t.Fatalf("a malformed payload must be skipped without error, got %v", err)
			}
			assertDeadLetteredNeverApplied(t, f, 7)
		})
	}
}

// assertDeadLetteredNeverApplied checks a malformed payload went to the DLQ
// exactly once, wrote and claimed nothing, left the row PENDING, was committed
// past and was not counted as a handled confirmation.
func assertDeadLetteredNeverApplied(t *testing.T, f *krcFixture, offset int64) {
	t.Helper()
	if f.dlq.count() != 1 {
		t.Fatalf("dlq = %d, want 1", f.dlq.count())
	}
	if f.store.claimCount() != 0 || f.store.entered != 0 {
		t.Fatalf("claims=%d transactions=%d: a malformed payload must not write, claim or open a transaction", f.store.claimCount(), f.store.entered)
	}
	if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StatePending {
		t.Fatalf("State = %q", c.State())
	}
	if got := f.committed(); len(got) != 1 || got[0] != offset {
		t.Fatalf("committed = %v, want [%d]", got, offset)
	}
	if len(f.metrics.recorded()) != 0 {
		t.Fatalf("metrics = %v, a dead-lettered message is not a handled confirmation", f.metrics.recorded())
	}
}

// The decoder, in isolation, from fixture JSON: exactly the field names and
// types wes-work-planning's WorkUnitCreated encoder emits.
func TestWorkUnitCreatedRequestFrom_DecodesExactlyReferenceAndLineNo(t *testing.T) {
	tests := []struct {
		name         string
		json         string
		wantRelevant bool
		wantRef      string
		wantLineNo   int
		wantErr      bool
	}{
		{
			name:         "with the new fields",
			json:         wuCreatedJSON(`{"path_id":"pick-a","work_unit_id":"ord-1-line-2","reference":"ord-1","line_no":2}`),
			wantRelevant: true, wantRef: "ord-1", wantLineNo: 2,
		},
		{
			name:         "today's golden payload, without them",
			json:         wuCreatedJSON(`{"path_id":"pick-a","work_unit_id":"wu-gold"}`),
			wantRelevant: true,
		},
		{
			name:         "reference present, line_no omitted (unknown)",
			json:         wuCreatedJSON(`{"path_id":"pick-a","work_unit_id":"wu","reference":"TR-9"}`),
			wantRelevant: true, wantRef: "TR-9",
		},
		{
			name:         "unknown extra fields are ignored (forward compatible)",
			json:         wuCreatedJSON(`{"path_id":"pick-a","work_unit_id":"x","reference":"ord-1","line_no":1,"priority":"HIGH"}`),
			wantRelevant: true, wantRef: "ord-1", wantLineNo: 1,
		},
		{
			name:         "a large but valid line number",
			json:         wuCreatedJSON(`{"reference":"ord-1","line_no":120}`),
			wantRelevant: true, wantRef: "ord-1", wantLineNo: 120,
		},
		{
			name:    "line_no as a string is malformed",
			json:    wuCreatedJSON(`{"reference":"ord-1","line_no":"2"}`),
			wantErr: true,
		},
		{
			name:    "data that is not an object is malformed",
			json:    wuCreatedJSON(`[1,2]`),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := cloudevents.Decode([]byte(tt.json))
			if err != nil {
				t.Fatalf("fixture is not a valid CloudEvent: %v", err)
			}
			req, relevant, err := releaseConfirmationRequestFrom(e)
			if relevant != tt.wantRelevant {
				t.Fatalf("relevant = %v, want %v", relevant, tt.wantRelevant)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if req.EventID != "11111111-1111-4111-8111-111111111111" {
				t.Fatalf("EventID = %q, want the CloudEvents id", req.EventID)
			}
			if req.Reference != tt.wantRef || req.LineNo != tt.wantLineNo {
				t.Fatalf("request = %q/%d, want %q/%d", req.Reference, req.LineNo, tt.wantRef, tt.wantLineNo)
			}
		})
	}
}

func TestWorkUnitCreatedRequestFrom_OtherTypesAreNotRelevant(t *testing.T) {
	for _, typ := range []string{
		"com.warehouse.wes.work-planning.workunit.WorkReleased",
		"WorkUnitCreated",
		"",
	} {
		e := ce.New(ce.CloudEventsVersionV1)
		e.SetType(typ)
		_, relevant, err := releaseConfirmationRequestFrom(e)
		if relevant || err != nil {
			t.Fatalf("type %q: relevant=%v err=%v, want neither", typ, relevant, err)
		}
	}
}

func wuCreatedJSON(data string) string {
	return `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/wes-work-planning",` +
		`"type":"com.warehouse.wes.work-planning.workunit.WorkUnitCreated","subject":"wu-gold","datacontenttype":"application/json",` +
		`"dataschema":"urn:warehouse:wes-work-planning:events:WorkUnitCreated:v1","time":"2026-09-30T12:00:00Z","data":` + data + `}`
}

// A transient repository failure: every attempt fails AFTER the claim, so
// the transaction rolls back each time -- the row stays PENDING and the
// claim is un-recorded -- and only once the bounded retries are exhausted
// does the message go to the DLQ (and only then is the offset committed).
func TestReleaseConfirmationConsumer_PersistentFailureRollsBackThenDeadLetters(t *testing.T) {
	f := newKRCFixture(t)
	f.store.saveErr = func(int) error { return errors.New("connection reset by peer") }

	if err := f.handle(t, rcMsg(t, 3, rcTypeCreated, rcEventID, "wu", rcData(rcOrderID, 2))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.store.saves != maxHandlerAttempts {
		t.Fatalf("save attempts = %d, want %d", f.store.saves, maxHandlerAttempts)
	}
	if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StatePending {
		t.Fatalf("State = %q, nothing may be written", c.State())
	}
	if f.store.isClaimed(rcEventID) {
		t.Fatal("the claim must be un-recorded, or a manual replay would be skipped as already handled")
	}
	if f.dlq.count() != 1 {
		t.Fatalf("dlq = %d, want 1", f.dlq.count())
	}
	if got := f.committed(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("committed = %v, want [3]", got)
	}
}

func TestReleaseConfirmationConsumer_TransientFailureThatRecoversIsAppliedWithoutDeadLettering(t *testing.T) {
	f := newKRCFixture(t)
	f.store.saveErr = func(call int) error {
		if call < 3 {
			return errors.New("deadlock detected")
		}
		return nil
	}
	if err := f.handle(t, rcMsg(t, 4, rcTypeCreated, rcEventID, "wu", rcData(rcOrderID, 2))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StateConfirmed || !f.store.isClaimed(rcEventID) {
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
func TestReleaseConfirmationConsumer_DLQPublishFailureDoesNotCommit(t *testing.T) {
	f := newKRCFixture(t)
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
func TestReleaseConfirmationConsumer_Run_CommitsAfterSuccessAndSurvivesCommitFailure(t *testing.T) {
	f := newKRCFixture(t)
	f.reader.commitFailures = 1
	f.reader.msgs = []kafkago.Message{
		rcMsg(t, 20, rcTypeCreated, "evt-20", "wu", rcData(rcOrderID, 2)),
		rcMsg(t, 21, "com.warehouse.wes.work-planning.workunit.WorkReleased", "evt-21", "wu", rcData(rcOrderID, 2)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := f.consumer.Run(ctx); err != nil {
		t.Fatalf("Run returned %v; it must only stop when ctx is done", err)
	}
	if got := f.committed(); len(got) != 2 || got[0] != 20 || got[1] != 21 {
		t.Fatalf("committed = %v, want [20 21]", got)
	}
	if c := f.store.row(rcOrderID, 2); c.State() != releaseconfirmation.StateConfirmed {
		t.Fatalf("State = %q, want CONFIRMED", c.State())
	}
	if f.store.saves != 1 {
		t.Fatalf("saves = %d, want 1: message 20's failed commit is retried and hits the already-claimed id (a no-op, not a second write)", f.store.saves)
	}
}

// Shutdown is not poison (fleet escape 18b): cancelling Run while a healthy
// message is mid-handle is a clean stop, nothing is dead-lettered and the
// offset stays uncommitted for redelivery.
func TestReleaseConfirmationConsumer_ShutdownMidHandleReturnsNilAndDeadLettersNothing(t *testing.T) {
	f := newKRCFixture(t)
	f.store.park = make(chan struct{})
	f.reader.msgs = []kafkago.Message{rcMsg(t, 30, rcTypeCreated, rcEventID, "wu", rcData(rcOrderID, 2))}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		f.store.mu.Lock()
		entered := f.store.entered
		f.store.mu.Unlock()
		if entered > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("the consumer never started handling the message")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation mid-handle = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}
	if f.dlq.count() != 0 {
		t.Fatal("a healthy message only interrupted by shutdown was dead-lettered")
	}
	if got := f.committed(); len(got) != 0 {
		t.Fatalf("committed = %v, want nothing: the message must be redelivered", got)
	}
}

func TestReleaseConfirmationConsumerForTopic_DerivesTheDLQTopicAndRequiresAllAcks(t *testing.T) {
	c := NewReleaseConfirmationConsumerForTopic([]string{"broker-1:9092"}, "grp-from-config", "warehouse.work-planning.events.itest", nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	if got := c.reader.Config().Topic; got != "warehouse.work-planning.events.itest" {
		t.Fatalf("topic = %q", got)
	}
	if got := c.reader.Config().GroupID; got != "grp-from-config" {
		t.Fatalf("group = %q, want the configured id verbatim (no default, no suffix)", got)
	}
	w, ok := c.dlqWriter.(*kafkago.Writer)
	if !ok || w.Topic != "warehouse.work-planning.events.itest.dlq" || !w.AllowAutoTopicCreation {
		t.Fatalf("dlq writer = %+v", c.dlqWriter)
	}
	if w.RequiredAcks != kafkago.RequireAll {
		t.Fatalf("dlq writer RequiredAcks = %v, want RequireAll", w.RequiredAcks)
	}
}

func TestNewReleaseConfirmationConsumer_ReadsTheWorkPlanningEventsTopic(t *testing.T) {
	if WorkPlanningEventsTopic != "warehouse.work-planning.events" {
		t.Fatalf("WorkPlanningEventsTopic = %q", WorkPlanningEventsTopic)
	}
	c := NewReleaseConfirmationConsumer([]string{"broker-1:9092"}, "order-management-release-confirmation", nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	if got := c.reader.Config().Topic; got != WorkPlanningEventsTopic {
		t.Fatalf("topic = %q", got)
	}
}
