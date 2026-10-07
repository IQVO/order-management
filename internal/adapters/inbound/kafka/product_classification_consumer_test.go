package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// Fixtures lifted from product-master's published AsyncAPI example.
const (
	clsEventID = "0f6d8a2b-3c4e-4d5f-8a9b-7c6d5e4f3a2b"
	clsType    = "com.warehouse.wms.product-master.product.ProductClassified"
)

var clsAt = time.Date(2026, 10, 6, 21, 2, 0, 0, time.UTC)

func clsData(sku string, version int64, tags ...string) map[string]any {
	d := map[string]any{
		"sku": sku, "handling_tags": tags, "temperature_class": "Frozen",
		"dot_hazard_class": 3, "classification_source": "native", "version": version,
	}
	return d
}

// clsMsg builds a real CloudEvents 1.0 structured-mode message with the
// official SDK, in product-master's exact wire format.
func clsMsg(t *testing.T, offset int64, ceType, id, subject string, data any) kafkago.Message {
	t.Helper()
	m := pcMsg(t, offset, ceType, id, subject, clsAt, data)
	return m
}

// clsStore: one in-memory copy + idempotency gate + UnitOfWork with real
// rollback, plus scripted upsert failures.
type clsStore struct {
	mu        sync.Mutex
	rows      map[string]ports.ProductClassificationRecord
	claimed   map[string]bool
	upsertErr func(call int) error
	upserts   int
}

func newClsStore() *clsStore {
	return &clsStore{rows: map[string]ports.ProductClassificationRecord{}, claimed: map[string]bool{}}
}

func (s *clsStore) Execute(ctx context.Context, fn func(context.Context) error) error {
	s.mu.Lock()
	rowSnap := map[string]ports.ProductClassificationRecord{}
	for k, v := range s.rows {
		rowSnap[k] = v
	}
	claimSnap := map[string]bool{}
	for k, v := range s.claimed {
		claimSnap[k] = v
	}
	s.mu.Unlock()
	if err := fn(ctx); err != nil {
		s.mu.Lock()
		s.rows, s.claimed = rowSnap, claimSnap
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *clsStore) MarkProcessed(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed[id] {
		return false, nil
	}
	s.claimed[id] = true
	return true, nil
}

func (s *clsStore) Upsert(_ context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts++
	if s.upsertErr != nil {
		if err := s.upsertErr(s.upserts); err != nil {
			return false, err
		}
	}
	if prev, ok := s.rows[rec.SKU]; ok && prev.Version >= rec.Version {
		return false, nil
	}
	s.rows[rec.SKU] = rec
	return true, nil
}

func (s *clsStore) get(sku string) (ports.ProductClassificationRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[sku]
	return r, ok
}

func (s *clsStore) isClaimed(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed[id]
}

func (s *clsStore) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows) == 0 && len(s.claimed) == 0
}

type clsFixture struct {
	store    *clsStore
	reader   *scriptedReader
	consumer *ProductClassificationConsumer
}

func newClsFixture() *clsFixture {
	store := newClsStore()
	reader := &scriptedReader{}
	return &clsFixture{
		store: store, reader: reader,
		consumer: &ProductClassificationConsumer{
			reader: reader,
			apply:  &usecases.ApplyProductClassification{Copy: store, Processed: store, UnitOfWork: store},
		},
	}
}

func (f *clsFixture) handle(msg kafkago.Message) error {
	return f.consumer.handleMessage(context.Background(), msg)
}

func (f *clsFixture) committed() []int64 {
	c, _, _ := f.reader.snapshot()
	return c
}

func TestProductClassificationConsumer_AppliesProductClassified(t *testing.T) {
	f := newClsFixture()
	if err := f.handle(clsMsg(t, 41, clsType, clsEventID, "SKU-1", clsData("SKU-1", 3, "Hazmat", "TemperatureSensitive"))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	got, ok := f.store.get("SKU-1")
	if !ok {
		t.Fatal("the copy must hold SKU-1")
	}
	if got.Version != 3 || len(got.HandlingTags) != 2 || got.HandlingTags[0] != "Hazmat" || got.HandlingTags[1] != "TemperatureSensitive" ||
		got.TemperatureClass != "Frozen" || got.DotHazardClass != 3 {
		t.Fatalf("copy = %+v", got)
	}
	if !f.store.isClaimed(clsEventID) {
		t.Fatal("the CloudEvents id must be claimed")
	}
	if c := f.committed(); len(c) != 1 || c[0] != 41 {
		t.Fatalf("committed = %v, want [41]", c)
	}
}

func TestProductClassificationConsumer_OptionalFieldsMayBeOmitted(t *testing.T) {
	f := newClsFixture()
	data := map[string]any{"sku": "SKU-2", "handling_tags": []string{"Fragile"}, "classification_source": "legacy-import", "version": 1}
	if err := f.handle(clsMsg(t, 1, clsType, "evt-opt", "SKU-2", data)); err != nil {
		t.Fatal(err)
	}
	got, ok := f.store.get("SKU-2")
	if !ok || got.TemperatureClass != "" || got.DotHazardClass != 0 || got.HandlingTags[0] != "Fragile" {
		t.Fatalf("copy = %+v (present=%v)", got, ok)
	}
}

func TestProductClassificationConsumer_StaleVersionIsIgnoredButCommitted(t *testing.T) {
	f := newClsFixture()
	steps := []struct {
		offset int64
		id     string
		ver    int64
		tag    string
	}{
		{1, "evt-v3", 3, "Hazmat"},
		{2, "evt-v2", 2, "Fragile"},    // older, arrived late
		{3, "evt-v3b", 3, "HighValue"}, // same version, different id
	}
	for _, s := range steps {
		if err := f.handle(clsMsg(t, s.offset, clsType, s.id, "SKU-1", clsData("SKU-1", s.ver, s.tag))); err != nil {
			t.Fatalf("offset %d: %v", s.offset, err)
		}
	}
	if got, _ := f.store.get("SKU-1"); got.Version != 3 || got.HandlingTags[0] != "Hazmat" {
		t.Fatalf("copy = %+v, want version 3 Hazmat", got)
	}
	if c := f.committed(); len(c) != 3 {
		t.Fatalf("committed = %v, want all three", c)
	}
}

func TestProductClassificationConsumer_ReplayedIDIsANoOp(t *testing.T) {
	f := newClsFixture()
	if err := f.handle(clsMsg(t, 1, clsType, clsEventID, "SKU-1", clsData("SKU-1", 1, "Hazmat"))); err != nil {
		t.Fatal(err)
	}
	if err := f.handle(clsMsg(t, 2, clsType, clsEventID, "SKU-1", clsData("SKU-1", 9, "Fragile"))); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.store.get("SKU-1"); got.Version != 1 {
		t.Fatalf("copy = %+v: a replayed id must not be applied", got)
	}
	if c := f.committed(); len(c) != 2 {
		t.Fatalf("committed = %v, want both", c)
	}
}

func TestProductClassificationConsumer_OtherTypesAreIgnoredAndCommitted(t *testing.T) {
	for _, typ := range []string{
		"com.warehouse.wms.product-master.product.ProductRegistered",
		"com.warehouse.wms.product-master.product.ProductDescriptionChanged",
		"com.warehouse.wms.product-master.product.ProductDimensionsDeclared",
		"com.warehouse.wms.product-master.product.ProductMeasured",
		"com.warehouse.wms.product-master.product.ProductClassified.v2",
		"com.warehouse.wms.inventory-storage.product.ProductClassified", // the legacy producer's type
		"ProductClassified", // a short name is not the full type
	} {
		t.Run(typ, func(t *testing.T) {
			f := newClsFixture()
			if err := f.handle(clsMsg(t, 9, typ, "evt-x", "SKU-1", clsData("SKU-1", 1, "Hazmat"))); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if !f.store.empty() || f.store.upserts != 0 {
				t.Fatal("an ignored type must touch nothing")
			}
			if c := f.committed(); len(c) != 1 || c[0] != 9 {
				t.Fatalf("committed = %v, want [9]", c)
			}
		})
	}
}

func TestProductClassificationConsumer_InvalidCloudEventsAreSkippedAndCommitted(t *testing.T) {
	for name, value := range map[string][]byte{
		"legacy flat envelope":       pcLegacyFlat("evt-legacy", "ProductClassified", `{"sku":"SKU-1"}`),
		"garbage bytes":              []byte("\x00\xffnot json"),
		"json but not a cloud event": []byte(`{"hello":"world"}`),
		"empty value":                nil,
	} {
		t.Run(name, func(t *testing.T) {
			f := newClsFixture()
			if err := f.handle(kafkago.Message{Offset: 5, Value: value}); err != nil {
				t.Fatalf("an invalid CloudEvent must be skipped without error, got %v", err)
			}
			if !f.store.empty() {
				t.Fatal("nothing may be written")
			}
			if c := f.committed(); len(c) != 1 || c[0] != 5 {
				t.Fatalf("committed = %v, want [5]: the partition must not block", c)
			}
		})
	}
}

func TestProductClassificationConsumer_InvalidPayloadsAreSkippedNeverApplied(t *testing.T) {
	mut := func(f func(map[string]any)) map[string]any {
		d := clsData("SKU-1", 3, "Hazmat")
		f(d)
		return d
	}
	tests := []struct {
		name    string
		subject string
		data    any
	}{
		{"missing sku", "SKU-1", mut(func(d map[string]any) { delete(d, "sku") })},
		{"empty sku and subject", "", mut(func(d map[string]any) { d["sku"] = "" })},
		{"missing version", "SKU-1", mut(func(d map[string]any) { delete(d, "version") })},
		{"version zero", "SKU-1", mut(func(d map[string]any) { d["version"] = 0 })},
		{"version of the wrong type", "SKU-1", mut(func(d map[string]any) { d["version"] = "three" })},
		{"tags of the wrong type", "SKU-1", mut(func(d map[string]any) { d["handling_tags"] = "Hazmat" })},
		{"subject disagrees with sku", "SKU-OTHER", clsData("SKU-1", 3, "Hazmat")},
		{"data is not an object", "SKU-1", []int{1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newClsFixture()
			if err := f.handle(clsMsg(t, 7, clsType, clsEventID, tt.subject, tt.data)); err != nil {
				t.Fatalf("an invalid payload must be skipped without error, got %v", err)
			}
			if !f.store.empty() || f.store.upserts != 0 {
				t.Fatal("an invalid payload must not write or claim anything")
			}
			if c := f.committed(); len(c) != 1 || c[0] != 7 {
				t.Fatalf("committed = %v, want [7]", c)
			}
		})
	}
}

// A transient failure is returned WITHOUT committing, so the loop retries
// the same message; the rollback leaves the claim un-recorded.
func TestProductClassificationConsumer_TransientFailureIsNotCommitted(t *testing.T) {
	f := newClsFixture()
	f.store.upsertErr = func(int) error { return errors.New("connection reset by peer") }
	if err := f.handle(clsMsg(t, 3, clsType, clsEventID, "SKU-1", clsData("SKU-1", 1, "Hazmat"))); err == nil {
		t.Fatal("a transient failure must be returned so the same message is retried")
	}
	if c := f.committed(); len(c) != 0 {
		t.Fatalf("committed = %v, want nothing", c)
	}
	if f.store.isClaimed(clsEventID) {
		t.Fatal("the claim must be rolled back")
	}
}

// Run retries the SAME message until the store recovers, then commits it
// once and moves on.
func TestProductClassificationConsumer_Run_RetriesTheSameMessageUntilApplied(t *testing.T) {
	f := newClsFixture()
	f.store.upsertErr = func(call int) error {
		if call < 3 {
			return errors.New("deadlock detected")
		}
		return nil
	}
	f.reader.msgs = []kafkago.Message{
		clsMsg(t, 10, clsType, "evt-10", "SKU-1", clsData("SKU-1", 1, "Hazmat")),
		clsMsg(t, 11, "com.warehouse.wms.product-master.product.ProductRegistered", "evt-11", "SKU-1", map[string]any{"sku": "SKU-1"}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := f.consumer.Run(ctx); err != nil {
		t.Fatalf("Run returned %v; it must only stop when ctx is done", err)
	}
	if c := f.committed(); len(c) != 2 || c[0] != 10 || c[1] != 11 {
		t.Fatalf("committed = %v, want [10 11]", c)
	}
	if got, ok := f.store.get("SKU-1"); !ok || got.HandlingTags[0] != "Hazmat" {
		t.Fatalf("copy = %+v, want the message applied after the retries", got)
	}
	if f.store.upserts != 3 {
		t.Fatalf("upserts = %d, want 3 (two failures, then success)", f.store.upserts)
	}
}

func TestProductClassificationConsumer_Run_SurvivesFetchAndCommitFailures(t *testing.T) {
	f := newClsFixture()
	f.reader.fetchFailures = 1
	f.reader.commitFailures = 1
	f.reader.msgs = []kafkago.Message{clsMsg(t, 20, clsType, "evt-20", "SKU-1", clsData("SKU-1", 1, "Hazmat"))}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := f.consumer.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c := f.committed(); len(c) != 1 || c[0] != 20 {
		t.Fatalf("committed = %v, want [20]", c)
	}
	if f.store.upserts != 1 {
		t.Fatalf("upserts = %d, want 1: the commit retry hits the already-claimed id", f.store.upserts)
	}
}

func TestProductClassificationConsumer_Run_StopsWhileWaitingToRetry(t *testing.T) {
	f := newClsFixture()
	f.store.upsertErr = func(int) error { return errors.New("database is down") }
	f.reader.msgs = []kafkago.Message{clsMsg(t, 30, clsType, "evt-30", "SKU-1", clsData("SKU-1", 1, "Hazmat"))}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if err := f.consumer.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c := f.committed(); len(c) != 0 {
		t.Fatalf("committed = %v: a message that never applied must never be committed", c)
	}
}

func TestProductClassificationConsumerForTopic_UsesTheConfiguredGroupVerbatim(t *testing.T) {
	c := NewProductClassificationConsumerForTopic([]string{"broker-1:9092"}, "grp-from-config", "warehouse.product-master.events.itest", nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	if got := c.reader.Config().Topic; got != "warehouse.product-master.events.itest" {
		t.Fatalf("topic = %q", got)
	}
	if got := c.reader.Config().GroupID; got != "grp-from-config" {
		t.Fatalf("group = %q, want the configured id verbatim", got)
	}
	p := NewProductClassificationConsumer([]string{"broker-1:9092"}, "grp", nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	if got := p.reader.Config().Topic; got != ProductMasterEventsTopic || ProductMasterEventsTopic != "warehouse.product-master.events" {
		t.Fatalf("production topic = %q", got)
	}
}
