package productclassificationcopy

import (
	"context"
	"sync"

	"github.com/claudioed/order-management/internal/application/ports"
)

// MemoryStore is the in-memory copy, used when DATABASE_URL is unset and in
// tests. It applies the same version guard as PostgresStore.
type MemoryStore struct {
	mu   sync.RWMutex
	rows map[string]ports.ProductClassificationRecord
}

var (
	_ ports.ProductClassificationLookup = (*MemoryStore)(nil)
	_ ports.ProductClassificationCopy   = (*MemoryStore)(nil)
)

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: make(map[string]ports.ProductClassificationRecord)}
}

// GetClassification answers Known=true with sku's tags, or Known=false when
// the copy holds no row for sku.
func (s *MemoryStore) GetClassification(_ context.Context, sku string) (ports.ProductClassification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.rows[sku]
	if !ok {
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	}
	return ports.ProductClassification{SKU: sku, HandlingTags: append([]string{}, rec.HandlingTags...), Known: true}, nil
}

// Upsert stores rec when no row exists for rec.SKU or the stored version is
// lower than rec.Version.
func (s *MemoryStore) Upsert(_ context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.rows[rec.SKU]; ok && prev.Version >= rec.Version {
		return false, nil
	}
	rec.HandlingTags = append([]string{}, rec.HandlingTags...)
	s.rows[rec.SKU] = rec
	return true, nil
}

// MemoryProcessedEvents is the in-memory
// ports.ProductClassificationProcessedEvents.
type MemoryProcessedEvents struct {
	mu        sync.Mutex
	processed map[string]bool
}

var _ ports.ProductClassificationProcessedEvents = (*MemoryProcessedEvents)(nil)

// NewMemoryProcessedEvents constructs an empty gate.
func NewMemoryProcessedEvents() *MemoryProcessedEvents {
	return &MemoryProcessedEvents{processed: make(map[string]bool)}
}

// MarkProcessed records eventId if absent, returning true iff this call
// newly recorded it.
func (r *MemoryProcessedEvents) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.processed[eventId] {
		return false, nil
	}
	r.processed[eventId] = true
	return true, nil
}

// PermissiveLookup is PRODUCT_CLASSIFICATION_MODE=permissive (the default):
// it holds no copy and always reports Known=false, which ReceiveOrder treats
// as "no derived product attributes" — the line is evaluated with only the
// attributes it already carries (gift wrap). Unlike the permissive
// inventory client (which fails LOUD, because reserving stock must never
// appear to succeed against a no-op), this fails OPEN: classification is a
// soft routing input.
type PermissiveLookup struct{}

var _ ports.ProductClassificationLookup = PermissiveLookup{}

// NewPermissiveLookup constructs a PermissiveLookup.
func NewPermissiveLookup() PermissiveLookup {
	return PermissiveLookup{}
}

// GetClassification always reports Known=false.
func (PermissiveLookup) GetClassification(_ context.Context, sku string) (ports.ProductClassification, error) {
	return ports.ProductClassification{SKU: sku, Known: false}, nil
}
