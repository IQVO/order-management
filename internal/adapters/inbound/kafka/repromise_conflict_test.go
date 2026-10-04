package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// conflictingOrderRepo fails Save with ports.ErrConcurrentModification
// for the first `conflicts` calls (a version-column race lost to another
// writer), then delegates. attempts counts every Save call. The in-memory
// repo hands out its stored pointer, so a real repo's "re-read returns
// the persisted state" is emulated by restoring the promise groups seen
// at FindByID time when a Save is rejected.
type conflictingOrderRepo struct {
	ports.OrderRepo
	mu        sync.Mutex
	conflicts int
	attempts  int
	persisted map[shared.OrderId][]order.PromiseGroup
}

func (r *conflictingOrderRepo) FindByID(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	o, err := r.OrderRepo.FindByID(ctx, id)
	if err != nil || o == nil {
		return o, err
	}
	r.mu.Lock()
	if r.persisted == nil {
		r.persisted = map[shared.OrderId][]order.PromiseGroup{}
	}
	if _, ok := r.persisted[id]; !ok {
		r.persisted[id] = append([]order.PromiseGroup(nil), o.PromiseGroups()...)
	}
	r.mu.Unlock()
	return o, nil
}

func (r *conflictingOrderRepo) Save(ctx context.Context, o *order.Order) error {
	r.mu.Lock()
	r.attempts++
	fail := r.conflicts > 0
	if fail {
		r.conflicts--
		o.SetPromiseGroups(r.persisted[o.ID()])
	}
	r.mu.Unlock()
	if fail {
		return ports.ErrConcurrentModification
	}
	return r.OrderRepo.Save(ctx, o)
}

func (r *conflictingOrderRepo) saveAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts
}

// rollbackUoW mimics the Postgres UnitOfWork: a failed scope rolls back
// the idempotency-gate mark, so a retry of the same event is "new" again.
type rollbackUoW struct {
	processed *repromiseFakeProcessed
}

func (u *rollbackUoW) Execute(ctx context.Context, fn func(context.Context) error) error {
	before := map[string]bool{}
	for k, v := range u.processed.seen {
		before[k] = v
	}
	if err := fn(ctx); err != nil {
		u.processed.seen = before
		return err
	}
	return nil
}

func conflictConsumer(t *testing.T, conflicts int) (*RepromiseConsumer, *conflictingOrderRepo, *repromiseCapturingPublisher, *scriptedReader, shared.OrderId) {
	t.Helper()
	rf := newRepromiseFixture(order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)})
	const orderID = shared.OrderId("ord-conflict-1")
	rf.seedOrder(t, orderID, time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC))

	repo := &conflictingOrderRepo{OrderRepo: rf.orders, conflicts: conflicts}
	uc := &usecases.RepromiseOrder{
		Orders: repo, Promise: order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)},
		Events: rf.events, Clock: rf.clock, Processed: rf.processed,
		UnitOfWork: &rollbackUoW{processed: rf.processed},
	}
	reader := &scriptedReader{}
	return &RepromiseConsumer{reader: reader, repromiseOrder: uc}, repo, rf.events, reader, orderID
}

func cptMissedMessage(t *testing.T, offset int64, orderID shared.OrderId) kafkago.Message {
	t.Helper()
	e := taskCPTMissedEnvelope(t, "evt-conflict-1", usecases.WorkUnitID(orderID, 1))
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Offset: offset, Value: b}
}

// ADR-0024: a version conflict is retried in-process (the handler
// re-reads the order) and the message is committed only after the retry
// succeeds — it must never be returned as handled without a commit.
func TestRepromiseConsumer_VersionConflictThenSuccess_RetriesInProcessAndCommits(t *testing.T) {
	c, repo, events, reader, orderID := conflictConsumer(t, 1)

	if err := c.handleMessage(context.Background(), cptMissedMessage(t, 7, orderID)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	if got := repo.saveAttempts(); got != 2 {
		t.Errorf("Save attempts = %d, want 2 (one conflict, one success)", got)
	}
	if len(events.published) != 1 {
		t.Fatalf("published events = %d, want 1 OrderRepromised after the retry", len(events.published))
	}
	committed, _, _ := reader.snapshot()
	if len(committed) != 1 || committed[0] != 7 {
		t.Fatalf("committed offsets = %v, want [7] (committed only after the retry succeeded)", committed)
	}
}

// A conflict that never heals is exhausted after maxHandlerAttempts and
// then dead-lettered + committed. It is never swallowed (the old
// behaviour returned nil WITHOUT committing, so the next message's
// commit silently skipped it).
func TestRepromiseConsumer_PersistentVersionConflict_ExhaustsRetriesAndCommits(t *testing.T) {
	c, repo, events, reader, orderID := conflictConsumer(t, 1_000)

	if err := c.handleMessage(context.Background(), cptMissedMessage(t, 9, orderID)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	if got := repo.saveAttempts(); got != maxHandlerAttempts {
		t.Errorf("Save attempts = %d, want %d (bounded in-process retries)", got, maxHandlerAttempts)
	}
	if len(events.published) != 0 {
		t.Errorf("published events = %d, want 0 (every attempt lost the race)", len(events.published))
	}
	committed, _, commits := reader.snapshot()
	if commits != 1 || len(committed) != 1 || committed[0] != 9 {
		t.Fatalf("commits=%d committed=%v, want exactly [9] committed after the DLQ step", commits, committed)
	}
}
