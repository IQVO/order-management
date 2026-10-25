//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// rcNow is "now" for these tests. It must track the REAL clock: the sweeper's
// 7-day CONFIRMED-row retention is measured against wall-clock time, so a
// hard-coded date silently turned this suite red once it aged past the
// retention window (develop CI went red on 2026-10-24). Whole seconds, UTC,
// so Postgres round-trips are exact.
var rcNow = time.Now().UTC().Truncate(time.Second)

func openPending(t *testing.T, repo *postgres.ReleaseConfirmationRepo, orderID shared.OrderId, lineNo int, releasedAt time.Time) releaseconfirmation.Confirmation {
	t.Helper()
	c, err := releaseconfirmation.NewPending(orderID, lineNo, usecases.WorkUnitID(orderID, lineNo), "pick", releasedAt)
	if err != nil {
		t.Fatalf("NewPending: %v", err)
	}
	if err := repo.Open(context.Background(), c); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return c
}

// newFlagUseCase wires FlagUnconfirmedReleases over real Postgres exactly as
// cmd/order does: the transactional outbox is the publisher and the Postgres
// UnitOfWork brackets the claim, the state change and the outbox row.
func newFlagUseCase(pool *pgxpool.Pool, repo *postgres.ReleaseConfirmationRepo, now time.Time) *usecases.FlagUnconfirmedReleases {
	analytics := &outboundkafka.AnalyticsPublisher{NewID: uuid.NewString}
	return &usecases.FlagUnconfirmedReleases{
		Confirmations: repo,
		Events:        postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisher(nil), analytics),
		Clock:         fixedClock{t: now},
		UnitOfWork:    postgres.NewUnitOfWork(pool),
		Timeout:       15 * time.Minute,
		BatchSize:     7,
	}
}

func stateCount(t *testing.T, pool *pgxpool.Pool, state string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM release_confirmations WHERE state = $1`, state).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", state, err)
	}
	return n
}

func TestReleaseConfirmationRepo_RoundTripAndIdempotentOpen(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)

	opened := openPending(t, repo, "ord-1", 2, rcNow)

	got, found, err := repo.Find(ctx, "ord-1", 2)
	if err != nil || !found {
		t.Fatalf("Find = %v found=%v", err, found)
	}
	if got.State() != releaseconfirmation.StatePending || got.WorkUnitID() != "ord-1-line-2" || got.PathID() != "pick" ||
		!got.ReleasedAt().Equal(rcNow) || got.ConfirmedAt() != nil || got.UnconfirmedAt() != nil {
		t.Fatalf("row = %+v", got)
	}

	if _, found, err := repo.Find(ctx, "ord-1", 3); err != nil || found {
		t.Fatalf("Find of an unknown line = found %v err %v, want not found", found, err)
	}
	if _, found, err := repo.Find(ctx, "ord-9", 2); err != nil || found {
		t.Fatalf("Find of an unknown order = found %v err %v, want not found", found, err)
	}

	// A second Open (a re-release) must not reset the line's history.
	got.Confirm(rcNow.Add(time.Second))
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Open(ctx, opened); err != nil {
		t.Fatalf("second Open: %v", err)
	}
	again, _, _ := repo.Find(ctx, "ord-1", 2)
	if again.State() != releaseconfirmation.StateConfirmed || again.ConfirmedAt() == nil || !again.ConfirmedAt().Equal(rcNow.Add(time.Second)) {
		t.Fatalf("after a second Open the row = %s %v, want it untouched (CONFIRMED)", again.State(), again.ConfirmedAt())
	}
}

func TestReleaseConfirmationRepo_SaveOfAMissingRowIsAnError(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewReleaseConfirmationRepo(pool)
	c, _ := releaseconfirmation.NewPending("ord-x", 1, "ord-x-line-1", "pick", rcNow)
	if err := repo.Save(context.Background(), c); err == nil {
		t.Fatal("Save of a row that was never opened must fail, not silently lose the transition")
	}
}

func TestReleaseConfirmationRepo_TheStateCheckConstraintRejectsAnUnknownState(t *testing.T) {
	pool := outboxDB(t)
	_, err := pool.Exec(context.Background(), `INSERT INTO release_confirmations
		(order_id, line_no, work_unit_id, released_at, state) VALUES ('o', 1, 'o-line-1', now(), 'LOST')`)
	if err == nil {
		t.Fatal("the table must only accept PENDING, CONFIRMED and UNCONFIRMED")
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO release_confirmations
		(order_id, line_no, work_unit_id, released_at) VALUES ('o', 0, 'o-line-0', now())`)
	if err == nil {
		t.Fatal("the table must reject a non-positive line number")
	}
}

func TestReleaseConfirmationRepo_ClaimOverdueOrdersLimitsAndSkipsLockedRows(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)

	cutoff := rcNow
	openPending(t, repo, "ord-b", 1, cutoff.Add(-2*time.Hour))
	openPending(t, repo, "ord-a", 1, cutoff.Add(-3*time.Hour))
	openPending(t, repo, "ord-c", 1, cutoff.Add(-time.Hour))
	openPending(t, repo, "ord-edge", 1, cutoff)                 // exactly at the cutoff: not overdue
	openPending(t, repo, "ord-new", 1, cutoff.Add(time.Minute)) // younger
	confirmed := openPending(t, repo, "ord-done", 1, cutoff.Add(-4*time.Hour))
	confirmed.Confirm(cutoff.Add(-3 * time.Hour))
	if err := repo.Save(ctx, confirmed); err != nil {
		t.Fatal(err)
	}

	uow := postgres.NewUnitOfWork(pool)
	err := uow.Execute(ctx, func(ctx context.Context) error {
		first, err := repo.ClaimOverdue(ctx, cutoff, 2)
		if err != nil {
			return err
		}
		if len(first) != 2 || first[0].OrderID() != "ord-a" || first[1].OrderID() != "ord-b" {
			t.Errorf("first claim = %v, want the two oldest PENDING rows, oldest first (ord-a, ord-b)", ids(first))
		}

		// A concurrent transaction (another replica's sweep) must skip the
		// rows this one holds, and take the next one instead.
		var other []releaseconfirmation.Confirmation
		if err := uow.Execute(context.Background(), func(ctx2 context.Context) error {
			var err error
			other, err = repo.ClaimOverdue(ctx2, cutoff, 5)
			return err
		}); err != nil {
			return err
		}
		if len(other) != 1 || other[0].OrderID() != "ord-c" {
			t.Errorf("concurrent claim = %v, want only the unlocked ord-c", ids(other))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("uow: %v", err)
	}
}

func ids(cs []releaseconfirmation.Confirmation) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.OrderID().String())
	}
	return out
}

func TestReleaseConfirmationProcessedEventsRepo_ClaimsOnce(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewReleaseConfirmationProcessedEventsRepo(pool)
	ctx := context.Background()
	if first, err := repo.MarkProcessed(ctx, "evt-1"); err != nil || !first {
		t.Fatalf("first = %v, %v; want newly recorded", first, err)
	}
	if again, err := repo.MarkProcessed(ctx, "evt-1"); err != nil || again {
		t.Fatalf("again = %v, %v; want already recorded", again, err)
	}
}

// A rolled-back transaction un-claims the id AND leaves the row PENDING --
// the atomicity the consumer's redelivery relies on (fleet escape 19).
func TestConfirmRelease_RealPostgres_ARollbackUnclaimsTheEventAndKeepsTheRowPending(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)
	processed := postgres.NewReleaseConfirmationProcessedEventsRepo(pool)
	openPending(t, repo, "ord-1", 1, rcNow)

	boom := fmt.Errorf("boom after the claim")
	uc := &usecases.ConfirmRelease{
		Confirmations: failingSave{ReleaseConfirmationRepo: repo, err: boom},
		Processed:     processed,
		Clock:         fixedClock{t: rcNow},
		UnitOfWork:    postgres.NewUnitOfWork(pool),
	}
	if _, err := uc.Execute(ctx, usecases.ConfirmReleaseRequest{EventID: "evt-1", Reference: "ord-1", LineNo: 1}); err == nil {
		t.Fatal("the failing Save must surface")
	}
	if first, _ := processed.MarkProcessed(ctx, "evt-1"); !first {
		t.Fatal("the claim survived the rollback: a redelivery would be skipped as already handled")
	}
	if got := stateCount(t, pool, "PENDING"); got != 1 {
		t.Fatalf("PENDING rows = %d, want 1", got)
	}
}

type failingSave struct {
	*postgres.ReleaseConfirmationRepo
	err error
}

func (f failingSave) Save(context.Context, releaseconfirmation.Confirmation) error { return f.err }

func TestConfirmRelease_RealPostgres_ConfirmsAPendingAndAnUnconfirmedLine(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)
	uc := &usecases.ConfirmRelease{
		Confirmations: repo, Processed: postgres.NewReleaseConfirmationProcessedEventsRepo(pool),
		Clock: fixedClock{t: rcNow}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	openPending(t, repo, "ord-1", 1, rcNow.Add(-time.Minute))
	flagged := openPending(t, repo, "ord-1", 2, rcNow.Add(-time.Hour))
	flagged.MarkUnconfirmed(rcNow.Add(-30 * time.Minute))
	if err := repo.Save(ctx, flagged); err != nil {
		t.Fatal(err)
	}

	for lineNo, want := range map[int]string{1: "confirmed", 2: "late_confirmed"} {
		got, err := uc.Execute(ctx, usecases.ConfirmReleaseRequest{EventID: fmt.Sprintf("evt-%d", lineNo), Reference: "ord-1", LineNo: lineNo})
		if err != nil || string(got) != want {
			t.Fatalf("line %d: outcome %q err %v, want %q", lineNo, got, err, want)
		}
	}
	if got := stateCount(t, pool, "CONFIRMED"); got != 2 {
		t.Fatalf("CONFIRMED rows = %d, want 2", got)
	}
	late, _, _ := repo.Find(ctx, "ord-1", 2)
	if late.UnconfirmedAt() == nil || late.ConfirmedAt() == nil || !late.ConfirmedAt().Equal(rcNow) {
		t.Fatalf("late row = %v/%v, want both times kept", late.UnconfirmedAt(), late.ConfirmedAt())
	}
}

// TestSweeper_FlagsOverdueReleaseConfirmationsExactlyOnce proves the ADR
// 0043 timeout sweep over real Postgres, driven by the real Sweeper: only
// PENDING rows older than the timeout are flagged, each raises exactly ONE
// analytics outbox row in the same transaction, a second pass flags nothing,
// and two replicas sweeping at the same time flag every line exactly once.
func TestSweeper_FlagsOverdueReleaseConfirmationsExactlyOnce(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)

	const overdue = 25
	for i := 1; i <= overdue; i++ {
		openPending(t, repo, shared.OrderId(fmt.Sprintf("ord-%02d", i)), 1, rcNow.Add(-time.Hour))
	}
	openPending(t, repo, "ord-young", 1, rcNow.Add(-10*time.Minute)) // inside the 15m timeout
	done := openPending(t, repo, "ord-done", 1, rcNow.Add(-2*time.Hour))
	done.Confirm(rcNow.Add(-time.Hour))
	if err := repo.Save(ctx, done); err != nil {
		t.Fatal(err)
	}

	newSweeper := func() *postgres.Sweeper {
		uc := newFlagUseCase(pool, repo, rcNow)
		return postgres.NewSweeper(pool, postgres.WithReleaseConfirmationSweep(uc.Execute))
	}

	var wg sync.WaitGroup
	results := make([]postgres.SweepResult, 2)
	errs := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = newSweeper().SweepOnce(ctx)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("sweeper %d: %v", i, err)
		}
	}
	if total := results[0].ReleasesUnconfirmed + results[1].ReleasesUnconfirmed; total != overdue {
		t.Fatalf("two concurrent sweepers flagged %d+%d lines, want %d in total (exactly once each)",
			results[0].ReleasesUnconfirmed, results[1].ReleasesUnconfirmed, overdue)
	}
	if got := stateCount(t, pool, "UNCONFIRMED"); got != overdue {
		t.Fatalf("UNCONFIRMED rows = %d, want %d", got, overdue)
	}
	if got := stateCount(t, pool, "PENDING"); got != 1 {
		t.Fatalf("PENDING rows = %d, want only the young one", got)
	}
	if got := stateCount(t, pool, "CONFIRMED"); got != 1 {
		t.Fatalf("CONFIRMED rows = %d, want the confirmed one untouched", got)
	}

	const unconfirmedType = "com.warehouse.wes.order-management.order.OrderReleaseUnconfirmed"
	if got := countOutboxWhereTopic(t, pool, outboundkafka.AnalyticsTopic, unconfirmedType); got != overdue {
		t.Fatalf("analytics OrderReleaseUnconfirmed rows = %d, want %d (one per flagged line)", got, overdue)
	}
	if got := countOutboxWhereTopic(t, pool, outboundkafka.Topic, unconfirmedType); got != 0 {
		t.Fatalf("integration topic rows = %d, want 0: the integration contract keeps its three events", got)
	}

	// A later pass, long after: nothing is flagged a second time.
	res, err := newSweeper().SweepOnce(ctx)
	if err != nil || res.ReleasesUnconfirmed != 0 {
		t.Fatalf("second pass = %+v, %v; want 0 newly flagged", res, err)
	}
	if got := countOutboxWhereTopic(t, pool, outboundkafka.AnalyticsTopic, unconfirmedType); got != overdue {
		t.Fatalf("outbox rows after the second pass = %d, want still %d", got, overdue)
	}
}

func TestSweeper_APublishFailureRollsTheFlagBackSoTheNextPassRetriesIt(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)
	openPending(t, repo, "ord-1", 1, rcNow.Add(-time.Hour))

	uc := newFlagUseCase(pool, repo, rcNow)
	uc.Events = brokenPublisher{}
	s := postgres.NewSweeper(pool, postgres.WithReleaseConfirmationSweep(uc.Execute))
	if _, err := s.SweepOnce(ctx); err == nil {
		t.Fatal("a failing publish must surface from the sweep")
	}
	if got := stateCount(t, pool, "PENDING"); got != 1 {
		t.Fatalf("PENDING rows = %d, want 1: the state change must roll back with the failed fact", got)
	}

	healthy := newFlagUseCase(pool, repo, rcNow)
	res, err := postgres.NewSweeper(pool, postgres.WithReleaseConfirmationSweep(healthy.Execute)).SweepOnce(ctx)
	if err != nil || res.ReleasesUnconfirmed != 1 {
		t.Fatalf("next pass = %+v, %v; want the line flagged now", res, err)
	}
}

type brokenPublisher struct{}

func (brokenPublisher) Publish(context.Context, shared.DomainEvent) error {
	return fmt.Errorf("outbox insert failed")
}

func TestSweeper_ReleaseConfirmationTimeoutZeroDisablesTheSweepAndNilHookIsOff(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)
	openPending(t, repo, "ord-1", 1, rcNow.Add(-1000*time.Hour))

	uc := newFlagUseCase(pool, repo, rcNow)
	uc.Timeout = 0
	res, err := postgres.NewSweeper(pool, postgres.WithReleaseConfirmationSweep(uc.Execute)).SweepOnce(ctx)
	if err != nil || res.ReleasesUnconfirmed != 0 {
		t.Fatalf("timeout 0: %+v, %v; want nothing flagged", res, err)
	}
	if res, err := postgres.NewSweeper(pool).SweepOnce(ctx); err != nil || res.ReleasesUnconfirmed != 0 {
		t.Fatalf("no hook: %+v, %v; want nothing flagged", res, err)
	}
	if got := stateCount(t, pool, "PENDING"); got != 1 {
		t.Fatalf("PENDING rows = %d, want 1", got)
	}
}

func TestSweeper_PrunesOnlyConfirmedReleaseConfirmationsPastRetention(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewReleaseConfirmationRepo(pool)

	mk := func(orderID shared.OrderId, state string, confirmedAgo time.Duration) {
		c := openPending(t, repo, orderID, 1, time.Now().Add(-30*24*time.Hour))
		switch state {
		case "CONFIRMED":
			c.Confirm(time.Now().Add(-confirmedAgo))
		case "UNCONFIRMED":
			c.MarkUnconfirmed(time.Now().Add(-confirmedAgo))
		}
		if err := repo.Save(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	mk("old-confirmed-1", "CONFIRMED", 8*24*time.Hour)
	mk("old-confirmed-2", "CONFIRMED", 9*24*time.Hour)
	mk("fresh-confirmed", "CONFIRMED", time.Hour)
	mk("old-unconfirmed", "UNCONFIRMED", 20*24*time.Hour) // an open problem: never pruned
	mk("old-pending", "PENDING", 0)                       // an open problem: never pruned

	s := postgres.NewSweeper(pool, postgres.WithSweepBatchSize(1)) // default 7d retention
	res, err := s.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if res.ReleaseConfirmationsPruned != 2 {
		t.Fatalf("pruned = %d, want the 2 CONFIRMED rows past 7 days (in batches of 1)", res.ReleaseConfirmationsPruned)
	}
	for _, keep := range []string{"fresh-confirmed", "old-unconfirmed", "old-pending"} {
		if _, found, _ := repo.Find(ctx, shared.OrderId(keep), 1); !found {
			t.Fatalf("%s was pruned; only CONFIRMED rows older than the retention may be", keep)
		}
	}

	// A retention of 0 keeps CONFIRMED rows forever.
	mk("ancient-confirmed", "CONFIRMED", 400*24*time.Hour)
	res, err = postgres.NewSweeper(pool, postgres.WithReleaseConfirmationRetention(0)).SweepOnce(ctx)
	if err != nil || res.ReleaseConfirmationsPruned != 0 {
		t.Fatalf("retention 0: %+v, %v; want nothing pruned", res, err)
	}
}
