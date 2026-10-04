package http_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// The test env's clock is 2026-08-25T09:00:00Z and its lead-time promise is
// 24h, so every order below is promised for 2026-08-26T09:00:00Z.
var (
	pcClockNow = time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	pcPromise  = time.Date(2026, 8, 26, 9, 0, 0, 0, time.UTC)
)

func pcPlan(id string, start, end time.Time) order.PlannedCapacityWindow {
	return order.PlannedCapacityWindow{
		PlanID: id, WarehouseID: "WH-7", Location: "SIM1", PathID: "pick-rebin-pack",
		Start: start, End: end, AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN", Status: order.PlannedCapacityPublished,
		AsOf: time.Date(2026, 8, 24, 21, 45, 10, 0, time.UTC),
	}
}

type pcFailingRepo struct{ err error }

func (r pcFailingRepo) Upsert(context.Context, order.PlannedCapacityWindow) (bool, error) {
	return false, r.err
}

func (r pcFailingRepo) ListByLocation(context.Context, string, time.Time) ([]order.PlannedCapacityWindow, error) {
	return nil, r.err
}

// newPlannedCapacityEnv builds the real router with the planned-capacity
// read model wired (site SIM1) over repo.
func newPlannedCapacityEnv(t *testing.T, repo ports.PlannedCapacityRepo) *testEnv {
	t.Helper()
	orders := memory.NewOrderRepo()
	inventory := &stubInventory{}
	publisher := nopPublisher{}
	clock := memory.NewFixedClock(pcClockNow)
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}

	server := &inboundhttp.Server{
		ReceiveOrder:        &usecases.ReceiveOrder{Orders: orders, Events: publisher, Clock: clock, Inventory: inventory, Promise: promise},
		RetryAllocation:     &usecases.RetryAllocation{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, Promise: promise},
		CancelOrder:         &usecases.CancelOrder{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock},
		GetOrder:            &usecases.GetOrder{Orders: orders},
		ReleaseHeld:         &usecases.ReleaseHeldOrder{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, Promise: promise},
		CapacityConstraints: &usecases.OrderCapacityConstraints{Windows: repo, Clock: clock, SiteID: "SIM1"},
		PlannedCapacity:     &usecases.GetPlannedCapacity{Windows: repo, Clock: clock},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &testEnv{handler: inboundhttp.NewRouter(server, logger, ""), orders: orders, inventory: inventory}
}

func placeOrder(t *testing.T, env *testEnv) (id string, raw string) {
	t.Helper()
	rec := env.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-9","quantity":3}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /orders = %d: %s", rec.Code, rec.Body.String())
	}
	return decodeOrder(t, rec).ID, rec.Body.String()
}

func TestOrderResponse_CapacityConstraintAnnotation(t *testing.T) {
	repo := memory.NewPlannedCapacityRepo()
	env := newPlannedCapacityEnv(t, repo)
	// A shortage window the 24h promise overlaps: 14:00-20:00 today.
	if _, err := repo.Upsert(context.Background(), pcPlan("plan-0b7a", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))); err != nil {
		t.Fatal(err)
	}

	id, created := placeOrder(t, env)
	for name, body := range map[string]string{
		"POST /orders":     created,
		"GET /orders/{id}": env.do(t, http.MethodGet, "/orders/"+id, "").Body.String(),
	} {
		for _, want := range []string{
			`"capacityConstraint":{"constrained":true,"site":"SIM1","windows":[{`,
			`"planId":"plan-0b7a"`,
			`"windowStart":"2026-08-25T14:00:00Z"`,
			`"windowEnd":"2026-08-25T20:00:00Z"`,
			`"shortage":4000`,
			`"bottleneckStep":"REBIN"`,
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("%s response lacks %s: %s", name, want, body)
			}
		}
	}

	// The annotation never touches the promise, status or lines.
	got := decodeOrder(t, env.do(t, http.MethodGet, "/orders/"+id, ""))
	if got.Status != "Released" || got.PromiseDate == nil || *got.PromiseDate != "2026-08-26T09:00:00Z" {
		t.Fatalf("order = %+v; the promise and status must be untouched", got)
	}
}

func TestOrderResponse_NoAnnotationWithoutAnOverlappingPublishedShortage(t *testing.T) {
	draft := pcPlan("plan-draft", pcClockNow.Add(time.Hour), pcClockNow.Add(2*time.Hour))
	draft.Status = order.PlannedCapacityDraft
	noShortage := pcPlan("plan-ok", pcClockNow.Add(time.Hour), pcClockNow.Add(2*time.Hour))
	noShortage.Shortage = 0
	otherSite := pcPlan("plan-other", pcClockNow.Add(time.Hour), pcClockNow.Add(2*time.Hour))
	otherSite.Location = "SIM2"

	tests := []struct {
		name    string
		windows []order.PlannedCapacityWindow
	}{
		{"empty read model", nil},
		{"draft plan", []order.PlannedCapacityWindow{draft}},
		{"published plan without a shortage", []order.PlannedCapacityWindow{noShortage}},
		{"shortage at another site", []order.PlannedCapacityWindow{otherSite}},
		{"window starts exactly at the promise cutoff", []order.PlannedCapacityWindow{pcPlan("plan-edge", pcPromise, pcPromise.Add(time.Hour))}},
		{"window ended before now", []order.PlannedCapacityWindow{pcPlan("plan-past", pcClockNow.Add(-3*time.Hour), pcClockNow)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := memory.NewPlannedCapacityRepo()
			for _, w := range tt.windows {
				if _, err := repo.Upsert(context.Background(), w); err != nil {
					t.Fatal(err)
				}
			}
			_, body := placeOrder(t, newPlannedCapacityEnv(t, repo))
			if strings.Contains(body, "capacityConstraint") {
				t.Fatalf("response must not carry the annotation: %s", body)
			}
		})
	}
}

func TestOrderResponse_OneSecondOverlapAnnotates(t *testing.T) {
	repo := memory.NewPlannedCapacityRepo()
	if _, err := repo.Upsert(context.Background(), pcPlan("plan-edge", pcPromise.Add(-time.Second), pcPromise.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	_, body := placeOrder(t, newPlannedCapacityEnv(t, repo))
	if !strings.Contains(body, `"planId":"plan-edge"`) {
		t.Fatalf("a one-second overlap must annotate: %s", body)
	}
}

// Without the read model wired (every pre-existing deployment) the response
// has no trace of the feature, even if a window exists somewhere.
func TestOrderResponse_NotWiredMeansNoAnnotationAndNoRoute(t *testing.T) {
	env := newTestEnv(t)
	_, body := placeOrder(t, env)
	if strings.Contains(body, "capacityConstraint") {
		t.Fatalf("unwired server must not annotate: %s", body)
	}
	if rec := env.do(t, http.MethodGet, "/planned-capacity?site=SIM1", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /planned-capacity without the read model = %d, want 404", rec.Code)
	}
}

// An advisory annotation must never fail an order read.
func TestOrderResponse_ReadModelFailureOmitsTheAnnotationButServesTheOrder(t *testing.T) {
	env := newPlannedCapacityEnv(t, pcFailingRepo{err: errors.New("db down")})
	id, body := placeOrder(t, env)
	if strings.Contains(body, "capacityConstraint") {
		t.Fatalf("annotation must be omitted when the lookup fails: %s", body)
	}
	if rec := env.do(t, http.MethodGet, "/orders/"+id, ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /orders/{id} = %d, want 200", rec.Code)
	}
}

func seededPlannedCapacityEnv(t *testing.T) *testEnv {
	t.Helper()
	repo := memory.NewPlannedCapacityRepo()
	published := pcPlan("plan-0b7a", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	draft := pcPlan("plan-draft", pcClockNow.Add(30*time.Hour), pcClockNow.Add(34*time.Hour))
	draft.Status, draft.Shortage = order.PlannedCapacityDraft, 17
	past := pcPlan("plan-past", pcClockNow.Add(-10*time.Hour), pcClockNow.Add(-2*time.Hour))
	other := pcPlan("plan-sim2", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	other.Location = "SIM2"
	for _, w := range []order.PlannedCapacityWindow{draft, published, past, other} {
		if _, err := repo.Upsert(context.Background(), w); err != nil {
			t.Fatal(err)
		}
	}
	return newPlannedCapacityEnv(t, repo)
}

func TestGetPlannedCapacity_ListsASitesWindowsEndingAfterNowInStartOrderAnyStatus(t *testing.T) {
	rec := seededPlannedCapacityEnv(t).do(t, http.MethodGet, "/planned-capacity?site=SIM1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		`"site":"SIM1"`, `"planId":"plan-0b7a"`, `"warehouseId":"WH-7"`, `"pathId":"pick-rebin-pack"`,
		`"assignedDemand":12000`, `"capacityOverWindow":8000`, `"shortage":4000`, `"status":"PUBLISHED"`,
		`"planId":"plan-draft"`, `"status":"DRAFT"`, `"asOf":"2026-08-24T21:45:10Z"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body lacks %s: %s", want, body)
		}
	}
	if strings.Contains(body, "plan-past") || strings.Contains(body, "plan-sim2") {
		t.Fatalf("a past window and another site must not be listed: %s", body)
	}
	if strings.Index(body, "plan-0b7a") > strings.Index(body, "plan-draft") {
		t.Fatalf("windows must be ordered by start: %s", body)
	}
}

func TestGetPlannedCapacity_FromReachesBackToIncludeAnEndedWindow(t *testing.T) {
	rec := seededPlannedCapacityEnv(t).do(t, http.MethodGet, "/planned-capacity?site=SIM1&from=2026-08-24T00:00:00Z", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "plan-past") {
		t.Fatalf("status=%d body=%s; want plan-past included", rec.Code, rec.Body.String())
	}
}

func TestGetPlannedCapacity_UnknownSiteIsAnEmptyListNotAnError(t *testing.T) {
	rec := seededPlannedCapacityEnv(t).do(t, http.MethodGet, "/planned-capacity?site=NOPE", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"windows":[]`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetPlannedCapacity_ErrorPaths(t *testing.T) {
	env := seededPlannedCapacityEnv(t)
	t.Run("missing site is a 400 problem", func(t *testing.T) {
		assertProblem(t, env.do(t, http.MethodGet, "/planned-capacity", ""), http.StatusBadRequest)
	})
	t.Run("unparseable from is a 400 problem", func(t *testing.T) {
		assertProblem(t, env.do(t, http.MethodGet, "/planned-capacity?site=SIM1&from=yesterday", ""), http.StatusBadRequest)
	})
	t.Run("a read-model failure is a 500 problem", func(t *testing.T) {
		failing := newPlannedCapacityEnv(t, pcFailingRepo{err: errors.New("db down")})
		assertProblem(t, failing.do(t, http.MethodGet, "/planned-capacity?site=SIM1", ""), http.StatusInternalServerError)
	})
}
