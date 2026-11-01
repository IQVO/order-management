package main_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Step definitions for the wave-2 coverage features (intake_validation,
// error_paths, cancellation_extended, held_orders_extended,
// allocation_events, required_ship_by, planned_capacity_extended). Like
// every other step in this suite they observe the service ONLY through its
// REST API and the recording event publisher; facts that arrive from Kafka
// (warehouse-planning events) are fed through the same use case the Kafka
// consumer drives, never through a side door.

const problemTypeBase = "https://errors.order-management.warehouse-systems.dev/"

func registerWave2Steps(sc *godog.ScenarioContext, w *world) {
	registerWave2Givens(sc, w)
	registerWave2Whens(sc, w)
	registerWave2Thens(sc, w)
}

func registerWave2Givens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^inventory-storage answers every reservation with a server error$`, w.serverErrorEverywhere)
	sc.Step(`^inventory-storage is unreachable for SKU "([^"]*)"$`, w.unreachableFor)
	sc.Step(`^inventory-storage can no longer revoke reservations$`, w.cannotRevoke)
	sc.Step(`^the site has CPT windows on path "([^"]*)" with a cycle time of (\d+) minutes:$`, w.cptWindows)
	sc.Step(`^warehouse-planning event "([^"]*)" states plan "([^"]*)":$`, w.planEvent)
}

func registerWave2Whens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^an order is placed with lines:$`, w.placeOrderWithLines)
	sc.Step(`^an order is posted with the body:$`, w.postBody)
	sc.Step(`^an order is placed for SKUs "([^"]*)" with required ship-by "([^"]*)"$`, w.placeWithShipBy)
	sc.Step(`^an order choosing process path "([^"]*)" is placed for SKU "([^"]*)"$`, w.placeWithCallerPath)
	sc.Step(`^the order "([^"]*)" is cancelled$`, w.cancelNamed)
	sc.Step(`^the order "([^"]*)" is released$`, w.releaseNamed)
	sc.Step(`^planned capacity is requested for site "([^"]*)" from "([^"]*)"$`, w.requestPlannedCapacityFrom)
}

func registerWave2Thens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the problem detail type is "([^"]*)"$`, w.problemTypeIs)
	sc.Step(`^the order has no promise date$`, w.hasNoPromiseDate)
	sc.Step(`^line (\d+) has a reservation$`, w.lineHasReservation)
	sc.Step(`^exactly (\d+) reservations? (?:was|were) revoked$`, w.revokedCountIs)
	sc.Step(`^exactly (\d+) "([^"]*)" events? (?:was|were) published$`, w.eventCountIs)
	sc.Step(`^the last "([^"]*)" event released exactly the lines "([^"]*)"$`, w.lastEventReleasedLines)
	sc.Step(`^the last "([^"]*)" event released line (\d+) on path "([^"]*)" with fulfillment class "([^"]*)" and gift wrap (true|false)$`, w.lastEventReleasedLine)
	sc.Step(`^the last "([^"]*)" event has promise basis "([^"]*)"$`, w.lastEventPromiseBasis)
	sc.Step(`^the last "OrderPartiallyAllocated" event reports (\d+) allocated and (\d+) backordered lines$`, w.partiallyAllocatedCounts)
	sc.Step(`^the last "OrderAllocationPartiallyFailed" event reports (\d+) allocated and (\d+) remaining lines$`, w.partiallyFailedCounts)
	sc.Step(`^the last "OrderCancelled" event revoked (\d+) reservations?$`, w.cancelledRevokedIs)
	sc.Step(`^the planned capacity lists plans in order "([^"]*)"$`, w.plannedCapacityOrder)
}

// ---- table and body helpers ----

// rows turns a Gherkin table into one header-keyed map per data row.
func rows(t *godog.Table) []map[string]string {
	if len(t.Rows) < 2 {
		return nil
	}
	header := t.Rows[0].Cells
	out := make([]map[string]string, 0, len(t.Rows)-1)
	for _, r := range t.Rows[1:] {
		m := make(map[string]string, len(header))
		for j, c := range header {
			m[c.Value] = r.Cells[j].Value
		}
		out = append(out, m)
	}
	return out
}

func simpleLines(skus string) []map[string]any {
	list := splitCSV(skus)
	lines := make([]map[string]any, 0, len(list))
	for _, sku := range list {
		lines = append(lines, map[string]any{"sku": sku, "quantity": 1, "giftWrap": false})
	}
	return lines
}

// postAndCapture POSTs body and, when intake accepted it, remembers the
// created order for the follow-up When/Then steps.
func (w *world) postAndCapture(body map[string]any) error {
	if err := w.do(http.MethodPost, "/orders", body); err != nil {
		return err
	}
	if w.lastStatus != http.StatusCreated {
		return nil
	}
	return w.capturePlacedOrder()
}

// ---- Given ----

func (w *world) serverErrorEverywhere() error {
	w.inventory.setMode("*", inventoryUnreachable)
	return nil
}

func (w *world) unreachableFor(sku string) error {
	w.inventory.setMode(sku, inventoryUnreachable)
	return nil
}

func (w *world) cannotRevoke() error {
	w.inventory.mu.Lock()
	defer w.inventory.mu.Unlock()
	w.inventory.revokeErr = fmt.Errorf("%w: inventory-storage: unexpected response status: 500", ports.ErrDownstreamUnavailable)
	return nil
}

// fakeSchedule is a scripted order.ScheduleSource: the site's upcoming CPT
// windows, ascending, filtered to those still ahead of the asked instant.
type fakeSchedule struct{ windows []order.CPTWindow }

func (f fakeSchedule) NextCutoffs(_ string, from time.Time, n int) ([]order.CPTWindow, bool) {
	out := []order.CPTWindow{}
	for _, win := range f.windows {
		if win.CutoffAt.After(from) && len(out) < n {
			out = append(out, win)
		}
	}
	return out, len(out) > 0
}

// fakeCapability is a scripted order.CapabilitySource: one known cycle time
// for every path.
type fakeCapability struct{ cycle time.Duration }

func (f fakeCapability) CycleTimeP95(shared.PathId) (time.Duration, bool) { return f.cycle, true }

func (w *world) cptWindows(path string, minutes int, table *godog.Table) error {
	var windows []order.CPTWindow
	for _, r := range rows(table) {
		cutoff, err := time.Parse(time.RFC3339, r["cutoffAt"])
		if err != nil {
			return fmt.Errorf("cutoffAt %q: %w", r["cutoffAt"], err)
		}
		windows = append(windows, order.CPTWindow{CptId: r["cptId"], CutoffAt: cutoff, EligiblePathIds: []string{path}})
	}
	policy := order.PromisePolicy{
		Schedule:      fakeSchedule{windows: windows},
		Capability:    fakeCapability{cycle: time.Duration(minutes) * time.Minute},
		Fallback:      order.NewLeadTimePolicy(24*time.Hour, nil),
		DefaultSiteId: "SIM1",
	}
	w.receive.Promise, w.retry.Promise, w.release.Promise = policy, policy, policy
	return nil
}

// planEvent feeds one warehouse-planning event through the real
// ApplyPlannedCapacity use case under an explicit CloudEvents id, so
// idempotency and last-writer-wins are observable.
func (w *world) planEvent(eventID, planID string, table *godog.Table) error {
	data := rows(table)
	if len(data) != 1 {
		return fmt.Errorf("planning event needs exactly one data row, got %d", len(data))
	}
	win, err := windowFromRow(planID, data[0])
	if err != nil {
		return err
	}
	return w.plannedApply.Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: eventID, Window: win})
}

func windowFromRow(planID string, r map[string]string) (order.PlannedCapacityWindow, error) {
	times := map[string]time.Time{}
	for _, key := range []string{"from", "to", "asOf"} {
		t, err := time.Parse(time.RFC3339, r[key])
		if err != nil {
			return order.PlannedCapacityWindow{}, fmt.Errorf("%s %q: %w", key, r[key], err)
		}
		times[key] = t
	}
	shortage, err := strconv.Atoi(r["shortage"])
	if err != nil {
		return order.PlannedCapacityWindow{}, fmt.Errorf("shortage %q: %w", r["shortage"], err)
	}
	return order.PlannedCapacityWindow{
		PlanID: planID, WarehouseID: "WH-1", Location: r["site"], PathID: "pick-rebin-pack",
		Start: times["from"], End: times["to"], AssignedDemand: 12000, CapacityOverWindow: float64(12000 - shortage),
		Shortage: float64(shortage), BottleneckStep: "REBIN", Status: order.PlannedCapacityStatus(r["status"]), AsOf: times["asOf"],
	}, nil
}

// ---- When ----

func (w *world) placeOrderWithLines(table *godog.Table) error {
	var lines []map[string]any
	for _, r := range rows(table) {
		quantity, err := strconv.Atoi(r["quantity"])
		if err != nil {
			return fmt.Errorf("quantity %q: %w", r["quantity"], err)
		}
		lines = append(lines, map[string]any{"sku": r["sku"], "quantity": quantity, "giftWrap": r["giftWrap"] == "true"})
	}
	return w.postAndCapture(map[string]any{"lines": lines})
}

// postBody sends a raw, possibly malformed JSON body: the rejection is the
// scenario's subject, so nothing is built or validated client-side.
func (w *world) postBody(doc *godog.DocString) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, w.server.URL+"/orders", strings.NewReader(doc.Content))
	if err != nil {
		return fmt.Errorf("build POST /orders: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.server.Client().Do(req)
	if err != nil {
		return fmt.Errorf("POST /orders: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read POST /orders response: %w", err)
	}
	w.lastStatus, w.lastBody, w.lastLocation = resp.StatusCode, raw, resp.Header.Get("Location")
	return nil
}

func (w *world) placeWithShipBy(skus, shipBy string) error {
	return w.postAndCapture(map[string]any{"lines": simpleLines(skus), "requiredShipBy": shipBy})
}

func (w *world) placeWithCallerPath(pathID, sku string) error {
	return w.postAndCapture(map[string]any{"lines": []map[string]any{{"sku": sku, "quantity": 1, "pathId": pathID}}})
}

func (w *world) cancelNamed(id string) error {
	return w.do(http.MethodDelete, "/orders/"+id, nil)
}

func (w *world) releaseNamed(id string) error {
	return w.do(http.MethodPost, "/orders/"+id+"/releases", nil)
}

func (w *world) requestPlannedCapacityFrom(site, from string) error {
	return w.do(http.MethodGet, "/planned-capacity?site="+url.QueryEscape(site)+"&from="+url.QueryEscape(from), nil)
}

// ---- Then: REST responses ----

func (w *world) problemTypeIs(slug string) error {
	got, err := w.stringField("type")
	if err != nil {
		return err
	}
	if want := problemTypeBase + slug; got != want {
		return fmt.Errorf("problem type = %q, want %q", got, want)
	}
	return nil
}

func (w *world) hasNoPromiseDate() error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	if v, present := obj["promiseDate"]; present {
		return fmt.Errorf("order unexpectedly has a promiseDate: %v", v)
	}
	return nil
}

func (w *world) lineHasReservation(lineNo int) error {
	lines, err := w.lines()
	if err != nil {
		return err
	}
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("line %d does not exist (response has %d lines)", lineNo, len(lines))
	}
	if id, _ := lines[lineNo-1].(map[string]any)["reservationId"].(string); id == "" {
		return fmt.Errorf("line %d has no reservation: %v", lineNo, lines[lineNo-1])
	}
	return nil
}

func (w *world) plannedCapacityOrder(want string) error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	windows, _ := obj["windows"].([]any)
	got := make([]string, 0, len(windows))
	for _, raw := range windows {
		m, _ := raw.(map[string]any)
		id, _ := m["planId"].(string)
		got = append(got, id)
	}
	if strings.Join(got, ",") != want {
		return fmt.Errorf("planned capacity order = %v, want %s", got, want)
	}
	return nil
}

// ---- Then: inventory and events ----

func (w *world) revokedCountIs(want int) error {
	w.inventory.mu.Lock()
	defer w.inventory.mu.Unlock()
	if got := len(w.inventory.revoked); got != want {
		return fmt.Errorf("%d reservations were revoked, want %d", got, want)
	}
	return nil
}

func (w *world) eventsNamed(name string) []shared.DomainEvent {
	w.publisher.mu.Lock()
	defer w.publisher.mu.Unlock()
	var out []shared.DomainEvent
	for _, e := range w.publisher.events {
		if e.EventName() == name {
			out = append(out, e)
		}
	}
	return out
}

func (w *world) lastEvent(name string) (shared.DomainEvent, error) {
	events := w.eventsNamed(name)
	if len(events) == 0 {
		return nil, fmt.Errorf("no %s event was published (captured: %v)", name, w.eventCounts())
	}
	return events[len(events)-1], nil
}

func (w *world) eventCountIs(want int, name string) error {
	if got := len(w.eventsNamed(name)); got != want {
		return fmt.Errorf("%d %s events were published, want %d (captured: %v)", got, name, want, w.eventCounts())
	}
	return nil
}

// releasedLinesOf returns the released-line payload of the two integration
// events that carry one.
func releasedLinesOf(ev shared.DomainEvent) ([]shared.ReleasedLine, error) {
	switch e := ev.(type) {
	case shared.OrderAllocated:
		return e.Lines, nil
	case shared.OrderPartiallyAllocated:
		return e.Lines, nil
	default:
		return nil, fmt.Errorf("%s carries no released lines", ev.EventName())
	}
}

func (w *world) lastReleasedLines(name string) ([]shared.ReleasedLine, error) {
	ev, err := w.lastEvent(name)
	if err != nil {
		return nil, err
	}
	return releasedLinesOf(ev)
}

func (w *world) lastEventReleasedLines(name, want string) error {
	lines, err := w.lastReleasedLines(name)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(lines))
	for _, l := range lines {
		got = append(got, strconv.Itoa(l.LineNo))
	}
	if strings.Join(got, ",") != want {
		return fmt.Errorf("%s released lines [%s], want [%s]", name, strings.Join(got, ","), want)
	}
	return nil
}

func (w *world) lastEventReleasedLine(name string, lineNo int, path, class, gift string) error {
	lines, err := w.lastReleasedLines(name)
	if err != nil {
		return err
	}
	for _, l := range lines {
		if l.LineNo != lineNo {
			continue
		}
		if l.PathID.String() != path || l.FulfillmentClass != class || strconv.FormatBool(l.GiftWrap) != gift {
			return fmt.Errorf("released line %d = path %s, class %s, giftWrap %v; want path %s, class %s, giftWrap %s",
				lineNo, l.PathID, l.FulfillmentClass, l.GiftWrap, path, class, gift)
		}
		return nil
	}
	return fmt.Errorf("%s did not release line %d", name, lineNo)
}

func (w *world) lastEventPromiseBasis(name, want string) error {
	ev, err := w.lastEvent(name)
	if err != nil {
		return err
	}
	allocated, ok := ev.(shared.OrderAllocated)
	if !ok {
		return fmt.Errorf("%s has no promise basis", name)
	}
	if allocated.PromiseBasis != want {
		return fmt.Errorf("%s promise basis = %q, want %q", name, allocated.PromiseBasis, want)
	}
	return nil
}

func (w *world) partiallyAllocatedCounts(allocated, backordered int) error {
	ev, err := w.lastEvent("OrderPartiallyAllocated")
	if err != nil {
		return err
	}
	e, _ := ev.(shared.OrderPartiallyAllocated)
	if e.AllocatedLines != allocated || e.BackorderedLines != backordered {
		return fmt.Errorf("OrderPartiallyAllocated = %d allocated / %d backordered, want %d / %d",
			e.AllocatedLines, e.BackorderedLines, allocated, backordered)
	}
	return nil
}

func (w *world) partiallyFailedCounts(allocated, remaining int) error {
	ev, err := w.lastEvent("OrderAllocationPartiallyFailed")
	if err != nil {
		return err
	}
	e, _ := ev.(shared.OrderAllocationPartiallyFailed)
	if e.AllocatedLines != allocated || e.RemainingLines != remaining {
		return fmt.Errorf("OrderAllocationPartiallyFailed = %d allocated / %d remaining, want %d / %d",
			e.AllocatedLines, e.RemainingLines, allocated, remaining)
	}
	return nil
}

func (w *world) cancelledRevokedIs(want int) error {
	ev, err := w.lastEvent("OrderCancelled")
	if err != nil {
		return err
	}
	e, _ := ev.(shared.OrderCancelled)
	if e.RevokedReservations != want {
		return fmt.Errorf("OrderCancelled revoked %d reservations, want %d", e.RevokedReservations, want)
	}
	return nil
}
