package main_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Step definitions for features/order_rules.feature and
// features/event_driven_rules.feature: scenarios derived from the Order
// Management documentation (business-context, use-cases,
// aggregate-design-canvas, domain-events, troubleshooting).
//
// Everything reachable over REST is driven over REST. Facts that arrive from
// Kafka in production (fulfillment-execution, wes-work-planning,
// outbound-shipping, product-master) are fed through the SAME use case the
// consumer calls, exactly as the existing planned-capacity steps do; the
// consumers' own decode step (order_ref -> order id + line) is mirrored with
// usecases.ParseWorkUnitID, the function the consumer uses.

// rulesState holds the doubles and use cases that only these features need.
// It is rebuilt before every scenario (world.reset).
type rulesState struct {
	readiness *inboundhttp.Readiness
	policy    order.PromisePolicy
	capacity  fakeCapacity

	repromise     *usecases.RepromiseOrder
	shipment      *usecases.ConfirmShipment
	confirmations *fakeConfirmations
	confirm       *usecases.ConfirmRelease
	flag          *usecases.FlagUnconfirmedReleases
	classes       *productclassificationcopy.MemoryStore
	classify      *usecases.ApplyProductClassification

	named        map[string]string
	reserveMark  int
	lastOutcome  string
	lastFlagged  int
	lastClassErr error
	shipOutcomes map[shared.OrderId]usecases.ShipmentOrderOutcome
}

func newRulesState(w *world) *rulesState {
	r := &rulesState{
		readiness: &inboundhttp.Readiness{},
		policy:    w.receive.Promise,
		capacity:  fakeCapacity{},
		named:     map[string]string{},
	}
	r.repromise = &usecases.RepromiseOrder{
		Orders: w.orders, Promise: r.policy, Events: w.publisher, Clock: w.clock,
		Processed: memory.NewRepromiseProcessedEventsRepo(),
	}
	r.shipment = &usecases.ConfirmShipment{
		Orders: w.orders, Events: w.publisher, Clock: w.clock,
		Processed: memory.NewShipmentProcessedEventsRepo(),
	}
	r.confirmations = &fakeConfirmations{rows: map[string]releaseconfirmation.Confirmation{}}
	r.confirm = &usecases.ConfirmRelease{
		Confirmations: r.confirmations, Processed: memory.NewRepromiseProcessedEventsRepo(), Clock: w.clock,
	}
	r.flag = &usecases.FlagUnconfirmedReleases{
		Confirmations: r.confirmations, Events: w.publisher, Clock: w.clock,
	}
	r.classes = productclassificationcopy.NewMemoryStore()
	r.classify = &usecases.ApplyProductClassification{
		Copy: r.classes, Processed: productclassificationcopy.NewMemoryProcessedEvents(),
	}
	w.receive.Classification = r.classes
	return r
}

// setPromise swaps the promise policy of every use case that computes one.
func (r *rulesState) setPromise(w *world, policy order.PromisePolicy) {
	r.policy = policy
	w.receive.Promise, w.retry.Promise, w.release.Promise = policy, policy, policy
	r.repromise.Promise = policy
}

// fakeCapacity is a scripted order.CapacitySource: remaining units per CPT id.
type fakeCapacity map[string]int

func (f fakeCapacity) Remaining(_ shared.PathId, cptID string, _ time.Time) (int, bool) {
	n, ok := f[cptID]
	return n, ok
}

// fakeCatalogue is a scripted ports.ProcessPathCatalogue.
type fakeCatalogue struct{ paths []shared.ActivePathCandidate }

func (c *fakeCatalogue) IsActive(id shared.PathId) bool {
	for _, p := range c.paths {
		if p.PathId == id {
			return true
		}
	}
	return false
}

func (c *fakeCatalogue) CycleTimeP95(id shared.PathId) (time.Duration, bool) {
	for _, p := range c.paths {
		if p.PathId == id {
			return p.CycleTimeP95, p.CycleTimeKnown
		}
	}
	return 0, false
}

func (c *fakeCatalogue) Eligibility(id shared.PathId) (shared.Eligibility, bool) {
	for _, p := range c.paths {
		if p.PathId == id {
			return p.Eligibility, true
		}
	}
	return shared.Eligibility{}, false
}

func (c *fakeCatalogue) ListActive() []shared.ActivePathCandidate {
	return append([]shared.ActivePathCandidate(nil), c.paths...)
}

// fakeConfirmations is an in-memory ports.ReleaseConfirmations.
type fakeConfirmations struct {
	mu   sync.Mutex
	rows map[string]releaseconfirmation.Confirmation
}

func confirmationKey(id shared.OrderId, lineNo int) string { return fmt.Sprintf("%s#%d", id, lineNo) }

func (f *fakeConfirmations) Open(_ context.Context, c releaseconfirmation.Confirmation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := confirmationKey(c.OrderID(), c.LineNo())
	if _, exists := f.rows[key]; !exists {
		f.rows[key] = c
	}
	return nil
}

func (f *fakeConfirmations) Find(_ context.Context, id shared.OrderId, lineNo int) (releaseconfirmation.Confirmation, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.rows[confirmationKey(id, lineNo)]
	return c, ok, nil
}

func (f *fakeConfirmations) Save(_ context.Context, c releaseconfirmation.Confirmation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[confirmationKey(c.OrderID(), c.LineNo())] = c
	return nil
}

func (f *fakeConfirmations) ClaimOverdue(_ context.Context, releasedBefore time.Time, limit int) ([]releaseconfirmation.Confirmation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []releaseconfirmation.Confirmation
	for _, c := range f.rows {
		if c.Overdue(releasedBefore) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ReleasedAt().Equal(out[j].ReleasedAt()) {
			return out[i].ReleasedAt().Before(out[j].ReleasedAt())
		}
		return confirmationKey(out[i].OrderID(), out[i].LineNo()) < confirmationKey(out[j].OrderID(), out[j].LineNo())
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func registerRulesSteps(sc *godog.ScenarioContext, w *world) {
	registerRulesGivens(sc, w)
	registerRulesWhens(sc, w)
	registerRulesThens(sc, w)
	registerRulesEventThens(sc, w)
	registerRulesConsumerSteps(sc, w)
}

func registerRulesGivens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the reservation for SKU "([^"]*)" has lapsed upstream$`, w.insufficientFor)
	sc.Step(`^inventory-storage is not configured$`, w.inventoryNotConfigured)
	sc.Step(`^inventory-storage cannot revoke the reservation for SKU "([^"]*)"$`, w.cannotRevokeFor)
	sc.Step(`^inventory-storage can revoke reservations again$`, w.canRevokeAgain)
	sc.Step(`^path "([^"]*)" has capacity for (\d+) units before CPT "([^"]*)"$`, w.pathCapacity)
	sc.Step(`^the clock advances (\d+) minutes$`, w.clockAdvances)
	sc.Step(`^the order is called "([^"]*)"$`, w.nameOrder)
	sc.Step(`^process-path-management declares the paths:$`, w.declarePaths)
	sc.Step(`^product-master has classified SKU "([^"]*)" as "([^"]*)" at version (\d+) in event "([^"]*)"$`, w.classifyStrict)
	sc.Step(`^the release confirmation loop is on$`, w.confirmationLoopOn)
}

func registerRulesWhens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the order "([^"]*)" is retried for allocation$`, w.retryNamed)
	sc.Step(`^an order allowing partial shipment is placed with lines:$`, w.placePartialWithLines)
	sc.Step(`^an order of (\d+) units of SKU "([^"]*)" is placed with required ship-by "([^"]*)"$`, w.placeUnitsWithShipBy)
	sc.Step(`^the service is probed for readiness$`, w.probeReadiness)
	sc.Step(`^the service begins shutting down$`, w.beginShutdown)
	sc.Step(`^product-master publishes event "([^"]*)" classifying SKU "([^"]*)" as "([^"]*)" at version (\d+)$`, w.classifyLoose)
	sc.Step(`^warehouse-planning event "([^"]*)" for plan "([^"]*)" is refused as invalid:$`, w.planRefused)
}

func registerRulesThens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the response flag "([^"]*)" is (true|false)$`, w.responseFlagIs)
	sc.Step(`^line (\d+) is for SKU "([^"]*)" with quantity (\d+) and gift wrap (true|false)$`, w.lineIs)
	sc.Step(`^inventory-storage received (\d+) reservation requests?$`, w.reservationCountIs)
	sc.Step(`^reservation request (\d+) asked for (\d+) of SKU "([^"]*)" for line (\d+) of the order$`, w.reservationRequestIs)
	sc.Step(`^the reservation requests are remembered$`, w.rememberReservations)
	sc.Step(`^no new reservation request was made$`, w.noNewReservation)
	sc.Step(`^the order is capacity-constrained at site "([^"]*)" by plan "([^"]*)"$`, w.constrainedAtSite)
	sc.Step(`^the order is not constrained by plan "([^"]*)"$`, w.notConstrainedBy)
	sc.Step(`^the classification is accepted$`, w.classificationAccepted)
	sc.Step(`^the classification is rejected as invalid$`, w.classificationRejected)
	sc.Step(`^no plan is listed for site "([^"]*)"$`, w.noPlanListedFor)
}

func registerRulesEventThens(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the OrderLineReleased event for line (\d+) carries the work unit id "([^"]*)"$`, w.releasedLineWorkUnit)
	sc.Step(`^the last "OrderAllocated" event has promise CPT "([^"]*)"$`, w.allocatedPromiseCPT)
	sc.Step(`^the last "OrderAllocated" event has no promise CPT$`, w.allocatedNoPromiseCPT)
	sc.Step(`^the last "OrderAllocated" event released line (\d+) under promise CPT "([^"]*)"$`, w.releasedLinePromiseCPT)
	sc.Step(`^the last "OrderAllocationPartiallyFailed" event reports (\d+) allocated lines$`, w.partiallyFailedAllocated)
	sc.Step(`^the last "SiteSkuDemandChanged" event is for line (\d+) due at "([^"]*)"$`, w.lastDemandLineDue)
	sc.Step(`^the last "OrderRepromised" event moved the promise from CPT "([^"]*)" to "([^"]*)" because of "([^"]*)"$`, w.repromisedFromTo)
	sc.Step(`^the last "(OrderLineShipped|OrderShipped|OrderPartiallyShipped)" event carries load "([^"]*)"$`, w.shippedEventLoad)
	sc.Step(`^the last "OrderReleaseUnconfirmed" event is for line (\d+) with work unit id "([^"]*)"$`, w.unconfirmedEventFor)
}

func registerRulesConsumerSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^fulfillment-execution reports (TaskCPTMissed|PackageManifested) event "([^"]*)" for line (\d+) of the order$`, w.reportMissedLine)
	sc.Step(`^fulfillment-execution reports (TaskCPTMissed|PackageManifested) event "([^"]*)" for line (\d+) of order "([^"]*)"$`, w.reportMissedLineOf)
	sc.Step(`^fulfillment-execution reports TaskCPTMissed event "([^"]*)" for order_ref "([^"]*)"$`, w.reportMissedRefStep)
	sc.Step(`^wes-work-planning confirms line (\d+) of the order in event "([^"]*)"$`, w.confirmLine)
	sc.Step(`^wes-work-planning publishes WorkUnitCreated "([^"]*)" for reference "([^"]*)" and line (\d+)$`, w.workUnitCreated)
	sc.Step(`^the confirmation outcome is "([^"]*)"$`, w.confirmationOutcomeIs)
	sc.Step(`^the confirmation of line (\d+) is "([^"]*)"$`, w.confirmationStateIs)
	sc.Step(`^there is no confirmation for line (\d+)$`, w.noConfirmationFor)
	sc.Step(`^the confirmation of line (\d+) keeps the time it was flagged$`, w.confirmationKeepsFlagTime)
	sc.Step(`^the housekeeping sweep runs with a confirmation timeout of (\d+) minutes$`, w.sweepRuns)
	sc.Step(`^the sweep flagged (\d+) lines?$`, w.sweepFlagged)
	sc.Step(`^outbound-shipping confirms load "([^"]*)" in event "([^"]*)" for orders "([^"]*)"$`, w.shipmentConfirmed)
	sc.Step(`^outbound-shipping confirms load "([^"]*)" in event "([^"]*)" naming no orders$`, w.shipmentConfirmedEmpty)
	sc.Step(`^the shipment outcome for "([^"]*)" is "([^"]*)"$`, w.shipmentOutcomeIs)
}

// ---- Given ----

func (w *world) inventoryNotConfigured() error {
	p := inventorystorage.NewPermissiveClient()
	w.receive.Inventory, w.retry.Inventory, w.release.Inventory, w.cancel.Inventory = p, p, p, p
	return nil
}

func (w *world) cannotRevokeFor(sku string) error {
	id, ok := w.resBySKU[sku]
	if !ok {
		return fmt.Errorf("no reservation was minted for SKU %s", sku)
	}
	w.inventory.mu.Lock()
	defer w.inventory.mu.Unlock()
	if w.inventory.revokeFail == nil {
		w.inventory.revokeFail = map[string]bool{}
	}
	w.inventory.revokeFail[id] = true
	return nil
}

func (w *world) canRevokeAgain() error {
	w.inventory.mu.Lock()
	defer w.inventory.mu.Unlock()
	w.inventory.revokeFail = nil
	return nil
}

func (w *world) pathCapacity(_ string, units int, cptID string) error {
	w.rules.capacity[cptID] = units
	policy := w.rules.policy
	policy.Capacity = w.rules.capacity
	w.rules.setPromise(w, policy)
	return nil
}

func (w *world) clockAdvances(minutes int) error {
	w.clock.Advance(time.Duration(minutes) * time.Minute)
	return nil
}

func (w *world) nameOrder(name string) error {
	if w.lastOrderID == "" {
		return fmt.Errorf("no order was placed in this scenario yet")
	}
	w.rules.named[name] = w.lastOrderID
	return nil
}

func splitList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func (w *world) declarePaths(table *godog.Table) error {
	cat := &fakeCatalogue{}
	for _, r := range rows(table) {
		c := shared.ActivePathCandidate{PathId: shared.PathId(r["pathId"])}
		if m := r["cycleMinutes"]; m != "unknown" {
			n, err := strconv.Atoi(m)
			if err != nil {
				return fmt.Errorf("cycleMinutes %q: %w", m, err)
			}
			c.CycleTimeP95, c.CycleTimeKnown = time.Duration(n)*time.Minute, true
		}
		for _, s := range splitList(r["intake"]) {
			c.Intake = append(c.Intake, shared.DemandSource(s))
		}
		var maxUnits *int
		if m := r["maxUnits"]; m != "" && m != "-" {
			n, err := strconv.Atoi(m)
			if err != nil {
				return fmt.Errorf("maxUnits %q: %w", m, err)
			}
			maxUnits = &n
		}
		c.Eligibility = shared.NewEligibility(maxUnits, splitList(r["requires"]), splitList(r["excludes"]), false)
		cat.paths = append(cat.paths, c)
	}
	w.receive.Catalogue = cat
	return nil
}

func (w *world) applyClassification(eventID, sku, tags string, version int) error {
	return w.rules.classify.Execute(context.Background(), usecases.ApplyProductClassificationRequest{
		EventID: eventID,
		Record:  ports.ProductClassificationRecord{SKU: sku, HandlingTags: splitList(tags), Version: int64(version)},
	})
}

func (w *world) classifyStrict(sku, tags string, version int, eventID string) error {
	return w.applyClassification(eventID, sku, tags, version)
}

func (w *world) classifyLoose(eventID, sku, tags string, version int) error {
	w.rules.lastClassErr = w.applyClassification(eventID, sku, tags, version)
	return nil
}

func (w *world) confirmationLoopOn() error {
	w.receive.ReleaseConfirmations = w.rules.confirmations
	w.retry.ReleaseConfirmations = w.rules.confirmations
	w.release.ReleaseConfirmations = w.rules.confirmations
	return nil
}

// ---- When ----

func (w *world) retryNamed(id string) error {
	return w.do(http.MethodPost, "/orders/"+id+"/allocation-retries", nil)
}

func (w *world) placePartialWithLines(table *godog.Table) error {
	var lines []map[string]any
	for _, r := range rows(table) {
		quantity, err := strconv.Atoi(r["quantity"])
		if err != nil {
			return fmt.Errorf("quantity %q: %w", r["quantity"], err)
		}
		lines = append(lines, map[string]any{"sku": r["sku"], "quantity": quantity, "giftWrap": r["giftWrap"] == "true"})
	}
	return w.postAndCapture(map[string]any{"lines": lines, "allowPartialShipment": true})
}

func (w *world) probeReadiness() error { return w.do(http.MethodGet, "/readyz", nil) }

func (w *world) placeUnitsWithShipBy(units int, sku, shipBy string) error {
	return w.postAndCapture(map[string]any{
		"lines":          []map[string]any{{"sku": sku, "quantity": units, "giftWrap": false}},
		"requiredShipBy": shipBy,
	})
}

func (w *world) beginShutdown() error {
	w.rules.readiness.SetNotReady()
	return nil
}

func (w *world) planRefused(eventID, planID string, table *godog.Table) error {
	data := rows(table)
	if len(data) != 1 {
		return fmt.Errorf("planning event needs exactly one data row, got %d", len(data))
	}
	win, err := windowFromRow(planID, data[0])
	if err != nil {
		return err
	}
	err = w.plannedApply.Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: eventID, Window: win})
	if !errors.Is(err, order.ErrInvalidPlannedCapacity) {
		return fmt.Errorf("event %s was not refused as invalid: %v", eventID, err)
	}
	return nil
}

// ---- Then: REST responses ----

func (w *world) responseFlagIs(field, want string) error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	got, ok := obj[field].(bool)
	if !ok {
		return fmt.Errorf("response field %q is not a boolean: %v", field, obj[field])
	}
	if strconv.FormatBool(got) != want {
		return fmt.Errorf("response field %q = %v, want %s", field, got, want)
	}
	return nil
}

func (w *world) lineIs(lineNo int, sku string, quantity int, gift string) error {
	lines, err := w.lines()
	if err != nil {
		return err
	}
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("line %d does not exist (response has %d lines)", lineNo, len(lines))
	}
	line, _ := lines[lineNo-1].(map[string]any)
	gotNo, _ := line["lineNo"].(float64)
	gotQty, _ := line["quantity"].(float64)
	gotGift, _ := line["giftWrap"].(bool)
	if int(gotNo) != lineNo || line["sku"] != sku || int(gotQty) != quantity || strconv.FormatBool(gotGift) != gift {
		return fmt.Errorf("line %d = %v, want lineNo %d, SKU %s, quantity %d, gift wrap %s", lineNo, line, lineNo, sku, quantity, gift)
	}
	return nil
}

func (w *world) constraintWindows(site string) ([]any, error) {
	c, ok, err := w.constraint()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("response has no capacityConstraint: %s", w.lastBody)
	}
	if c["constrained"] != true || c["site"] != site {
		return nil, fmt.Errorf("capacityConstraint = %v, want constrained=true at site %s", c, site)
	}
	windows, _ := c["windows"].([]any)
	return windows, nil
}

func windowsContain(windows []any, planID string) bool {
	for _, raw := range windows {
		if m, _ := raw.(map[string]any); m != nil && m["planId"] == planID {
			return true
		}
	}
	return false
}

func (w *world) constrainedAtSite(site, planID string) error {
	windows, err := w.constraintWindows(site)
	if err != nil {
		return err
	}
	if !windowsContain(windows, planID) {
		return fmt.Errorf("capacityConstraint.windows = %v, want plan %q", windows, planID)
	}
	return nil
}

func (w *world) notConstrainedBy(planID string) error {
	c, ok, err := w.constraint()
	if err != nil || !ok {
		return err
	}
	windows, _ := c["windows"].([]any)
	if windowsContain(windows, planID) {
		return fmt.Errorf("order is unexpectedly constrained by plan %q: %v", planID, windows)
	}
	return nil
}

func (w *world) noPlanListedFor(site string) error {
	if err := w.requestPlannedCapacity(site); err != nil {
		return err
	}
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	if windows, _ := obj["windows"].([]any); len(windows) != 0 {
		return fmt.Errorf("site %s lists %d plans, want none: %s", site, len(windows), w.lastBody)
	}
	return nil
}

func (w *world) readReservations() []ports.ReservationRequest {
	w.inventory.mu.Lock()
	defer w.inventory.mu.Unlock()
	return append([]ports.ReservationRequest(nil), w.inventory.requests...)
}

func (w *world) reservationCountIs(want int) error {
	if got := len(w.readReservations()); got != want {
		return fmt.Errorf("inventory-storage received %d reservation requests, want %d", got, want)
	}
	return nil
}

func (w *world) reservationRequestIs(idx, quantity int, sku string, lineNo int) error {
	reqs := w.readReservations()
	if idx < 1 || idx > len(reqs) {
		return fmt.Errorf("reservation request %d does not exist (%d were made)", idx, len(reqs))
	}
	r := reqs[idx-1]
	if r.SKU.String() != sku || r.Quantity != quantity || r.LineNo != lineNo || r.DemandRef.String() != w.lastOrderID {
		return fmt.Errorf("reservation request %d = %d of %s for line %d of %s; want %d of %s for line %d of %s",
			idx, r.Quantity, r.SKU, r.LineNo, r.DemandRef, quantity, sku, lineNo, w.lastOrderID)
	}
	return nil
}

func (w *world) rememberReservations() error {
	w.rules.reserveMark = len(w.readReservations())
	return nil
}

func (w *world) noNewReservation() error {
	if got := len(w.readReservations()); got != w.rules.reserveMark {
		return fmt.Errorf("%d new reservation requests were made, want none", got-w.rules.reserveMark)
	}
	return nil
}

func (w *world) classificationAccepted() error {
	if w.rules.lastClassErr != nil {
		return fmt.Errorf("classification was refused: %w", w.rules.lastClassErr)
	}
	return nil
}

func (w *world) classificationRejected() error {
	if !errors.Is(w.rules.lastClassErr, usecases.ErrInvalidProductClassification) {
		return fmt.Errorf("classification error = %v, want ErrInvalidProductClassification", w.rules.lastClassErr)
	}
	return nil
}

// ---- Then: events ----

func (w *world) releasedLineWorkUnit(lineNo int, want string) error {
	want = strings.ReplaceAll(want, "{order}", w.lastOrderID)
	for _, ev := range w.eventsNamed("OrderLineReleased") {
		if e, ok := ev.(shared.OrderLineReleased); ok && e.LineNo == lineNo {
			if e.WorkUnitID != want {
				return fmt.Errorf("OrderLineReleased line %d work unit id = %q, want %q", lineNo, e.WorkUnitID, want)
			}
			return nil
		}
	}
	return fmt.Errorf("no OrderLineReleased event for line %d (captured: %v)", lineNo, w.eventCounts())
}

func (w *world) lastAllocated() (shared.OrderAllocated, error) {
	ev, err := w.lastEvent("OrderAllocated")
	if err != nil {
		return shared.OrderAllocated{}, err
	}
	e, ok := ev.(shared.OrderAllocated)
	if !ok {
		return shared.OrderAllocated{}, fmt.Errorf("event is %T, not OrderAllocated", ev)
	}
	return e, nil
}

func (w *world) allocatedPromiseCPT(want string) error {
	e, err := w.lastAllocated()
	if err != nil {
		return err
	}
	if e.PromiseCptId != want {
		return fmt.Errorf("OrderAllocated promise CPT = %q, want %q", e.PromiseCptId, want)
	}
	return nil
}

func (w *world) allocatedNoPromiseCPT() error { return w.allocatedPromiseCPT("") }

func (w *world) releasedLinePromiseCPT(lineNo int, want string) error {
	e, err := w.lastAllocated()
	if err != nil {
		return err
	}
	for _, l := range e.Lines {
		if l.LineNo != lineNo {
			continue
		}
		if l.PromiseCptId == nil || *l.PromiseCptId != want {
			return fmt.Errorf("released line %d promise CPT = %v, want %q", lineNo, l.PromiseCptId, want)
		}
		return nil
	}
	return fmt.Errorf("OrderAllocated did not release line %d", lineNo)
}

func (w *world) partiallyFailedAllocated(want int) error {
	ev, err := w.lastEvent("OrderAllocationPartiallyFailed")
	if err != nil {
		return err
	}
	e, _ := ev.(shared.OrderAllocationPartiallyFailed)
	if e.AllocatedLines != want {
		return fmt.Errorf("OrderAllocationPartiallyFailed allocated %d lines, want %d", e.AllocatedLines, want)
	}
	return nil
}

func (w *world) lastDemandLineDue(lineNo int, due string) error {
	ev, err := w.lastEvent("SiteSkuDemandChanged")
	if err != nil {
		return err
	}
	d, _ := ev.(shared.SiteSkuDemandChanged)
	if d.LineNo != lineNo || d.DueAt.UTC().Format(time.RFC3339) != due {
		return fmt.Errorf("SiteSkuDemandChanged = line %d due %s; want line %d due %s",
			d.LineNo, d.DueAt.UTC().Format(time.RFC3339), lineNo, due)
	}
	return nil
}

func (w *world) repromisedFromTo(from, to, reason string) error {
	ev, err := w.lastEvent("OrderRepromised")
	if err != nil {
		return err
	}
	e, _ := ev.(shared.OrderRepromised)
	if e.CptIdOld != from || e.CptIdNew != to || e.Reason != reason || e.OrderID.String() != w.lastOrderID {
		return fmt.Errorf("OrderRepromised = %s %q -> %q (%s); want %s %q -> %q (%s)",
			e.OrderID, e.CptIdOld, e.CptIdNew, e.Reason, w.lastOrderID, from, to, reason)
	}
	return nil
}

func (w *world) shippedEventLoad(name, load string) error {
	ev, err := w.lastEvent(name)
	if err != nil {
		return err
	}
	var got string
	switch e := ev.(type) {
	case shared.OrderLineShipped:
		got = e.LoadID
	case shared.OrderShipped:
		got = e.LoadID
	case shared.OrderPartiallyShipped:
		got = e.LoadID
	}
	if got != load {
		return fmt.Errorf("%s carries load %q, want %q", name, got, load)
	}
	return nil
}

func (w *world) unconfirmedEventFor(lineNo int, workUnitID string) error {
	ev, err := w.lastEvent("OrderReleaseUnconfirmed")
	if err != nil {
		return err
	}
	e, _ := ev.(shared.OrderReleaseUnconfirmed)
	want := strings.ReplaceAll(workUnitID, "{order}", w.lastOrderID)
	if e.LineNo != lineNo || e.WorkUnitID != want {
		return fmt.Errorf("OrderReleaseUnconfirmed = line %d, work unit %q; want line %d, work unit %q", e.LineNo, e.WorkUnitID, lineNo, want)
	}
	return nil
}

// ---- Consumer-driven facts ----

func (w *world) reportMissedLine(kind, eventID string, lineNo int) error {
	return w.reportMissedRef(eventID, usecases.WorkUnitID(shared.OrderId(w.lastOrderID), lineNo), kind)
}

func (w *world) reportMissedLineOf(kind, eventID string, lineNo int, orderID string) error {
	return w.reportMissedRef(eventID, usecases.WorkUnitID(shared.OrderId(orderID), lineNo), kind)
}

func (w *world) reportMissedRefStep(eventID, ref string) error {
	return w.reportMissedRef(eventID, ref)
}

// reportMissedRef mirrors RepromiseConsumer.handleFulfillmentEvent: parse the
// order_ref as a work unit id, skip what does not parse, otherwise drive
// RepromiseOrder with the event name as the reason. The default reason is
// TaskCPTMissed for the raw-order_ref form.
func (w *world) reportMissedRef(eventID, ref string, reason ...string) error {
	kind := "TaskCPTMissed"
	if len(reason) > 0 {
		kind = reason[0]
	}
	ref = strings.ReplaceAll(ref, "{order}", w.lastOrderID)
	orderID, lineNo, ok := usecases.ParseWorkUnitID(ref)
	if !ok {
		return nil
	}
	return w.rules.repromise.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: eventID, OrderId: orderID, LineNo: lineNo, Reason: kind,
	})
}

func (w *world) confirmLine(lineNo int, eventID string) error {
	return w.workUnitCreated(eventID, w.lastOrderID, lineNo)
}

func (w *world) workUnitCreated(eventID, reference string, lineNo int) error {
	reference = strings.ReplaceAll(reference, "{order}", w.lastOrderID)
	outcome, err := w.rules.confirm.Execute(context.Background(), usecases.ConfirmReleaseRequest{
		EventID: eventID, Reference: reference, LineNo: lineNo,
	})
	if err != nil {
		return err
	}
	w.rules.lastOutcome = string(outcome)
	return nil
}

func (w *world) confirmationOutcomeIs(want string) error {
	if w.rules.lastOutcome != want {
		return fmt.Errorf("confirmation outcome = %q, want %q", w.rules.lastOutcome, want)
	}
	return nil
}

func (w *world) confirmationOf(lineNo int) (releaseconfirmation.Confirmation, bool) {
	c, ok, _ := w.rules.confirmations.Find(context.Background(), shared.OrderId(w.lastOrderID), lineNo)
	return c, ok
}

func (w *world) confirmationStateIs(lineNo int, want string) error {
	c, ok := w.confirmationOf(lineNo)
	if !ok {
		return fmt.Errorf("line %d has no confirmation", lineNo)
	}
	if string(c.State()) != want {
		return fmt.Errorf("confirmation of line %d is %s, want %s", lineNo, c.State(), want)
	}
	return nil
}

func (w *world) noConfirmationFor(lineNo int) error {
	if c, ok := w.confirmationOf(lineNo); ok {
		return fmt.Errorf("line %d unexpectedly has a %s confirmation", lineNo, c.State())
	}
	return nil
}

func (w *world) confirmationKeepsFlagTime(lineNo int) error {
	c, ok := w.confirmationOf(lineNo)
	if !ok {
		return fmt.Errorf("line %d has no confirmation", lineNo)
	}
	if c.UnconfirmedAt() == nil || c.ConfirmedAt() == nil {
		return fmt.Errorf("line %d lost its flag or confirmation time: unconfirmed %v, confirmed %v", lineNo, c.UnconfirmedAt(), c.ConfirmedAt())
	}
	return nil
}

func (w *world) sweepRuns(minutes int) error {
	w.rules.flag.Timeout = time.Duration(minutes) * time.Minute
	n, err := w.rules.flag.Execute(context.Background())
	w.rules.lastFlagged = n
	return err
}

func (w *world) sweepFlagged(want int) error {
	if w.rules.lastFlagged != want {
		return fmt.Errorf("the sweep flagged %d lines, want %d", w.rules.lastFlagged, want)
	}
	return nil
}

func (w *world) resolveOrders(names string) []shared.OrderId {
	var ids []shared.OrderId
	for _, name := range splitList(names) {
		if id, ok := w.rules.named[name]; ok {
			name = id
		}
		ids = append(ids, shared.OrderId(name))
	}
	return ids
}

func (w *world) shipmentConfirmed(loadID, eventID, names string) error {
	ids := w.resolveOrders(names)
	outcomes, err := w.rules.shipment.Execute(context.Background(), usecases.ConfirmShipmentRequest{
		EventID: eventID, LoadID: loadID, OrderIDs: ids,
	})
	if err != nil {
		return err
	}
	w.rules.shipOutcomes = outcomes
	return nil
}

func (w *world) shipmentConfirmedEmpty(loadID, eventID string) error {
	return w.shipmentConfirmed(loadID, eventID, "")
}

func (w *world) shipmentOutcomeIs(name, want string) error {
	id := shared.OrderId(name)
	if named, ok := w.rules.named[name]; ok {
		id = shared.OrderId(named)
	}
	if name == "no orders" {
		id = ""
	}
	got, ok := w.rules.shipOutcomes[id]
	if !ok {
		return fmt.Errorf("no shipment outcome for %q (have %v)", name, w.rules.shipOutcomes)
	}
	if string(got) != want {
		return fmt.Errorf("shipment outcome for %q = %q, want %q", name, got, want)
	}
	return nil
}
