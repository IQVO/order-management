// Package main_test contains the BDD / acceptance test suite: godog
// (Cucumber for Go) drives the Gherkin scenarios under features/ against
// the real chi router over HTTP, wired to the same in-memory adapters the
// service's own httptest suite uses. It is a black-box test — it only ever
// touches the REST API.
package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// fixedNow is the clock every scenario runs against, so the suite is
// deterministic (the promise-date assertion leans on it implicitly: the
// policy below promises 24h out from this instant).
var fixedNow = time.Date(2026, 9, 7, 9, 0, 0, 0, time.UTC)

// inventoryMode is one scripted behaviour of the fake inventory-storage
// client for a SKU.
type inventoryMode int

const (
	inventoryOK inventoryMode = iota
	inventoryInsufficient
	inventoryUnreachable
)

// fakeInventory is a scripted ports.InventoryReservationClient: per-SKU
// modes (defaulting to OK), plus a record of every revoked reservation so
// BR6's revoke-on-cancel is observable. No scenario reaches a network.
type fakeInventory struct {
	mu      sync.Mutex
	modes   map[string]inventoryMode
	nextID  int
	revoked map[string]string // reservation id -> sku
}

func newFakeInventory() *fakeInventory {
	return &fakeInventory{modes: map[string]inventoryMode{}, revoked: map[string]string{}}
}

func (f *fakeInventory) setMode(sku string, m inventoryMode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.modes[sku] = m
}

func (f *fakeInventory) Reserve(_ context.Context, req ports.ReservationRequest) (ports.ReservationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	mode, ok := f.modes["*"]
	if !ok {
		// No transport-layer script: fall back to the SKU's own mode,
		// defaulting to OK.
		mode, ok = f.modes[req.SKU.String()]
		if !ok {
			mode = inventoryOK
		}
	}

	switch mode {
	case inventoryInsufficient:
		// The business fact behind inventory-storage's HTTP 409.
		return ports.ReservationResult{}, ports.ErrInsufficientStock
	case inventoryUnreachable:
		// A transport/5xx failure: NOT a business fact, never a backorder.
		return ports.ReservationResult{}, fmt.Errorf("inventory-storage: connection refused")
	default:
		f.nextID++
		return ports.ReservationResult{ReservationID: fmt.Sprintf("res-bdd-%d", f.nextID)}, nil
	}
}

func (f *fakeInventory) RevokeReservation(_ context.Context, reservationID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Record it so a scenario can assert WHICH reservations a
	// cancellation actually revoked.
	f.revoked[reservationID] = ""
	return nil
}

// capturePublisher records every published event in order, so scenarios
// could assert on integration facts (kept minimal here: presence only).
type capturePublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

func (p *capturePublisher) Publish(_ context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

// world is the per-scenario state: one server with its own in-memory
// adapters and scripted inventory, plus the last HTTP response.
type world struct {
	server    *httptest.Server
	inventory *fakeInventory
	publisher *capturePublisher

	lastOrderID string
	// resBySKU remembers each placement's reservation id per SKU, so a
	// post-cancel assertion works even though a 204 body is empty.
	resBySKU map[string]string
	// lastLocation records the Location response header, so a scenario
	// can assert where a created order advertises itself.
	lastLocation string
	// eventSnapshot is the per-name event count captured by the
	// "published events are remembered" step, so a later "no new event"
	// assertion compares against it rather than against zero.
	eventSnapshot map[string]int
	lastStatus    int
	lastBody      []byte
}

func (w *world) reset() {
	if w.server != nil {
		w.server.Close()
	}

	orders := memory.NewOrderRepo()
	w.inventory = newFakeInventory()
	w.publisher = &capturePublisher{}
	clock := memory.NewFixedClock(fixedNow)
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}

	server := &inboundhttp.Server{
		ReceiveOrder:    &usecases.ReceiveOrder{Orders: orders, Events: w.publisher, Clock: clock, Inventory: w.inventory, Promise: promise},
		RetryAllocation: &usecases.RetryAllocation{Orders: orders, Inventory: w.inventory, Events: w.publisher, Clock: clock, Promise: promise},
		ReleaseHeld:     &usecases.ReleaseHeldOrder{Orders: orders, Inventory: w.inventory, Events: w.publisher, Clock: clock, Promise: promise},
		CancelOrder:     &usecases.CancelOrder{Orders: orders, Inventory: w.inventory, Events: w.publisher, Clock: clock},
		GetOrder:        &usecases.GetOrder{Orders: orders},
	}

	// A discard logger keeps the middleware on the code path (so it is
	// exercised) without flooding the pretty-format scenario output.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	w.server = httptest.NewServer(inboundhttp.NewRouter(server, logger, ""))
	w.lastOrderID = ""
	w.resBySKU = map[string]string{}
	w.lastLocation = ""
	w.eventSnapshot = nil
	w.lastStatus = 0
	w.lastBody = nil
}

func (w *world) close() {
	if w.server != nil {
		w.server.Close()
		w.server = nil
	}
}

// do performs a real net/http call against the httptest server and records
// the response as the "last" one for the assertion steps.
func (w *world) do(method, path string, body any) error {
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(context.Background(), method, w.server.URL+path, reader)
	if err != nil {
		return fmt.Errorf("build %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.server.Client().Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s %s response: %w", method, path, err)
	}

	w.lastStatus = resp.StatusCode
	w.lastBody = raw
	w.lastLocation = resp.Header.Get("Location")
	return nil
}

// decodeLast unmarshals the last response body into a generic JSON object.
func (w *world) decodeLast() (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal(w.lastBody, &out); err != nil {
		return nil, fmt.Errorf("decode response %q: %w", string(w.lastBody), err)
	}
	return out, nil
}

func (w *world) stringField(field string) (string, error) {
	obj, err := w.decodeLast()
	if err != nil {
		return "", err
	}
	value, ok := obj[field].(string)
	if !ok {
		return "", fmt.Errorf("response field %q is not a string: %v", field, obj[field])
	}
	return value, nil
}

// lines returns the last response's lines array.
func (w *world) lines() ([]any, error) {
	obj, err := w.decodeLast()
	if err != nil {
		return nil, err
	}
	raw, ok := obj["lines"].([]any)
	if !ok {
		return nil, fmt.Errorf("response has no lines array: %v", obj)
	}
	return raw, nil
}

// placeOrder POSTs an order for a comma-separated SKU list, quantity 1
// per line, and remembers the created order's id for the When steps.
func (w *world) placeOrder(skus string, allowPartial bool) error {
	skuList := splitCSV(skus)
	lines := make([]map[string]any, 0, len(skuList))
	for _, sku := range skuList {
		lines = append(lines, map[string]any{"sku": sku, "quantity": 1, "giftWrap": false})
	}
	if err := w.do(http.MethodPost, "/orders", map[string]any{
		"lines":                lines,
		"allowPartialShipment": allowPartial,
	}); err != nil {
		return err
	}
	return w.capturePlacedOrder()
}

// capturePlacedOrder records the just-created order's id and each
// line's reservation id, so later steps can address the order and
// assert which reservations a cancellation revoked.
func (w *world) capturePlacedOrder() error {
	id, err := w.stringField("id")
	if err != nil {
		return fmt.Errorf("place order: %w", err)
	}
	w.lastOrderID = id

	// Remember the reservation id minted for each line's SKU so the
	// revoke assertion works after a body-less 204.
	linesResp, err := w.lines()
	if err != nil {
		return fmt.Errorf("place order: %w", err)
	}
	for _, raw := range linesResp {
		line, _ := raw.(map[string]any)
		if line == nil {
			continue
		}
		sku, _ := line["sku"].(string)
		resID, _ := line["reservationId"].(string)
		if sku != "" && resID != "" {
			w.resBySKU[sku] = resID
		}
	}
	return nil
}

// tryPlaceOrder POSTs an order with an explicit releaseOnAllocation
// flag and records the created order ONLY when intake is accepted —
// the intake-rejection scenarios assert the problem response via the
// Then steps instead of failing here.
func (w *world) tryPlaceOrder(skus string, allowPartial, hold bool) error {
	skuList := splitCSV(skus)
	lines := make([]map[string]any, 0, len(skuList))
	for _, sku := range skuList {
		lines = append(lines, map[string]any{"sku": sku, "quantity": 1, "giftWrap": false})
	}
	if err := w.do(http.MethodPost, "/orders", map[string]any{
		"lines":                lines,
		"allowPartialShipment": allowPartial,
		"releaseOnAllocation":  !hold,
	}); err != nil {
		return err
	}
	if w.lastStatus != http.StatusCreated {
		return nil
	}
	return w.capturePlacedOrder()
}

// postInvalidOrder POSTs a deliberately invalid intake body: the
// rejection is the scenario's subject, so nothing is captured.
func (w *world) postInvalidOrder(lines []map[string]any) error {
	return w.do(http.MethodPost, "/orders", map[string]any{"lines": lines})
}

func splitCSV(s string) []string {
	out := []string{}
	current := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, current)
			current = ""
			continue
		}
		current += string(r)
	}
	if current != "" {
		out = append(out, current)
	}
	return out
}

// ---- Given ----

func (w *world) serviceIsRunning() error {
	return nil // reset() already wired a fresh server before the scenario.
}

func (w *world) stockFor(sku string) error {
	w.inventory.setMode(sku, inventoryOK)
	return nil
}

func (w *world) insufficientFor(sku string) error {
	w.inventory.setMode(sku, inventoryInsufficient)
	return nil
}

func (w *world) unreachable() error {
	// Route every SKU (including any used later in the scenario) at the
	// transport layer.
	w.inventory.setMode("*", inventoryUnreachable)
	return nil
}

func (w *world) orderPlaced(skus string) error { return w.placeOrder(skus, false) }

func (w *world) partialOrderPlaced(skus string) error { return w.placeOrder(skus, true) }

func (w *world) heldOrderPlaced(skus string) error { return w.tryPlaceOrder(skus, false, true) }

func (w *world) heldPartialOrderPlaced(skus string) error { return w.tryPlaceOrder(skus, true, true) }

func (w *world) orderPlacedWithEmptySKU() error {
	return w.postInvalidOrder([]map[string]any{{"sku": "", "quantity": 1, "giftWrap": false}})
}

func (w *world) orderPlacedWithQuantity(quantity int) error {
	return w.postInvalidOrder([]map[string]any{{"sku": "SKU-BOOK-0001", "quantity": quantity, "giftWrap": false}})
}

func (w *world) orderPlacedWithNoLines() error {
	return w.postInvalidOrder([]map[string]any{})
}

// ---- When ----

func (w *world) retryAllocation() error {
	if w.lastOrderID == "" {
		return fmt.Errorf("no order placed in this scenario yet")
	}
	return w.do(http.MethodPost, "/orders/"+w.lastOrderID+"/retry-allocation", nil)
}

func (w *world) cancelOrder() error {
	if w.lastOrderID == "" {
		return fmt.Errorf("no order placed in this scenario yet")
	}
	return w.do(http.MethodDelete, "/orders/"+w.lastOrderID, nil)
}

func (w *world) releaseOrder() error {
	if w.lastOrderID == "" {
		return fmt.Errorf("no order placed in this scenario yet")
	}
	return w.do(http.MethodPost, "/orders/"+w.lastOrderID+"/release", nil)
}

func (w *world) fetchPlacedOrder() error {
	if w.lastOrderID == "" {
		return fmt.Errorf("no order placed in this scenario yet")
	}
	return w.do(http.MethodGet, "/orders/"+w.lastOrderID, nil)
}

func (w *world) fetchOrder(id string) error {
	return w.do(http.MethodGet, "/orders/"+id, nil)
}

func (w *world) probeLiveness() error {
	return w.do(http.MethodGet, "/healthz", nil)
}

// ---- Then ----

func (w *world) statusIs(want int) error {
	if w.lastStatus == 0 {
		return fmt.Errorf("no request made in this scenario yet")
	}
	// 201/200/204 are all "accepted"; the feature files state the exact
	// code, so compare directly.
	if w.lastStatus != want {
		body := string(w.lastBody)
		if len(body) > 200 {
			body = body[:200]
		}
		return fmt.Errorf("got status %d, want %d (body: %s)", w.lastStatus, want, body)
	}
	return nil
}

func (w *world) orderStatusIs(want string) error {
	got, err := w.stringField("status")
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("order status = %q, want %q", got, want)
	}
	return nil
}

func (w *world) lineStatusIs(lineNo int, want string) error {
	lines, err := w.lines()
	if err != nil {
		return err
	}
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("line %d does not exist (response has %d lines)", lineNo, len(lines))
	}
	got, ok := lines[lineNo-1].(map[string]any)["status"].(string)
	if !ok {
		return fmt.Errorf("line %d has no string status: %v", lineNo, lines[lineNo-1])
	}
	if got != want {
		return fmt.Errorf("line %d status = %q, want %q", lineNo, got, want)
	}
	return nil
}

func (w *world) hasPromiseDate() error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	if _, ok := obj["promiseDate"].(string); !ok {
		return fmt.Errorf("order has no promiseDate: %v", obj)
	}
	return nil
}

func (w *world) problemTitleIs(want string) error {
	got, err := w.stringField("title")
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("problem title = %q, want %q", got, want)
	}
	return nil
}

func (w *world) reservationWasRevokedFor(sku string) error {
	// Uses the reservation id captured at placement time (the cancel
	// response is a body-less 204), then asserts the cancellation actually
	// revoked it upstream.
	resID, ok := w.resBySKU[sku]
	if !ok {
		return fmt.Errorf("no reservation was minted for SKU %s at placement", sku)
	}

	w.inventory.mu.Lock()
	_, revoked := w.inventory.revoked[resID]
	w.inventory.mu.Unlock()
	if !revoked {
		return fmt.Errorf("reservation %s (%s) was not revoked", resID, sku)
	}
	return nil
}

func (w *world) responseFieldIs(field, want string) error {
	got, err := w.stringField(field)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("response field %q = %q, want %q", field, got, want)
	}
	return nil
}

func (w *world) linePathIs(lineNo int, want string) error {
	lines, err := w.lines()
	if err != nil {
		return err
	}
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("line %d does not exist (response has %d lines)", lineNo, len(lines))
	}
	got, ok := lines[lineNo-1].(map[string]any)["pathId"].(string)
	if !ok {
		return fmt.Errorf("line %d has no string pathId: %v", lineNo, lines[lineNo-1])
	}
	if got != want {
		return fmt.Errorf("line %d process path = %q, want %q", lineNo, got, want)
	}
	return nil
}

func (w *world) locationHeaderPointsToCreatedOrder() error {
	want := "/orders/" + w.lastOrderID
	if w.lastLocation != want {
		return fmt.Errorf("Location header = %q, want %q", w.lastLocation, want)
	}
	return nil
}

// eventCounts tallies the captured events by name.
func (w *world) eventCounts() map[string]int {
	w.publisher.mu.Lock()
	defer w.publisher.mu.Unlock()
	counts := map[string]int{}
	for _, event := range w.publisher.events {
		counts[event.EventName()]++
	}
	return counts
}

func (w *world) rememberEvents() error {
	w.eventSnapshot = w.eventCounts()
	return nil
}

func (w *world) eventWasPublished(name string) error {
	if w.eventCounts()[name] == 0 {
		return fmt.Errorf("no %s event was published (captured: %v)", name, w.eventCounts())
	}
	return nil
}

func (w *world) noEventWasPublished(name string) error {
	if n := w.eventCounts()[name]; n > 0 {
		return fmt.Errorf("%d %s events were published, want none", n, name)
	}
	return nil
}

func (w *world) noNewEventWasPublished(name string) error {
	if w.eventSnapshot == nil {
		return fmt.Errorf("no event snapshot was taken in this scenario yet")
	}
	before, after := w.eventSnapshot[name], w.eventCounts()[name]
	if after != before {
		return fmt.Errorf("%d new %s events were published (had %d), want none", after-before, name, before)
	}
	return nil
}

// InitializeScenario registers every step definition and gives each
// scenario a fresh server over fresh in-memory adapters, so scenarios are
// independent.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.reset()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.close()
		return ctx, nil
	})

	// Given / When (placement steps serve both positions)
	sc.Step(`^the Order Management service is running$`, w.serviceIsRunning)
	sc.Step(`^inventory-storage has usable stock for SKU "([^"]*)"$`, w.stockFor)
	sc.Step(`^inventory-storage reports insufficient usable stock for SKU "([^"]*)"$`, w.insufficientFor)
	sc.Step(`^inventory-storage is unreachable$`, w.unreachable)
	sc.Step(`^an order is placed for SKUs "([^"]*)"$`, w.orderPlaced)
	sc.Step(`^an order allowing partial shipment is placed for SKUs "([^"]*)"$`, w.partialOrderPlaced)
	sc.Step(`^an order is placed on hold for SKUs "([^"]*)"$`, w.heldOrderPlaced)
	sc.Step(`^an order is placed on hold allowing partial shipment for SKUs "([^"]*)"$`, w.heldPartialOrderPlaced)
	sc.Step(`^an order is placed with an empty SKU$`, w.orderPlacedWithEmptySKU)
	sc.Step(`^an order is placed with quantity (-?\d+)$`, w.orderPlacedWithQuantity)
	sc.Step(`^an order is placed with no lines$`, w.orderPlacedWithNoLines)
	sc.Step(`^the order is retried for allocation$`, w.retryAllocation)
	sc.Step(`^the order is cancelled$`, w.cancelOrder)
	sc.Step(`^the order is released$`, w.releaseOrder)
	sc.Step(`^the order is fetched$`, w.fetchPlacedOrder)
	sc.Step(`^the order "([^"]*)" is fetched$`, w.fetchOrder)
	sc.Step(`^the service is probed for liveness$`, w.probeLiveness)
	sc.Step(`^the published events are remembered$`, w.rememberEvents)

	// Then
	sc.Step(`^the request is accepted with status (\d+)$`, w.statusIs)
	sc.Step(`^the request is rejected with status (\d+)$`, w.statusIs)
	sc.Step(`^the order status is "([^"]*)"$`, w.orderStatusIs)
	sc.Step(`^line (\d+) status is "([^"]*)"$`, w.lineStatusIs)
	sc.Step(`^line (\d+) is on process path "([^"]*)"$`, w.linePathIs)
	sc.Step(`^the order has a promise date$`, w.hasPromiseDate)
	sc.Step(`^the response field "([^"]*)" is "([^"]*)"$`, w.responseFieldIs)
	sc.Step(`^the Location header points to the created order$`, w.locationHeaderPointsToCreatedOrder)
	sc.Step(`^the problem detail title is "([^"]*)"$`, w.problemTitleIs)
	sc.Step(`^the reservation for SKU "([^"]*)" was revoked$`, w.reservationWasRevokedFor)
	sc.Step(`^an "([^"]*)" event was published$`, w.eventWasPublished)
	sc.Step(`^no "([^"]*)" event was published$`, w.noEventWasPublished)
	sc.Step(`^no new "([^"]*)" event was published$`, w.noNewEventWasPublished)
}

// TestFeatures runs the Gherkin acceptance suite under features/.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format: "pretty",
			Paths:  []string{"features"},
			// Strict makes an undefined or pending step fail the suite instead
			// of silently skipping it.
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}
