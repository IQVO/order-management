package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Fixtures follow outbound-shipping's merged producer contract
// (apis/asyncapi.yaml on its develop, pinned by ADR 0051): full
// CloudEvents types
// com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed and
// com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad,
// with the REAL payloads' full field sets (including the members this
// consumer deliberately ignores) so the fixed-struct decode is exercised
// exactly as the producer emits it.
const (
	osTypeShipmentConfirmed = "com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed"
	osTypePackageMissedLoad = "com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad"
	osTypeBolIssued         = "com.warehouse.wms.outbound-shipping.billoflading.BillOfLadingIssued"
	osEventID               = "22222222-2222-4222-8222-222222222222"
	osMissedEventID         = "33333333-3333-4333-8333-333333333333"
	osOrderID               = shared.OrderId("ord-os-1")
	osUnknownOrderID        = shared.OrderId("ord-os-unknown")
	osLoadID                = "load-os-77"
)

var osDepartedAt = time.Date(2026, 11, 6, 17, 5, 0, 0, time.UTC)

func osMsg(t *testing.T, offset int64, ceType, id, subject, dataschema string, data any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/outbound-shipping")
	e.SetType(ceType)
	e.SetSubject(subject)
	e.SetTime(osDepartedAt)
	e.SetDataSchema(dataschema)
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Offset: offset, Key: []byte(subject), Value: b}
}

// osShipmentData is the REAL ShipmentConfirmed payload shape: every
// required member of the producer's contract, so the decode runs against
// the full wire form, not a reduced stub.
func osShipmentData(orderIDs ...string) map[string]any {
	orders := make([]map[string]any, 0, len(orderIDs))
	for _, id := range orderIDs {
		orders = append(orders, map[string]any{"order_id": id})
	}
	return map[string]any{
		"load_id":     osLoadID,
		"site_id":     "SITE1",
		"cpt_id":      "sp1-1800",
		"carrier":     "CARRIER-X",
		"trailer_id":  "trailer-9",
		"seal_number": "seal-4",
		"departed_at": "2026-11-06T17:05:00Z",
		"orders":      orders,
		"version":     3,
	}
}

// osMissedLoadData is the REAL PackageMissedLoad payload shape.
func osMissedLoadData(orderID string) map[string]any {
	return map[string]any{
		"package_id":      "pkg-os-1",
		"load_id":         osLoadID,
		"site_id":         "SITE1",
		"source_order_id": orderID,
		"cutoff":          "2026-11-06T16:00:00Z",
		"detected_at":     "2026-11-06T16:05:00Z",
		"version":         2,
	}
}

// osPublisher records published domain events.
type osPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

func (p *osPublisher) Publish(_ context.Context, e shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *osPublisher) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.events))
	for _, e := range p.events {
		out = append(out, e.EventName())
	}
	return out
}

// osShipClock is a fixed clock so the raised events are deterministic.
type osShipClock struct{}

func (osShipClock) Now() time.Time { return osDepartedAt }

// kosFixture wires the consumer against REAL use cases over the
// in-memory order repo: one released two-line order (ord-os-1) plus one
// pending order (ord-os-2), so dispatch/decode/idempotency are exercised
// through the whole use-case path, not a stub.
type kosFixture struct {
	orders   *memory.OrderRepo
	gate     *memory.ShipmentProcessedEventsRepo
	events   *osPublisher
	reader   *scriptedReader
	dlq      *captureDLQ
	consumer *OutboundShippingConsumer
}

func newKOSFixture(t *testing.T) *kosFixture {
	t.Helper()
	orders := memory.NewOrderRepo()
	gate := memory.NewShipmentProcessedEventsRepo()
	events := &osPublisher{}
	reader := &scriptedReader{}
	dlq := &captureDLQ{}

	released := newOSOrder(t, "ord-os-1", true)
	mustOSShipAllocate(t, released, 1)
	mustOSShipAllocate(t, released, 2)
	mustOSRelease(t, released, 1)
	mustOSRelease(t, released, 2)
	setOSPromiseGroups(t, released)
	if err := orders.Save(context.Background(), released); err != nil {
		t.Fatalf("save released: %v", err)
	}

	pending := newOSOrder(t, "ord-os-2", true)
	if err := orders.Save(context.Background(), pending); err != nil {
		t.Fatalf("save pending: %v", err)
	}

	confirm := &usecases.ConfirmShipment{
		Orders: orders, Events: events, Clock: osShipClock{}, Processed: gate,
	}
	repromise := &usecases.RepromiseOrder{
		Orders: orders, Promise: osMissedPromisePolicy(), Events: events,
		Clock: osShipClock{}, Processed: gate,
	}
	return &kosFixture{
		orders: orders, gate: gate, events: events, reader: reader, dlq: dlq,
		consumer: &OutboundShippingConsumer{reader: reader, dlqWriter: dlq, confirm: confirm, repromise: repromise},
	}
}

// osMissedPromisePolicy is a fallback-only policy with a DIFFERENT lead
// time than the fixture order's stored groups, so a missed-load
// re-promise genuinely moves the promise and publishes OrderRepromised.
func osMissedPromisePolicy() order.PromisePolicy {
	return order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
}

func newOSOrder(t *testing.T, id string, allowPartial bool) *order.Order {
	t.Helper()
	l1, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := order.NewOrderLine(2, "SKU-2", 2, "pick", false)
	if err != nil {
		t.Fatal(err)
	}
	o, err := order.New(shared.OrderId(id), []*order.OrderLine{l1, l2}, allowPartial)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func mustOSShipAllocate(t *testing.T, o *order.Order, lineNo int) {
	t.Helper()
	if err := o.Allocate(lineNo, "res-os"); err != nil {
		t.Fatalf("Allocate(%d): %v", lineNo, err)
	}
}

func mustOSRelease(t *testing.T, o *order.Order, lineNo int) {
	t.Helper()
	if err := o.Release(lineNo); err != nil {
		t.Fatalf("Release(%d): %v", lineNo, err)
	}
}

// setOSPromiseGroups stamps the groups a real allocation would have
// persisted: one group over both lines with a 24h-lead-time promise —
// deliberately different from osMissedPromisePolicy's 6h, so the
// missed-load recompute has something to move.
func setOSPromiseGroups(t *testing.T, o *order.Order) {
	t.Helper()
	cutoff := osDepartedAt.Add(24 * time.Hour)
	o.SetPromiseGroups([]order.PromiseGroup{{
		LineNos: []int{1, 2},
		Promise: order.Promise{CutoffAt: cutoff, Basis: order.BasisLeadTime},
	}})
}

func (f *kosFixture) handle(t *testing.T, msg kafkago.Message) error {
	t.Helper()
	return f.consumer.handleMessage(context.Background(), msg)
}

func (f *kosFixture) committed() []int64 {
	c, _, _ := f.reader.snapshot()
	return c
}

func (f *kosFixture) orderStatus(t *testing.T, id shared.OrderId) order.Status {
	t.Helper()
	o, err := f.orders.FindByID(context.Background(), id)
	if err != nil || o == nil {
		t.Fatalf("FindByID(%s): %v %v", id, o, err)
	}
	return o.Status()
}

func TestOutboundShippingConsumer_ShipmentConfirmedShipsTheOrder(t *testing.T) {
	f := newKOSFixture(t)
	err := f.handle(t, osMsg(t, 11, osTypeShipmentConfirmed, osEventID, osLoadID,
		"urn:warehouse:outbound-shipping:events:ShipmentConfirmed:v1", osShipmentData("ord-os-1")))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if got := f.orderStatus(t, "ord-os-1"); got != order.StatusShipped {
		t.Fatalf("status = %q, want Shipped", got)
	}
	if got := f.committed(); len(got) != 1 || got[0] != 11 {
		t.Fatalf("committed = %v, want [11]", got)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("dead-lettered %d messages, want 0", f.dlq.count())
	}
	for _, want := range []string{"OrderLineShipped", "OrderShipped"} {
		if !contains(f.events.names(), want) {
			t.Fatalf("published events %v, want %s among them", f.events.names(), want)
		}
	}
}

func TestOutboundShippingConsumer_DuplicateDeliveryChangesNothing(t *testing.T) {
	f := newKOSFixture(t)
	msg := osMsg(t, 11, osTypeShipmentConfirmed, osEventID, osLoadID,
		"urn:warehouse:outbound-shipping:events:ShipmentConfirmed:v1", osShipmentData("ord-os-1"))
	if err := f.handle(t, msg); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	first := f.events.names()
	if err := f.handle(t, msg); err != nil {
		t.Fatalf("re-delivery: %v", err)
	}
	if got := f.orderStatus(t, "ord-os-1"); got != order.StatusShipped {
		t.Fatalf("status after re-delivery = %q, want Shipped", got)
	}
	if len(f.events.names()) != len(first) {
		t.Fatalf("re-delivery published %v, want no new events beyond %v", f.events.names(), first)
	}
	if got := f.committed(); len(got) != 2 {
		t.Fatalf("committed = %v, want both deliveries committed", got)
	}
}

func TestOutboundShippingConsumer_UnknownOrderIsSkippedNotDeadLettered(t *testing.T) {
	f := newKOSFixture(t)
	err := f.handle(t, osMsg(t, 12, osTypeShipmentConfirmed, osEventID, osLoadID,
		"urn:warehouse:outbound-shipping:events:ShipmentConfirmed:v1", osShipmentData(osUnknownOrderID.String())))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("dead-lettered %d messages, want 0 (skip-and-log, ADR 0051)", f.dlq.count())
	}
	if got := f.committed(); len(got) != 1 {
		t.Fatalf("committed = %v, want the offset committed past", got)
	}
	if len(f.events.names()) != 0 {
		t.Fatalf("published %v, want nothing for an unknown order", f.events.names())
	}
}

func TestOutboundShippingConsumer_NotShippableOrderIsSkippedNotDeadLettered(t *testing.T) {
	f := newKOSFixture(t)
	err := f.handle(t, osMsg(t, 13, osTypeShipmentConfirmed, osEventID, osLoadID,
		"urn:warehouse:outbound-shipping:events:ShipmentConfirmed:v1", osShipmentData("ord-os-2")))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if got := f.orderStatus(t, "ord-os-2"); got != order.StatusReceived {
		t.Fatalf("status = %q, want Received (nothing transitioned)", got)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("dead-lettered %d messages, want 0", f.dlq.count())
	}
	if len(f.events.names()) != 0 {
		t.Fatalf("published %v, want nothing for a not-shippable order", f.events.names())
	}
}

func TestOutboundShippingConsumer_UnknownEventTypeIsIgnored(t *testing.T) {
	f := newKOSFixture(t)
	err := f.handle(t, osMsg(t, 14, osTypeBolIssued, osEventID, "bol-1",
		"urn:warehouse:outbound-shipping:events:BillOfLadingIssued:v1",
		map[string]any{"bol_number": "BOL-1", "load_id": osLoadID, "package_count": 2}))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if got := f.committed(); len(got) != 1 {
		t.Fatalf("committed = %v, want the unknown type committed past", got)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("dead-lettered %d messages, want 0 (forward compatibility)", f.dlq.count())
	}
	if len(f.events.names()) != 0 {
		t.Fatalf("published %v, want nothing", f.events.names())
	}
}

func TestOutboundShippingConsumer_PackageMissedLoadRepromisesTheOrder(t *testing.T) {
	f := newKOSFixture(t)
	err := f.handle(t, osMsg(t, 15, osTypePackageMissedLoad, osMissedEventID, "pkg-os-1",
		"urn:warehouse:outbound-shipping:events:PackageMissedLoad:v1", osMissedLoadData("ord-os-1")))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if !contains(f.events.names(), "OrderRepromised") {
		t.Fatalf("published %v, want OrderRepromised", f.events.names())
	}
	if got := f.committed(); len(got) != 1 {
		t.Fatalf("committed = %v, want [15]", got)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("dead-lettered %d messages, want 0", f.dlq.count())
	}
	// The re-promise must have moved the stored cutoff to the 6h policy.
	o, err := f.orders.FindByID(context.Background(), osOrderID)
	if err != nil || o == nil {
		t.Fatalf("FindByID: %v %v", o, err)
	}
	groups := o.PromiseGroups()
	if len(groups) != 1 {
		t.Fatalf("PromiseGroups = %d, want 1", len(groups))
	}
	want := osDepartedAt.Add(6 * time.Hour)
	if !groups[0].Promise.CutoffAt.Equal(want) {
		t.Fatalf("cutoff = %v, want %v", groups[0].Promise.CutoffAt, want)
	}
}

func TestOutboundShippingConsumer_MissedLoadWithoutSourceOrderIsIgnored(t *testing.T) {
	f := newKOSFixture(t)
	data := osMissedLoadData("")
	err := f.handle(t, osMsg(t, 16, osTypePackageMissedLoad, osMissedEventID, "pkg-os-2",
		"urn:warehouse:outbound-shipping:events:PackageMissedLoad:v1", data))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if got := f.committed(); len(got) != 1 {
		t.Fatalf("committed = %v, want committed past", got)
	}
	if f.dlq.count() != 0 || len(f.events.names()) != 0 {
		t.Fatalf("dlq=%d events=%v, want nothing", f.dlq.count(), f.events.names())
	}
}

func TestOutboundShippingConsumer_NonCloudEventsMessageIsDeadLettered(t *testing.T) {
	f := newKOSFixture(t)
	if err := f.handle(t, poison(17)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("dead-lettered %d messages, want 1", f.dlq.count())
	}
	if got := f.committed(); len(got) != 1 {
		t.Fatalf("committed = %v, want the poison message committed past", got)
	}
}

func TestOutboundShippingConsumer_UndecodableDataIsDeadLettered(t *testing.T) {
	f := newKOSFixture(t)
	// A ShipmentConfirmed whose data is not the promised object shape:
	// orders as a bare string list can never decode into the payload
	// struct — deterministic poison, dead-letter it.
	bad := osMsg(t, 18, osTypeShipmentConfirmed, osEventID, osLoadID,
		"urn:warehouse:outbound-shipping:events:ShipmentConfirmed:v1", map[string]any{
			"load_id": osLoadID, "orders": "ord-os-1",
		})
	if err := f.handle(t, bad); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("dead-lettered %d messages, want 1", f.dlq.count())
	}
	if got := f.committed(); len(got) != 1 {
		t.Fatalf("committed = %v, want the poison message committed past", got)
	}
	if got := f.orderStatus(t, "ord-os-1"); got != order.StatusReleased {
		t.Fatalf("status = %q, want Released (nothing transitioned)", got)
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
