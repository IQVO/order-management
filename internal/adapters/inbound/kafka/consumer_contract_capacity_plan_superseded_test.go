package kafka

// Consumer contract tests (E1-E4) for CapacityPlanSuperseded (ADR 0052),
// written against the PINNED copy of warehouse-planning's own
// apis/asyncapi.yaml (testdata/warehouse-planning.asyncapi.yaml, registered
// in contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/warehouse-planning@develop). The golden fixture is the
// producer spec's OWN CapacityPlanSuperseded example, not a local one.
// warehouse-planning's file is channel-perspective: its own topic sits
// under 'subscribe:'.

import (
	"context"
	"testing"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/domain/order"
)

const supersededMessage = "CapacityPlanSuperseded"

// supersededGolden returns the producer's own CapacityPlanSuperseded example.
func supersededGolden(t *testing.T) pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedWarehousePlanSpec)
	return doc.goldenMessage(t, supersededMessage).Examples[0]
}

// E1: the producer documents CapacityPlanSuperseded on the topic this
// repo's reader subscribes to, under the exact type string the consumer
// dispatches on.
func TestPinnedAsyncAPICoversSubscriptions_CapacityPlanSuperseded(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWarehousePlanSpec)
	names := doc.channelMessageRefs(t, PlanningEventsTopic, "subscribe")
	documented := map[string]bool{}
	for _, name := range names {
		documented[doc.documentedType(t, doc.message(t, name))] = true
	}
	if !documented[ceTypeCapacityPlanSuperseded] {
		t.Errorf("pinned warehouse-planning spec does not document %s on %s (documented: %v)",
			ceTypeCapacityPlanSuperseded, PlanningEventsTopic, documented)
	}
	if got := doc.documentedType(t, doc.message(t, supersededMessage)); got != ceTypeCapacityPlanSuperseded {
		t.Errorf("documented type = %q, want %q", got, ceTypeCapacityPlanSuperseded)
	}
}

// E2: the producer's own example carries the type string, decodes through
// supersededRequestFrom, has subject = plan_id, and carries location and
// warehouse_id, so the tombstone is listed under the plan's location.
func TestGoldenEventDecodes_CapacityPlanSuperseded(t *testing.T) {
	ex := supersededGolden(t)
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode the producer's example: %v", err)
	}
	if evt.Type() != ceTypeCapacityPlanSuperseded {
		t.Fatalf("type = %q, want %q", evt.Type(), ceTypeCapacityPlanSuperseded)
	}
	data := ex.Payload["data"].(map[string]any)
	if evt.Subject() != data["plan_id"] {
		t.Errorf("subject %q must equal data.plan_id %q", evt.Subject(), data["plan_id"])
	}
	for _, field := range []string{"location", "warehouse_id", "site_id", "superseded_by", "plan_id"} {
		if v, _ := data[field].(string); v == "" {
			t.Errorf("the producer's example must carry data.%s", field)
		}
	}

	req, relevant, err := supersededRequestFrom(evt)
	if err != nil || !relevant {
		t.Fatalf("supersededRequestFrom = relevant %v, err %v; want a relevant request", relevant, err)
	}
	w := req.Window
	if req.EventID != ex.Payload["id"] || w.PlanID != data["plan_id"] || w.SupersededBy != data["superseded_by"] ||
		w.Status != order.PlannedCapacitySuperseded || w.Constraining() {
		t.Fatalf("request = %+v; want a non-constraining SUPERSEDED row from the example", req)
	}
	for field, got := range map[string]string{"location": w.Location, "site_id": w.SiteID, "path_id": w.PathID} {
		if got != data[field] {
			t.Errorf("window %s = %q, want the example's %q", field, got, data[field])
		}
	}
}

// E2 through the REAL consumer handle: the example is stored as a
// SUPERSEDED tombstone listed under the plan's location, never
// dead-lettered, and an additive producer field fails nothing (E4).
func TestGoldenEventHandled_CapacityPlanSuperseded(t *testing.T) {
	ex := supersededGolden(t)
	data := ex.Payload["data"].(map[string]any)
	f := newKPCFixture()
	if err := f.consumer.handleMessage(context.Background(), kafkagoMessage{Offset: 1, Value: goldenJSON(t, ex.Payload)}); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatal("the producer's example must not be dead-lettered")
	}
	w, ok := f.store.get(data["plan_id"].(string))
	if !ok {
		t.Fatal("the example's plan must be in the read model")
	}
	if w.Status != order.PlannedCapacitySuperseded || w.Location != data["location"] || w.SiteID != data["site_id"] || w.Constraining() {
		t.Fatalf("window = %+v; want a non-constraining SUPERSEDED row under the plan's location", w)
	}

	tolerant := clonePayload(ex.Payload)
	tolerant["id"] = "contract-superseded-additive-0001"
	tolerant["data"].(map[string]any)["future_additive_field"] = map[string]any{"nested": []any{1}}
	if err := f.consumer.handleMessage(context.Background(), kafkagoMessage{Offset: 2, Value: goldenJSON(t, tolerant)}); err != nil {
		t.Fatalf("unknown data attributes must not fail handling: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatal("an additive field must not dead-letter the event")
	}
}

// E3: the example carries the fleet CloudEvents 1.0 envelope with
// warehouse-planning's source and dataschema, and the pinned envelope
// schema requires every mandatory attribute.
func TestCloudEventsEnvelopeInvariants_CapacityPlanSuperseded(t *testing.T) {
	ex := supersededGolden(t)
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode the producer's example: %v", err)
	}
	assertCloudEventsEnvelope(t, ex.Name, evt, "/warehouse/warehouse-planning",
		"urn:warehouse:warehouse-planning:events:CapacityPlanSuperseded:v1")
	assertEnvelopeRequired(t, loadPinnedAsyncAPI(t, pinnedWarehousePlanSpec), supersededMessage)
}
