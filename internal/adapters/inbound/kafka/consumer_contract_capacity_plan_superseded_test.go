package kafka

// Consumer contract test for CapacityPlanSuperseded (ADR 0052), written
// against a LOCAL example of the wire shape warehouse-planning ADR 0024
// decision 3 specifies (testdata/capacity-plan-superseded.example.json),
// NOT the pinned producer spec: warehouse-planning has not published the
// type yet, so the pinned copy does not document it and this test must not
// require it to.
//
// FOLLOW-UP once warehouse-planning's change merges: refresh the pinned
// testdata/warehouse-planning.asyncapi.yaml, then add the pinned-example
// assertions (E1: the channel documents ceTypeCapacityPlanSuperseded; E2:
// the producer's own example decodes and routes here) and drop the local
// example.

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/domain/order"
)

const localSupersededExample = "testdata/capacity-plan-superseded.example.json"

func loadLocalSupersededExample(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(localSupersededExample)
	if err != nil {
		t.Fatalf("read %s: %v", localSupersededExample, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", localSupersededExample, err)
	}
	return doc
}

// The example carries the fleet CloudEvents 1.0 envelope with
// warehouse-planning's source and the dataschema convention, the full type
// this consumer dispatches on, and subject = data.plan_id.
func TestLocalExampleEnvelope_CapacityPlanSuperseded(t *testing.T) {
	doc := loadLocalSupersededExample(t)
	evt, err := cloudevents.Decode(goldenJSON(t, doc))
	if err != nil {
		t.Fatalf("decode the local example: %v", err)
	}
	assertCloudEventsEnvelope(t, "capacity-plan-superseded", evt, "/warehouse/warehouse-planning",
		"urn:warehouse:warehouse-planning:events:CapacityPlanSuperseded:v1")
	if evt.Type() != ceTypeCapacityPlanSuperseded {
		t.Fatalf("type = %q, want %q", evt.Type(), ceTypeCapacityPlanSuperseded)
	}
	data := doc["data"].(map[string]any)
	for _, field := range []string{"plan_id", "superseded_by", "site_id", "path_id", "window_start", "window_end"} {
		if _, ok := data[field]; !ok {
			t.Errorf("the ADR 0024 field %q is missing from the example", field)
		}
	}
	if evt.Subject() != data["plan_id"] {
		t.Errorf("subject %q must equal data.plan_id %q", evt.Subject(), data["plan_id"])
	}
}

// The example routes through the REAL decode path and consumer handle and
// stores a SUPERSEDED tombstone carrying the example's own fields.
func TestLocalExampleDecodes_CapacityPlanSuperseded(t *testing.T) {
	doc := loadLocalSupersededExample(t)
	data := doc["data"].(map[string]any)
	f := newKPCFixture()
	if err := f.consumer.handleMessage(context.Background(), kafkagoMessage{Offset: 1, Value: goldenJSON(t, doc)}); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatal("the example must not be dead-lettered")
	}
	w, ok := f.store.get(data["plan_id"].(string))
	if !ok {
		t.Fatal("the example's plan must be in the read model")
	}
	if w.Status != order.PlannedCapacitySuperseded || w.SupersededBy != data["superseded_by"] ||
		w.Location != data["site_id"] || w.PathID != data["path_id"] || w.Constraining() {
		t.Fatalf("window = %+v; want a non-constraining SUPERSEDED row from the example", w)
	}

	// Additive producer fields must not fail decode or handling.
	tolerant := clonePayload(doc)
	tolerant["id"] = "contract-superseded-additive-0001"
	tolerant["data"].(map[string]any)["future_additive_field"] = map[string]any{"nested": []any{1}}
	if err := f.consumer.handleMessage(context.Background(), kafkagoMessage{Offset: 2, Value: goldenJSON(t, tolerant)}); err != nil {
		t.Fatalf("unknown data attributes must not fail handling: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatal("an additive field must not dead-letter the event")
	}
}
