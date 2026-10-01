package cloudevents

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestType(t *testing.T) {
	got := Type("order", "OrderAllocated")
	want := "com.warehouse.wes.order-management.order.OrderAllocated"
	if got != want {
		t.Fatalf("Type = %q, want %q", got, want)
	}
}

func TestDataSchema(t *testing.T) {
	if got, want := DataSchema(StreamEvents, "OrderAllocated", 1), "urn:warehouse:order-management:events:OrderAllocated:v1"; got != want {
		t.Fatalf("DataSchema(events) = %q, want %q", got, want)
	}
	if got, want := DataSchema(StreamAnalytics, "OrderReceived", 2), "urn:warehouse:order-management:analytics:OrderReceived:v2"; got != want {
		t.Fatalf("DataSchema(analytics) = %q, want %q", got, want)
	}
}

func TestSource(t *testing.T) {
	if Source != "/warehouse/order-management" {
		t.Fatalf("Source = %q", Source)
	}
}

func TestNew_GoldenJSON(t *testing.T) {
	b, err := New(Spec{
		ID:        "11111111-1111-4111-8111-111111111111",
		Entity:    "order",
		EventName: "OrderAllocated",
		Subject:   "ORD-1",
		Time:      time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("x", -3*3600)),
		Stream:    StreamEvents,
		Version:   1,
		Data:      map[string]any{"order_id": "ORD-1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/order-management","type":"com.warehouse.wes.order-management.order.OrderAllocated","subject":"ORD-1","datacontenttype":"application/json","dataschema":"urn:warehouse:order-management:events:OrderAllocated:v1","time":"2026-09-30T15:00:00Z","data":{"order_id":"ORD-1"}}`
	assertJSONEqual(t, b, want)
}

func TestNew_DefaultsVersionToOne(t *testing.T) {
	b, err := New(Spec{ID: "id-1", Entity: "order", EventName: "X", Subject: "s", Time: time.Unix(0, 0), Stream: StreamAnalytics, Data: map[string]any{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.DataSchema() != "urn:warehouse:order-management:analytics:X:v1" {
		t.Fatalf("dataschema = %q", e.DataSchema())
	}
}

func TestNew_RejectsEmptySubject(t *testing.T) {
	if _, err := New(Spec{ID: "id", Entity: "order", EventName: "X", Time: time.Now(), Stream: StreamEvents, Data: map[string]any{}}); err == nil {
		t.Fatal("expected error for empty subject")
	}
}

func TestNew_RejectsEmptyID(t *testing.T) {
	if _, err := New(Spec{Entity: "order", EventName: "X", Subject: "s", Time: time.Now(), Stream: StreamEvents, Data: map[string]any{}}); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("header = %s: %s", h.Key, h.Value)
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	b, err := New(Spec{ID: "abc", Entity: "order", EventName: "OrderRepromised", Subject: "ORD-9", Time: time.Unix(100, 0), Stream: StreamEvents, Version: 1, Data: map[string]string{"order_id": "ORD-9"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "abc" || e.Subject() != "ORD-9" || e.Type() != "com.warehouse.wes.order-management.order.OrderRepromised" {
		t.Fatalf("unexpected event %v", e)
	}
	var data map[string]string
	if err := e.DataAs(&data); err != nil || data["order_id"] != "ORD-9" {
		t.Fatalf("DataAs: %v %v", data, err)
	}
}

func TestDecode_RejectsLegacyFlatEnvelope(t *testing.T) {
	flat := []byte(`{"event_id":"e1","event_type":"OrderAllocated","occurred_at":"2026-01-01T00:00:00Z","source":"order-management","data":{}}`)
	if _, err := Decode(flat); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsBadJSON(t *testing.T) {
	if _, err := Decode([]byte("not json")); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsWrongSpecVersion(t *testing.T) {
	raw := []byte(`{"specversion":"0.3","id":"1","source":"/x","type":"t"}`)
	if _, err := Decode(raw); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsMissingRequiredAttribute(t *testing.T) {
	raw := []byte(`{"specversion":"1.0","source":"/x","type":"t"}`)
	if _, err := Decode(raw); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("unmarshal got: %v (%s)", err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}
