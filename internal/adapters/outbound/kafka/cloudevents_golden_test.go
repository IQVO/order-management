package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// goldenTime is the fixed occurred-at every golden case uses, expressed in
// a non-UTC zone so the tests also prove `time` is normalised to UTC.
var goldenTime = time.Date(2026, 9, 30, 9, 0, 0, 0, time.FixedZone("BRT", -3*3600))

// assertContentTypeHeader asserts msg carries the CloudEvents structured
// mode content-type header.
func assertContentTypeHeader(t *testing.T, msg kafkago.Message) {
	t.Helper()
	for _, h := range msg.Headers {
		if h.Key == "content-type" {
			if string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
				t.Errorf("content-type header = %q", h.Value)
			}
			return
		}
	}
	t.Errorf("message has no content-type header: %+v", msg.Headers)
}

// assertGolden compares raw against want after replacing the
// randomly-minted CloudEvents id with "<id>" (asserted non-empty).
func assertGolden(t *testing.T, raw []byte, want string) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if id, _ := got["id"].(string); id == "" {
		t.Fatalf("id is empty: %s", raw)
	}
	got["id"] = "<id>"
	var w map[string]any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Errorf("golden mismatch\n got: %s\nwant: %s", gb, wb)
	}
}

func TestPublisher_GoldenCloudEvents(t *testing.T) {
	promise := time.Date(2026, 10, 1, 18, 0, 0, 0, time.UTC)
	lines := []shared.ReleasedLine{{LineNo: 1, SKU: "SKU-1", PathID: "pick", GiftWrap: true, FulfillmentClass: "SINGLE"}}

	cases := []struct {
		name  string
		event shared.DomainEvent
		want  string
	}{
		{
			name:  "OrderAllocated",
			event: shared.NewOrderAllocated(goldenTime, "ord-1", promise, lines),
			want: `{"specversion":"1.0","id":"<id>","source":"/warehouse/order-management",
				"type":"com.warehouse.wes.order-management.order.OrderAllocated","subject":"ord-1",
				"time":"2026-09-30T12:00:00Z","datacontenttype":"application/json",
				"dataschema":"urn:warehouse:order-management:events:OrderAllocated:v1",
				"data":{"order_id":"ord-1","promise_date":"2026-10-01T18:00:00Z","lines":[{"line_no":1,"sku":"SKU-1","path_id":"pick","gift_wrap":true,"fulfillment_class":"SINGLE"}]}}`,
		},
		{
			name:  "OrderPartiallyAllocated",
			event: shared.NewOrderPartiallyAllocated(goldenTime, "ord-2", 1, 1, promise, lines),
			want: `{"specversion":"1.0","id":"<id>","source":"/warehouse/order-management",
				"type":"com.warehouse.wes.order-management.order.OrderPartiallyAllocated","subject":"ord-2",
				"time":"2026-09-30T12:00:00Z","datacontenttype":"application/json",
				"dataschema":"urn:warehouse:order-management:events:OrderPartiallyAllocated:v1",
				"data":{"order_id":"ord-2","promise_date":"2026-10-01T18:00:00Z","lines":[{"line_no":1,"sku":"SKU-1","path_id":"pick","gift_wrap":true,"fulfillment_class":"SINGLE"}]}}`,
		},
		{
			name:  "OrderRepromised",
			event: shared.NewOrderRepromised(goldenTime, "ord-3", "sp1-1200", "sp1-1800", "TaskCPTMissed"),
			want: `{"specversion":"1.0","id":"<id>","source":"/warehouse/order-management",
				"type":"com.warehouse.wes.order-management.order.OrderRepromised","subject":"ord-3",
				"time":"2026-09-30T12:00:00Z","datacontenttype":"application/json",
				"dataschema":"urn:warehouse:order-management:events:OrderRepromised:v1",
				"data":{"order_id":"ord-3","cpt_id_old":"sp1-1200","cpt_id_new":"sp1-1800","reason":"TaskCPTMissed"}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/publish", func(t *testing.T) {
			w := &fakeWriter{}
			msg := publishOne(t, w, kafka.NewPublisher(w), tc.event)
			assertGolden(t, msg.Value, tc.want)
			assertContentTypeHeader(t, msg)
		})
		t.Run(tc.name+"/encode", func(t *testing.T) {
			enc, ok, err := kafka.NewPublisher(&fakeWriter{}).Encode(context.Background(), tc.event)
			if err != nil || !ok {
				t.Fatalf("Encode: ok=%v err=%v", ok, err)
			}
			assertGolden(t, enc.Value, tc.want)
			assertContentTypeHeader(t, kafkago.Message{Headers: enc.Headers})
			if enc.Topic != kafka.Topic {
				t.Errorf("topic = %q", enc.Topic)
			}
			if enc.EventType != typeOf(tc.name) {
				t.Errorf("EventType = %q, want %q", enc.EventType, typeOf(tc.name))
			}
		})
	}
}

// TestPublisher_Encode_IDStableInValue proves the id is minted once at
// encode time and lives inside the persisted bytes (what the outbox
// stores and the relay re-sends verbatim).
func TestPublisher_Encode_IDStableInValue(t *testing.T) {
	ev := shared.NewOrderRepromised(goldenTime, "ord-3", "a", "b", "r")
	pub := kafka.NewPublisher(&fakeWriter{})
	a, _, _ := pub.Encode(context.Background(), ev)
	b, _, _ := pub.Encode(context.Background(), ev)
	ea, eb := decodeCE(t, a.Value), decodeCE(t, b.Value)
	if ea.ID == "" || ea.ID == eb.ID {
		t.Fatalf("ids must be non-empty and unique per encode: %q %q", ea.ID, eb.ID)
	}
	if again := decodeCE(t, a.Value); again.ID != ea.ID {
		t.Fatalf("id not stable across re-decode of persisted bytes")
	}
}

func TestAnalyticsPublisher_GoldenCloudEvents(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	cases := []struct {
		name  string
		event shared.DomainEvent
		data  string
	}{
		{"OrderReceived", shared.NewOrderReceived(goldenTime, "o1", 2), `{"order_id":"o1","path_id":"pick","line_count":2}`},
		{"OrderAllocated", shared.NewOrderAllocated(goldenTime, "o1", goldenTime, []shared.ReleasedLine{{LineNo: 1, SKU: sku, PathID: "singles"}}), `{"order_id":"o1","path_id":"singles","split_shipment":false}`},
		{"OrderPartiallyAllocated", shared.NewOrderPartiallyAllocated(goldenTime, "o1", 1, 1, goldenTime, []shared.ReleasedLine{{LineNo: 1, SKU: sku, PathID: "singles"}}), `{"order_id":"o1","path_id":"singles","allocated_lines":1,"backordered_lines":1,"split_shipment":false}`},
		{"OrderAllocationPartiallyFailed", shared.NewOrderAllocationPartiallyFailed(goldenTime, "o1", 1, 2, "boom"), `{"order_id":"o1","path_id":"pick","allocated_lines":1,"remaining_lines":2}`},
		{"OrderReleased", shared.NewOrderReleased(goldenTime, "o1"), `{"order_id":"o1","path_id":"pick"}`},
		{"OrderCancelled", shared.NewOrderCancelled(goldenTime, "o1", 3), `{"order_id":"o1","path_id":"pick","revoked_reservations":3}`},
		{"OrderLineAllocated", shared.NewOrderLineAllocated(goldenTime, "o1", 1, sku, 2, "res-1"), `{"order_id":"o1","line_no":1,"path_id":"pick","sku":"SKU-1"}`},
		{"OrderLineBackordered", shared.NewOrderLineBackordered(goldenTime, "o1", 1, sku, 2), `{"order_id":"o1","line_no":1,"path_id":"pick","sku":"SKU-1"}`},
		{"OrderLineReleased", shared.NewOrderLineReleased(goldenTime, "o1", 1, "singles", "wu-1"), `{"order_id":"o1","line_no":1,"path_id":"singles","work_unit_id":"wu-1"}`},
		{"OrderRepromised", shared.NewOrderRepromised(goldenTime, "o1", "a", "b", "TaskCPTMissed"), `{"order_id":"o1","cpt_id_old":"a","cpt_id_new":"b","reason":"TaskCPTMissed"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &fakeWriter{}
			p := kafka.NewAnalyticsPublisher(nil, fakeOrderRepo{order: orderOnPath(t, "o1", "pick")}, func() string { return "evt-fixed" })
			p.Writer = w
			if err := p.Publish(context.Background(), tc.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.messages) != 1 {
				t.Fatalf("messages = %d", len(w.messages))
			}
			want := `{"specversion":"1.0","id":"<id>","source":"/warehouse/order-management",
				"type":"com.warehouse.wes.order-management.order.` + tc.name + `","subject":"o1",
				"time":"2026-09-30T12:00:00Z","datacontenttype":"application/json",
				"dataschema":"urn:warehouse:order-management:analytics:` + tc.name + `:v1",
				"data":` + tc.data + `}`
			assertGolden(t, w.messages[0].Value, want)
			assertContentTypeHeader(t, w.messages[0])
			var raw map[string]any
			_ = json.Unmarshal(w.messages[0].Value, &raw)
			if _, has := raw["schema_version"]; has {
				t.Error("schema_version must not be present on the wire")
			}

			enc, ok, err := p.Encode(context.Background(), tc.event)
			if err != nil || !ok {
				t.Fatalf("Encode: ok=%v err=%v", ok, err)
			}
			assertGolden(t, enc.Value, want)
			assertContentTypeHeader(t, kafkago.Message{Headers: enc.Headers})
			if enc.Topic != kafka.AnalyticsTopic || enc.EventType != typeOf(tc.name) {
				t.Errorf("topic/type = %q/%q", enc.Topic, enc.EventType)
			}
		})
	}
}
