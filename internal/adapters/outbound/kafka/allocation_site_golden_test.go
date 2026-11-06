package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR 0050: OrderAllocated/OrderPartiallyAllocated carry the order's
// caller-assigned fulfillment site as the OPTIONAL top-level `site_id`.
// These goldens pin the contract additively:
//
//   - empty site  => the `data` bytes are BYTE-IDENTICAL to the
//     pre-ADR-0050 payload (same literals TestPublisher_GoldenCloudEvents
//     has pinned since the freeze — omitempty adds nothing);
//   - site set    => exactly one `site_id` member appears, in struct
//     order after the promise fields, before `lines`.
//
// The comparison is on the RAW member bytes (json.RawMessage), not a
// re-marshalled map, so key order, omission and spacing are all pinned.

var siteGoldenTime = time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)

var siteGoldenLines = []shared.ReleasedLine{
	{LineNo: 1, SKU: "SKU-1", PathID: "pick", GiftWrap: true, FulfillmentClass: "SINGLE"},
}

// emptySiteAllocatedData is today's (pre-ADR-0050) OrderAllocated data for
// a LeadTime-basis promise: no promise_cpt_id/promise_basis, no site.
const emptySiteAllocatedData = `{"order_id":"ord-1","promise_date":"2026-11-06T18:00:00Z","lines":[{"line_no":1,"sku":"SKU-1","path_id":"pick","gift_wrap":true,"fulfillment_class":"SINGLE"}]}`

// sitedAllocatedData adds exactly the site_id member.
const sitedAllocatedData = `{"order_id":"ord-1","promise_date":"2026-11-06T18:00:00Z","site_id":"WH2","lines":[{"line_no":1,"sku":"SKU-1","path_id":"pick","gift_wrap":true,"fulfillment_class":"SINGLE"}]}`

// emptySitePartialData is today's OrderPartiallyAllocated data for a
// Capability-basis promise (both promise members present, no site).
const emptySitePartialData = `{"order_id":"ord-2","promise_date":"2026-11-06T18:00:00Z","promise_cpt_id":"sp1-1800","promise_basis":"Capability","lines":[{"line_no":1,"sku":"SKU-1","path_id":"pick","gift_wrap":true,"fulfillment_class":"SINGLE"}]}`

// sitedPartialData proves site_id slots after the promise members.
const sitedPartialData = `{"order_id":"ord-2","promise_date":"2026-11-06T18:00:00Z","promise_cpt_id":"sp1-1800","promise_basis":"Capability","site_id":"DC7","lines":[{"line_no":1,"sku":"SKU-1","path_id":"pick","gift_wrap":true,"fulfillment_class":"SINGLE"}]}`

func TestAllocationSite_GoldenPayloadBytes(t *testing.T) {
	promise := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		event     shared.DomainEvent
		wantData  string
		wantSiteK bool // must the byte sequence "site_id" appear anywhere?
	}{
		{
			name:      "OrderAllocated/empty-site-is-byte-identical",
			event:     shared.NewOrderAllocatedWithPromise(siteGoldenTime, "ord-1", promise, "", "", siteGoldenLines),
			wantData:  emptySiteAllocatedData,
			wantSiteK: false,
		},
		{
			name:      "OrderAllocated/site-id-present",
			event:     shared.NewOrderAllocatedWithPromise(siteGoldenTime, "ord-1", promise, "", "", siteGoldenLines).WithSite("WH2"),
			wantData:  sitedAllocatedData,
			wantSiteK: true,
		},
		{
			name:      "OrderPartiallyAllocated/empty-site-is-byte-identical",
			event:     shared.NewOrderPartiallyAllocatedWithPromise(siteGoldenTime, "ord-2", 1, 1, promise, "sp1-1800", "Capability", siteGoldenLines),
			wantData:  emptySitePartialData,
			wantSiteK: false,
		},
		{
			name:      "OrderPartiallyAllocated/site-id-present",
			event:     shared.NewOrderPartiallyAllocatedWithPromise(siteGoldenTime, "ord-2", 1, 1, promise, "sp1-1800", "Capability", siteGoldenLines).WithSite("DC7"),
			wantData:  sitedPartialData,
			wantSiteK: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, ok, err := kafka.NewPublisher(&fakeWriter{}).Encode(context.Background(), tc.event)
			if err != nil || !ok {
				t.Fatalf("Encode: ok=%v err=%v", ok, err)
			}
			var envelope struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(enc.Value, &envelope); err != nil {
				t.Fatalf("unmarshal envelope: %v (%s)", err, enc.Value)
			}
			if string(envelope.Data) != tc.wantData {
				t.Errorf("data bytes:\n got: %s\nwant: %s", envelope.Data, tc.wantData)
			}
			if got := bytes.Contains(enc.Value, []byte("site_id")); got != tc.wantSiteK {
				t.Errorf("value contains \"site_id\" = %v, want %v (%s)", got, tc.wantSiteK, enc.Value)
			}
		})
	}
}
