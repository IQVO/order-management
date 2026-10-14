package kafkacatalog

import (
	"slices"
	"testing"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR-0042: the catalogue decodes process-path-management's additive
// `intake` (its ADR 0034) onto every active path, so PathSelectionPolicy can
// keep customer order lines on CUSTOMER_ORDER paths. Absent (a producer
// older than ADR 0034) decodes as the empty intake.

func listedIntake(t *testing.T, c *Consumer, id shared.PathId) []shared.DemandSource {
	t.Helper()
	for _, a := range c.ListActive() {
		if a.PathId == id {
			return a.Intake
		}
	}
	t.Fatalf("ListActive() has no %s", id)
	return nil
}

func TestConsumer_DecodesIntakeOntoListActive(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	// The exact data shape process-path-management's golden test pins.
	if err := c.handle(envelopeMsg(t, 0, 0, ceTypeCreated, map[string]any{
		"path_id": "PICK", "match_prefix": "pick", "direct": true, "required_capabilities": []string{"pick"},
		"cycle_time_p95": "2h0m0s", "eligibility": map[string]any{}, "intake": []string{"CUSTOMER_ORDER"},
	})); err != nil {
		t.Fatalf("handle: %v", err)
	}
	got := listedIntake(t, c, "PICK")
	if !slices.Equal(got, []shared.DemandSource{shared.DemandSourceCustomerOrder}) {
		t.Fatalf("PICK intake = %v, want [CUSTOMER_ORDER]", got)
	}
}

func TestConsumer_AbsentIntake_DecodesAsEmpty(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(envelopeMsg(t, 0, 0, ceTypeCreated, map[string]any{
		"path_id": "PACK", "match_prefix": "pack", "cycle_time_p95": "2h0m0s", "eligibility": map[string]any{},
	})); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if got := listedIntake(t, c, "PACK"); len(got) != 0 {
		t.Fatalf("PACK intake = %v, want empty for a payload without intake", got)
	}
}

func TestConsumer_UpdatedEvent_ReplacesIntake(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(envelopeMsg(t, 0, 0, ceTypeCreated, map[string]any{
		"path_id": "PICK", "match_prefix": "pick", "cycle_time_p95": "2h0m0s", "eligibility": map[string]any{}, "intake": []string{},
	})); err != nil {
		t.Fatalf("handle created: %v", err)
	}
	if err := c.handle(envelopeMsg(t, 0, 1, ceTypeUpdated, map[string]any{
		"path_id": "PICK", "match_prefix": "pick", "cycle_time_p95": "2h0m0s", "eligibility": map[string]any{},
		"intake": []string{"CUSTOMER_ORDER", "STOCK_TRANSFER"},
	})); err != nil {
		t.Fatalf("handle updated: %v", err)
	}
	got := listedIntake(t, c, "PICK")
	want := []shared.DemandSource{shared.DemandSourceCustomerOrder, "STOCK_TRANSFER"}
	if !slices.Equal(got, want) {
		t.Fatalf("PICK intake = %v, want %v", got, want)
	}
}

func TestConsumer_ListActiveIntakeIsADefensiveCopy(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(envelopeMsg(t, 0, 0, ceTypeCreated, map[string]any{
		"path_id": "PICK", "match_prefix": "pick", "cycle_time_p95": "2h0m0s", "eligibility": map[string]any{}, "intake": []string{"CUSTOMER_ORDER"},
	})); err != nil {
		t.Fatalf("handle: %v", err)
	}
	listedIntake(t, c, "PICK")[0] = "MUTATED"
	if got := listedIntake(t, c, "PICK"); got[0] != shared.DemandSourceCustomerOrder {
		t.Fatalf("mutating a ListActive result changed the cache: %v", got)
	}
}
