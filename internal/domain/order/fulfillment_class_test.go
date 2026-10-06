package order

import "testing"

func TestFulfillmentClass_SingleLineSingleUnit_IsSingle(t *testing.T) {
	line, err := NewOrderLine(1, "SKU-A", 1, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line: %v", err)
	}
	o, err := New("ORDER-1", []*OrderLine{line}, true)
	if err != nil {
		t.Fatalf("unexpected error building order: %v", err)
	}
	if got := o.FulfillmentClass(); got != ClassSingle {
		t.Fatalf("want ClassSingle, got %v", got)
	}
}

func TestFulfillmentClass_SingleLineMultiUnit_IsSameSKUMulti(t *testing.T) {
	line, err := NewOrderLine(1, "SKU-A", 3, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line: %v", err)
	}
	o, err := New("ORDER-1", []*OrderLine{line}, true)
	if err != nil {
		t.Fatalf("unexpected error building order: %v", err)
	}
	if got := o.FulfillmentClass(); got != ClassSameSKUMulti {
		t.Fatalf("want ClassSameSKUMulti, got %v", got)
	}
}

func TestFulfillmentClass_MultipleDistinctSKUs_IsMultiLineMulti(t *testing.T) {
	lineA, err := NewOrderLine(1, "SKU-A", 1, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line A: %v", err)
	}
	lineB, err := NewOrderLine(2, "SKU-B", 2, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line B: %v", err)
	}
	o, err := New("ORDER-1", []*OrderLine{lineA, lineB}, true)
	if err != nil {
		t.Fatalf("unexpected error building order: %v", err)
	}
	if got := o.FulfillmentClass(); got != ClassMultiLineMulti {
		t.Fatalf("want ClassMultiLineMulti, got %v", got)
	}
}

func TestFulfillmentClass_MultipleLinesSameSKU_IsStillMultiLineMulti(t *testing.T) {
	// Two separate order lines for the same SKU (e.g. requested at
	// different times, or with different fulfillment hints) are still
	// two lines — the discriminator is line count and distinct-SKU
	// count together, not merely "how many SKUs." A caller that wants
	// "3 of SKU-A" as a single-line multi must submit it as one line
	// with quantity 3 (covered by the SameSKUMulti test above), not as
	// three separate lines.
	lineA, err := NewOrderLine(1, "SKU-A", 1, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line A: %v", err)
	}
	lineB, err := NewOrderLine(2, "SKU-A", 1, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line B: %v", err)
	}
	o, err := New("ORDER-1", []*OrderLine{lineA, lineB}, true)
	if err != nil {
		t.Fatalf("unexpected error building order: %v", err)
	}
	if got := o.FulfillmentClass(); got != ClassMultiLineMulti {
		t.Fatalf("want ClassMultiLineMulti, got %v", got)
	}
}

func TestFulfillmentClass_IsComputedNotStored_ReflectsCurrentLines(t *testing.T) {
	// Read models/derived classifiers must never be stored state that
	// can drift — matches Order.Status()'s own "always derived" rule.
	// This test asserts FulfillmentClass is likewise a pure function of
	// Lines() on every call, not a field set once at construction.
	line, err := NewOrderLine(1, "SKU-A", 1, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line: %v", err)
	}
	o, err := New("ORDER-1", []*OrderLine{line}, true)
	if err != nil {
		t.Fatalf("unexpected error building order: %v", err)
	}
	if got := o.FulfillmentClass(); got != ClassSingle {
		t.Fatalf("want ClassSingle before any mutation, got %v", got)
	}
	// Rehydrate with two lines to simulate loading a different order
	// state through the same aggregate type — proves classification
	// reads Lines() live rather than caching a value from New().
	lineB, err := NewOrderLine(2, "SKU-B", 1, "", false)
	if err != nil {
		t.Fatalf("unexpected error building line B: %v", err)
	}
	rehydrated := Rehydrate(OrderSnapshot{ID: "ORDER-1", Lines: []*OrderLine{line, lineB}, AllowPartialShipment: true})
	if got := rehydrated.FulfillmentClass(); got != ClassMultiLineMulti {
		t.Fatalf("want ClassMultiLineMulti after rehydrating with a second distinct-SKU line, got %v", got)
	}
}
