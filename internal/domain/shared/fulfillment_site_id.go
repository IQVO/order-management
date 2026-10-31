package shared

import "fmt"

// FulfillmentSiteID names the site an order is to be fulfilled from, as
// ASSIGNED BY THE CALLER (ADR 0044). This context never infers, looks up
// or sources a site: it only carries what the caller said, validated to
// the fleet's site-code shape.
//
// The shape mirrors facility-layout's site code (`^[A-Z0-9]+$`, at least
// one character): uppercase ASCII letters and digits only. No upper
// length bound is imposed because facility-layout imposes none; a bound
// invented here could reject a site that exists. The rule is mirrored,
// not called — there is no cross-service lookup, and an unknown but
// well-formed code is accepted (existence is the caller's concern).
type FulfillmentSiteID string

// NewFulfillmentSiteID validates and constructs a FulfillmentSiteID.
func NewFulfillmentSiteID(value string) (FulfillmentSiteID, error) {
	if value == "" {
		return "", fmt.Errorf("%w: must not be empty", ErrInvalidFulfillmentSiteID)
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		return "", fmt.Errorf("%w: %q must contain only uppercase letters and digits", ErrInvalidFulfillmentSiteID, value)
	}
	return FulfillmentSiteID(value), nil
}

func (s FulfillmentSiteID) String() string { return string(s) }
