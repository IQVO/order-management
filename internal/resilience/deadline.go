package resilience

import (
	"context"
	"time"
)

// CallTimeout derives the timeout for one outbound call from the
// inbound request's remaining context budget (plan section 2.2 / ADR-
// 0025), instead of a hardcoded fresh timeout that could outlast the
// caller's own patience:
//
//   - ctx has a deadline and it is sooner than maxPerCall from now: the
//     returned context inherits that deadline unchanged (ctx itself is
//     returned as-is — no need to wrap it further, and doing so would
//     just add a redundant cancel func).
//   - ctx has no deadline, or one further away than maxPerCall: the
//     returned context is capped at maxPerCall from now, so a caller
//     with a very long or absent deadline (a background job, a test)
//     can never make a single outbound call hang indefinitely.
//
// The returned CancelFunc must always be called by the caller (typically
// via defer) — when ctx's own deadline is reused unchanged this is a
// cheap no-op wrapper, not a leak.
func CallTimeout(ctx context.Context, maxPerCall time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining <= maxPerCall {
			// The inbound deadline is already at or inside the
			// per-call cap: honor it exactly, don't shorten it
			// further and don't wrap it in a second cancel scope
			// the caller has to remember is also required.
			return context.WithCancel(ctx)
		}
	}
	return context.WithTimeout(ctx, maxPerCall)
}
