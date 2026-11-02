// Package logsanitize neutralizes CWE-117 log injection for untrusted
// values that reach log statements: the line-forging control characters
// (newline and carriage return) are stripped, so an attacker-controlled
// value can neither forge a fake log line nor ride along onto a second
// line once the record reaches a downstream viewer or aggregator that
// does not preserve structured logging's own escaping.
//
// The strip is built on strings.NewReplacer — not a strings.Map
// callback — because the replacer is the sanitizer shape CodeQL's
// go/log-injection query recognizes; this mirrors the sanitizeForLog
// helpers in this repo's inbound/http package and across the fleet
// (wes-work-planning #243, whose merged variant cleared the alerts).
package logsanitize

import "strings"

// logReplacer is package-level so the replacer is built once, not per call.
var logReplacer = strings.NewReplacer("\n", "", "\r", "")

// Sanitize returns s with every newline and carriage return removed.
// It is deliberately conservative: nothing is interpreted, and the
// output can never span more log lines than the caller intended, making
// it safe to apply at any log sink that receives untrusted data --
// order ids from the wire, SKUs from product-master's classification
// copy, site ids from consumed schedule events.
func Sanitize(s string) string {
	return logReplacer.Replace(s)
}
