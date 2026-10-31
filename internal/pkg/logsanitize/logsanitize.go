// Package logsanitize neutralizes CWE-117 log injection for untrusted
// values that reach log statements: control characters (newline,
// carriage return, tab, the rest of the C0 range, and DEL) are rewritten
// to spaces, so an attacker-controlled value can neither forge a fake
// log line nor shift a log line's alignment once the record reaches a
// downstream viewer or aggregator that does not preserve structured
// logging's own escaping.
package logsanitize

import "strings"

// Sanitize returns s with every control rune replaced by a single space.
// It is deliberately conservative: nothing is dropped (the value's width
// is preserved, so log alignment does not collapse) and the input is
// never interpreted, making it safe to apply at any log sink that
// receives untrusted data -- order ids from the wire, SKUs from
// product-master's classification copy, site ids from consumed schedule
// events.
func Sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f { // C0 controls (\t \n \r ...) and DEL
			return ' '
		}
		return r
	}, s)
}
