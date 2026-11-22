package kafka

import (
	"log/slog"
	"reflect"
	"strings"
)

// Event-derived values (CloudEvent ids, types, subjects, payload fields, decode
// and use-case errors) are external input: strip CR and LF before they reach a
// log attribute so a crafted event cannot forge a log line.

// sanitizeLog strips CR and LF from s before it is logged.
func sanitizeLog(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r", ""), "\n", "")
}

// sanitizeErr is sanitizeLog for an error's text ("" for a nil error).
func sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeLog(err.Error())
}

// sanitizeArgs returns slog key/value args with every string-kinded value, every
// string attr and every error passed through sanitizeLog.
func sanitizeArgs(args []any) []any {
	out := make([]any, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case error:
			out[i] = sanitizeErr(v)
		case slog.Attr:
			if v.Value.Kind() == slog.KindString {
				v = slog.String(v.Key, sanitizeLog(v.Value.String()))
			}
			out[i] = v
		default:
			if rv := reflect.ValueOf(a); rv.Kind() == reflect.String {
				out[i] = sanitizeLog(rv.String())
			} else {
				out[i] = a
			}
		}
	}
	return out
}
