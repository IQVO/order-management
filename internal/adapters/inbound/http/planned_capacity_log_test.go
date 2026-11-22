package http_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// pcLineRecorder keeps the string form of every attribute value of every record.
type pcLineRecorder struct{ values *[]string }

func (h pcLineRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h pcLineRecorder) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h pcLineRecorder) WithGroup(string) slog.Handler            { return h }
func (h pcLineRecorder) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		*h.values = append(*h.values, a.Value.String())
		return true
	})
	return nil
}

// A failed capacity read model lookup on an order read can carry request
// text; the warning must keep it on one line.
func TestLogHygiene_CapacityLookupFailureIsLoggedOnOneLine(t *testing.T) {
	env := newPlannedCapacityEnv(t, pcFailingRepo{err: errors.New("site x-1\r\nlevel=INFO msg=forged")})
	var values []string
	prev := slog.Default()
	slog.SetDefault(slog.New(pcLineRecorder{values: &values}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	placeOrder(t, env)

	if len(values) == 0 {
		t.Fatal("a failed capacity lookup logged nothing")
	}
	for _, v := range values {
		if strings.ContainsAny(v, "\r\n") {
			t.Fatalf("logged attribute %q holds a line break", v)
		}
	}
}
