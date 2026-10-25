package consumerdeath

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func recv(ch <-chan error) (error, bool) {
	select {
	case err := <-ch:
		return err, true
	case <-time.After(300 * time.Millisecond):
		return nil, false
	}
}

// StopCause is the whole policy: a stop the service asked for is clean, any
// other stop is a death.
func TestStopCause(t *testing.T) {
	boom := errors.New("read tcp: connection reset by peer")
	live, cancelled := context.Background(), func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}()
	cases := []struct {
		name      string
		ctx       context.Context
		err       error
		wantDeath bool
	}{
		{"an error while the service runs is a death", live, boom, true},
		{"a broker rebalance error is a death", live, errors.New("[27] Group Load In Progress"), true},
		{"nil while the service runs is a death", live, nil, true},
		{"context.Canceled is a clean stop", live, context.Canceled, false},
		{"a wrapped context.Canceled is a clean stop", live, errors.Join(errors.New("fetch"), context.Canceled), false},
		{"an error after the context ended is a clean stop", cancelled, boom, false},
		{"nil after the context ended is a clean stop", cancelled, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StopCause(tc.ctx, tc.err)
			if (got != nil) != tc.wantDeath {
				t.Fatalf("StopCause = %v, want death=%v", got, tc.wantDeath)
			}
			if tc.wantDeath && tc.err != nil && !errors.Is(got, tc.err) {
				t.Fatalf("StopCause = %v, want it to carry %v", got, tc.err)
			}
		})
	}
}

// Watch reports a death on C(), and reports nothing for a stop the service
// itself asked for.
func TestWatch(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name      string
		run       func(ctx context.Context) func(context.Context) error
		cancel    bool // cancel the consumer's context before Run returns
		wantDeath bool
	}{
		{"an error is reported", func(context.Context) func(context.Context) error {
			return func(context.Context) error { return boom }
		}, false, true},
		{"nil while running is reported", func(context.Context) func(context.Context) error {
			return func(context.Context) error { return nil }
		}, false, true},
		{"a stop caused by the stop func is silent", func(context.Context) func(context.Context) error {
			return func(ctx context.Context) error { <-ctx.Done(); return boom }
		}, true, false},
		{"context.Canceled is silent", func(context.Context) func(context.Context) error {
			return func(context.Context) error { return context.Canceled }
		}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d := New()
			done := make(chan struct{})
			go func() { defer close(done); d.Watch(ctx, "test consumer", quietLogger(), tc.run(ctx)) }()
			if tc.cancel {
				cancel()
			}
			<-done
			err, got := recv(d.C())
			if got != tc.wantDeath {
				t.Fatalf("death reported = %v (%v), want %v", got, err, tc.wantDeath)
			}
			if tc.wantDeath && !errors.Is(err, boom) && tc.name == "an error is reported" {
				t.Fatalf("death = %v, want it to wrap boom", err)
			}
		})
	}
}

// Several consumers dying together must not block each other even when nobody
// reads the channel, and exactly one death is held.
func TestWatch_SeveralDeathsNeverBlockAndOneIsHeld(t *testing.T) {
	boom := errors.New("boom")
	const n = 4
	d := New()
	finished := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			d.Watch(context.Background(), "test consumer", quietLogger(), func(context.Context) error { return boom })
			finished <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d consumers finished: a dying consumer blocked", i, n)
		}
	}
	err, got := recv(d.C())
	if !got || !errors.Is(err, boom) {
		t.Fatalf("held death = %v (reported %v), want it to wrap boom", err, got)
	}
	if _, more := recv(d.C()); more {
		t.Fatal("more than one death was held")
	}
}

// A nil *Deaths only logs: it must not panic and its channel never fires.
func TestNilDeaths(t *testing.T) {
	var d *Deaths
	d.Watch(context.Background(), "test consumer", quietLogger(), func(context.Context) error { return errors.New("boom") })
	if _, got := recv(d.C()); got {
		t.Fatal("a nil Deaths must never report")
	}
}
