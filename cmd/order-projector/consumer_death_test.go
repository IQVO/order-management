package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/consumerdeath"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// ADR 0045: the projector's analytics consumer dying ends the process; a
// signal is a clean shutdown, even when a death lands with it.
func TestWaitForStop(t *testing.T) {
	boom := errors.New("fetching message: [27] Group Load In Progress")
	t.Run("a consumer death is returned", func(t *testing.T) {
		deaths := consumerdeath.New()
		deaths.Watch(context.Background(), "analytics consumer", quietLogger(), func(context.Context) error { return boom })
		got := make(chan error, 1)
		go func() { got <- waitForStop(make(chan os.Signal, 1), deaths) }()
		select {
		case err := <-got:
			if !errors.Is(err, boom) {
				t.Fatalf("waitForStop = %v, want it to wrap the consumer's error", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a dead consumer did not end the process")
		}
	})
	t.Run("a signal is a clean shutdown", func(t *testing.T) {
		stop := make(chan os.Signal, 1)
		stop <- os.Interrupt
		if err := waitForStop(stop, consumerdeath.New()); err != nil {
			t.Fatalf("waitForStop = %v, want nil on a signal", err)
		}
	})
	t.Run("a death that lands with a signal is part of the shutdown", func(t *testing.T) {
		deaths := consumerdeath.New()
		deaths.Watch(context.Background(), "analytics consumer", quietLogger(), func(context.Context) error { return boom })
		stop := make(chan os.Signal, 1)
		stop <- os.Interrupt
		if err := waitForStop(stop, deaths); err != nil {
			t.Fatalf("waitForStop = %v, want nil when a signal is pending", err)
		}
	})
}
