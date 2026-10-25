// Package consumerdeath turns a Kafka consumer goroutine that stops on its own
// into a process exit (ADR 0045). A command selects on Deaths.C() beside its
// HTTP server and signal context and returns the error from run().
package consumerdeath

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Deaths is the one channel every Kafka consumer goroutine of this
// process reports an UNEXPECTED stop on (ADR 0045). serveAndShutdown selects
// on it beside the HTTP server and the signal context, so a consumer that
// dies ends the process (run() returns the error, the process exits non-zero
// and the kubelet restarts the pod) instead of leaving a Ready pod whose read
// model is frozen.
//
// It holds exactly one death: run() exits on the first, and every later
// death is logged and dropped so a second dying consumer never blocks on a
// send. A nil *Deaths is valid and only logs (tests that start a
// consumer without a serve loop).
type Deaths struct {
	ch chan error
}

// New returns an empty Deaths ready to be shared by every consumer goroutine.
func New() *Deaths {
	return &Deaths{ch: make(chan error, 1)}
}

// C is the receive side serveAndShutdown selects on. A nil receiver yields a
// nil channel, which blocks forever in a select.
func (d *Deaths) C() <-chan error {
	if d == nil {
		return nil
	}
	return d.ch
}

// Watch runs one consumer to completion on the caller's goroutine and
// reports how it stopped. ctx is the context the consumer runs on; whoever
// cancels it (the consumer's stop func, the catalogue cleanup, the repromise
// drain) is asking for a stop.
func (d *Deaths) Watch(ctx context.Context, name string, logger *slog.Logger, run func(context.Context) error) {
	err := StopCause(ctx, run(ctx))
	if err == nil {
		return
	}
	logger.Error(name+" stopped", "error", err)
	if d == nil {
		return
	}
	select {
	case d.ch <- fmt.Errorf("%s stopped: %w", name, err):
	default:
	}
}

// StopCause classifies the return of a consumer's Run: nil for a clean stop,
// otherwise the error that ends the process. A consumer is meant to run until
// its context ends, so:
//
//   - the context ended (whatever Run returned): a requested stop, clean;
//   - context.Canceled: the same request seen from inside Run, clean;
//   - any other error: a death;
//   - nil while the context is still live: a death.
func StopCause(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return nil
	}
	if err == nil {
		return errors.New("returned without an error while the service was still running")
	}
	return err
}
