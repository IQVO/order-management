package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/order-management/internal/consumerdeath"
)

// failingReader is a kafkacatalog.Reader whose broker answers with an error,
// the way the live one did under load.
type failingReader struct{ err error }

func (r failingReader) ReadMessage(context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, r.err
}
func (failingReader) Close() error { return nil }

// ADR 0045: a consumer that stops on its own makes serveAndShutdown, and so
// run(), return its error while the HTTP server is still healthy. A consumer
// that stopped because the service asked it to reports nothing: only the real
// death ends the process.
func TestServeAndShutdown_AConsumerDeathReturnsTheErrorFromRun(t *testing.T) {
	boom := errors.New("fetching message: [27] Group Load In Progress")
	deaths := consumerdeath.New()

	httpServer := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}
	t.Cleanup(func() { _ = httpServer.Close() })

	// A consumer stopped on request first: it must not end the process.
	stopped, stop := context.WithCancel(context.Background())
	stop()
	deaths.Watch(stopped, "stopped consumer", discardLogger(), func(context.Context) error { return boom })

	result := make(chan error, 1)
	go func() {
		result <- serveAndShutdown(discardLogger(), httpServer.Addr, httpServer, nil, context.Background(),
			func() {}, &inboundhttp.Readiness{}, nil, deaths)
	}()

	select {
	case err := <-result:
		t.Fatalf("serveAndShutdown returned %v for a clean consumer stop", err)
	case <-time.After(200 * time.Millisecond):
	}

	// The real catalogue consumer, started the way wiring.go starts it, on a
	// broker that fails its read.
	consumer := &kafkacatalog.Consumer{Reader: failingReader{err: boom}, Logger: discardLogger()}
	go deaths.Watch(context.Background(), "process-path catalogue consumer", discardLogger(), consumer.Run)

	select {
	case err := <-result:
		if !errors.Is(err, boom) {
			t.Fatalf("serveAndShutdown = %v, want it to wrap the consumer's error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a dead consumer did not end the process: serveAndShutdown kept serving")
	}
}

// A stop func that cancels a consumer's context is a requested stop, even if
// the consumer's Run reports the broker error it hit on the way out.
func TestWatch_AStopRequestedByTheServiceIsNotADeath(t *testing.T) {
	deaths := consumerdeath.New()
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &kafkacatalog.Consumer{Reader: ctxBlockedReader{}, Logger: discardLogger()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		deaths.Watch(ctx, "process-path catalogue consumer", discardLogger(), consumer.Run)
	}()
	cancel()
	<-done
	select {
	case err := <-deaths.C():
		t.Fatalf("a requested stop was reported as a death: %v", err)
	default:
	}
}

// ctxBlockedReader blocks until the context ends, then fails with a raw
// network error instead of context.Canceled.
type ctxBlockedReader struct{}

func (ctxBlockedReader) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	<-ctx.Done()
	return kafkago.Message{}, errors.New("read tcp: use of closed network connection")
}
func (ctxBlockedReader) Close() error { return nil }
