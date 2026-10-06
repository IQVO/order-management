package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
)

type orderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *orderLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *orderLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type fakeHTTPShutdown struct {
	onShutdown func()
}

func (f fakeHTTPShutdown) Shutdown(context.Context) error {
	f.onShutdown()
	return nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// ADR-0025 §8 / ADR-0022: readiness flips FIRST, the drain delay elapses
// BEFORE the listener closes, then HTTP, then the repromise consumer, and
// the outbox relay LAST (it must do its final pass only after every
// outbox writer has stopped).
func TestDrainUnderShutdown_OrderAndReadinessFlip(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	var log orderLog
	const delay = 60 * time.Millisecond
	start := time.Now()

	consumerDone := make(chan struct{})
	relayDone := make(chan struct{})

	httpServer := fakeHTTPShutdown{onShutdown: func() {
		if readiness.Ready() {
			t.Error("readiness must already be not-ready when the HTTP listener closes")
		}
		if elapsed := time.Since(start); elapsed < delay {
			t.Errorf("HTTP shutdown began after %v, before the %v drain delay elapsed", elapsed, delay)
		}
		log.add("http")
	}}

	err := drainUnderShutdown(discardLogger(), httpServer, readiness, delay,
		func() { log.add("relay"); close(relayDone) }, relayDone,
		nil, consumerDone, func() { log.add("consumer"); close(consumerDone) })
	if err != nil {
		t.Fatalf("drainUnderShutdown: %v", err)
	}

	got := log.get()
	want := []string{"http", "consumer", "relay"}
	if len(got) != len(want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("steps = %v, want %v (the relay must stop after every outbox writer)", got, want)
		}
	}
	if readiness.Ready() {
		t.Fatal("readiness must stay not-ready after shutdown")
	}
}

func TestDrainUnderShutdown_ZeroDelayDoesNotWait(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	relayDone := make(chan struct{})
	consumerDone := make(chan struct{})
	start := time.Now()

	err := drainUnderShutdown(discardLogger(), fakeHTTPShutdown{onShutdown: func() {}}, readiness, 0,
		func() { close(relayDone) }, relayDone,
		nil, consumerDone, func() { close(consumerDone) })
	if err != nil {
		t.Fatalf("drainUnderShutdown: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("zero drain delay still waited %v", time.Since(start))
	}
	if readiness.Ready() {
		t.Fatal("readiness must be flipped even with a zero drain delay")
	}
}

func TestDrainDelayFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		set  bool
		want time.Duration
	}{
		{"unset defaults to 5s", "", false, DefaultShutdownDrainDelay},
		{"explicit duration", "8s", true, 8 * time.Second},
		{"zero disables the delay", "0", true, 0},
		{"zero duration string disables the delay", "0s", true, 0},
		{"negative falls back", "-1s", true, DefaultShutdownDrainDelay},
		{"garbage falls back", "soon", true, DefaultShutdownDrainDelay},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv("SHUTDOWN_DRAIN_DELAY", tt.env)
			} else {
				t.Setenv("SHUTDOWN_DRAIN_DELAY", "")
			}
			if got := drainDelayFromEnv(discardLogger()); got != tt.want {
				t.Fatalf("drainDelayFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}
