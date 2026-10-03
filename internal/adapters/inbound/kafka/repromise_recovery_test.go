package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// scriptedReader fails FetchMessage / CommitMessages a set number of times
// (a broker restart), then serves msgs, then blocks until ctx is done.
type scriptedReader struct {
	mu             sync.Mutex
	fetchFailures  int
	commitFailures int
	msgs           []kafkago.Message
	committed      []int64
	fetchCalls     int
	commitCalls    int
}

func (r *scriptedReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	r.fetchCalls++
	if r.fetchFailures > 0 {
		r.fetchFailures--
		r.mu.Unlock()
		return kafkago.Message{}, errors.New("fetching message: read tcp: i/o timeout")
	}
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *scriptedReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitCalls++
	if r.commitFailures > 0 {
		r.commitFailures--
		return errors.New("write tcp 127.0.0.1:57054->127.0.0.1:9092: use of closed network connection")
	}
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *scriptedReader) Config() kafkago.ReaderConfig {
	return kafkago.ReaderConfig{Topic: "warehouse.fulfillment.events"}
}
func (r *scriptedReader) Close() error { return nil }

func (r *scriptedReader) snapshot() (committed []int64, fetches, commits int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...), r.fetchCalls, r.commitCalls
}

// A non-CloudEvents payload is the cheapest message to drive end to end:
// it goes straight to (nil) DLQ + commit, so every failure the test sees
// is a broker-side one.
func poison(offset int64) kafkago.Message {
	return kafkago.Message{Offset: offset, Value: []byte(`{"not":"a cloudevent"}`)}
}

func runFor(t *testing.T, c *RepromiseConsumer, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return c.Run(ctx)
}

// Regression: a broker restart (failed fetches, then a failed commit with
// "use of closed network connection") used to make Run return the error,
// and main exited the whole order-management process.
func TestRepromiseConsumer_Run_SurvivesBrokerOutage(t *testing.T) {
	r := &scriptedReader{fetchFailures: 3, commitFailures: 2, msgs: []kafkago.Message{poison(10), poison(11)}}
	c := &RepromiseConsumer{reader: r}

	if err := runFor(t, c, 8*time.Second); err != nil {
		t.Fatalf("Run returned %v; it must only stop when ctx is done", err)
	}
	committed, fetches, commits := r.snapshot()
	if len(committed) != 2 || committed[0] != 10 || committed[1] != 11 {
		t.Fatalf("committed offsets %v, want [10 11] (the message whose commit failed must be retried, not skipped)", committed)
	}
	if fetches < 6 {
		t.Fatalf("fetch calls = %d, want >= 6 (3 failures + 2 messages + idle)", fetches)
	}
	if commits != 4 {
		t.Fatalf("commit calls = %d, want 4 (2 failed + 2 successful)", commits)
	}
}

func TestRepromiseConsumer_Run_StopsPromptlyOnCancelDuringBackoff(t *testing.T) {
	r := &scriptedReader{fetchFailures: 1_000_000}
	c := &RepromiseConsumer{reader: r}
	start := time.Now()
	if err := runFor(t, c, 300*time.Millisecond); err != nil {
		t.Fatalf("Run returned %v, want nil on cancel", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Run took %v to stop after cancel; backoff must observe ctx", time.Since(start))
	}
}

func TestNewRecoverBackoff_NeverGivesUp(t *testing.T) {
	b := newRecoverBackoff()
	for i := 0; i < 50; i++ {
		if d := b.NextBackOff(); d <= 0 || d > recoverMaxInterval*2 {
			t.Fatalf("attempt %d backoff %v, want (0, ~%v]", i, d, recoverMaxInterval)
		}
	}
}
