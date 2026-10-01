package kafkacatalog

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// TestReaderConfig_AsyncCommitForReplay pins the replay reader's config:
// a zero CommitInterval makes kafka-go commit synchronously after every
// ReadMessage, which made boot replay too slow to fit WaitReadyTimeout.
func TestReaderConfig_AsyncCommitForReplay(t *testing.T) {
	brokers := []string{"broker-a:9092", "broker-b:9092"}
	cfg := readerConfig(brokers, "some.topic", "group-x")

	if cfg.CommitInterval <= 0 {
		t.Fatalf("CommitInterval = %v, want > 0 (async periodic commits)", cfg.CommitInterval)
	}
	if cfg.CommitInterval != replayCommitInterval {
		t.Errorf("CommitInterval = %v, want %v", cfg.CommitInterval, replayCommitInterval)
	}
	if cfg.StartOffset != kafkago.FirstOffset {
		t.Errorf("StartOffset = %d, want FirstOffset (%d)", cfg.StartOffset, kafkago.FirstOffset)
	}
	if cfg.GroupID != "group-x" {
		t.Errorf("GroupID = %q, want %q", cfg.GroupID, "group-x")
	}
	if cfg.Topic != "some.topic" {
		t.Errorf("Topic = %q, want %q", cfg.Topic, "some.topic")
	}
	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "broker-a:9092" || cfg.Brokers[1] != "broker-b:9092" {
		t.Errorf("Brokers = %v, want %v", cfg.Brokers, brokers)
	}
}

// TestReaderConfig_UniqueGroupIsAccepted checks the config built from the
// production group helper is a valid kafka-go reader config (Validate is
// what NewReader panics on).
func TestReaderConfig_UniqueGroupIsAccepted(t *testing.T) {
	cfg := readerConfig([]string{"localhost:1"}, "some.topic", uniqueConsumerGroup())
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
	if cfg.GroupID == "" {
		t.Fatal("GroupID is empty, want a process-unique group")
	}
}
