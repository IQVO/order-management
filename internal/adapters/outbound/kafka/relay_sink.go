package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// RelaySink is the outbox relay's Sink: it wraps a *kafkago.Writer with NO
// fixed topic (AllowAutoTopicCreation, Hash balancer — same defaults every
// other writer in this package uses, see Publisher.NewWriterForTopic's doc
// comment on why Hash and not LeastBytes) and sets each message's Topic
// from its own Encoded.Topic before writing. kafka-go rejects a message
// that sets Topic when the Writer ALSO has one configured, and vice
// versa — which is exactly why Publisher's and AnalyticsPublisher's own
// writers keep their fixed topic and never set kafkago.Message.Topic
// themselves: only this relay-only writer routes per message.
type RelaySink struct {
	writer *kafkago.Writer
}

// NewRelaySink builds a RelaySink over a fresh writer addressed at
// brokers, with no topic of its own.
func NewRelaySink(brokers ...string) *RelaySink {
	return &RelaySink{writer: &kafkago.Writer{
		BatchTimeout:           syncWriterBatchTimeout,
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}}
}

// Send writes enc to its own Topic, keyed and headered exactly as it was
// encoded.
func (s *RelaySink) Send(ctx context.Context, enc Encoded) error {
	msg := kafkago.Message{Topic: enc.Topic, Key: enc.Key, Value: enc.Value, Headers: enc.Headers}
	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka: relay send %s to %s: %w", enc.EventType, enc.Topic, err)
	}
	return nil
}

// Close releases the underlying writer.
func (s *RelaySink) Close() error {
	return s.writer.Close()
}
