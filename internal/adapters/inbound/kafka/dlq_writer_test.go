package kafka

import "testing"

// TestNewDLQWriter_AutoCreatesTheDeadLetterTopic pins the writer config
// the integration test proves end to end: a missing "<topic>.dlq" must be
// created on first publish instead of stopping the consumer.
func TestNewDLQWriter_AutoCreatesTheDeadLetterTopic(t *testing.T) {
	w := newDLQWriter([]string{"localhost:9092"}, "warehouse.fulfillment.events")
	t.Cleanup(func() { _ = w.Close() })
	if w.Topic != "warehouse.fulfillment.events.dlq" {
		t.Fatalf("topic = %q, want warehouse.fulfillment.events.dlq", w.Topic)
	}
	if !w.AllowAutoTopicCreation {
		t.Fatal("DLQ writer must set AllowAutoTopicCreation")
	}
}
