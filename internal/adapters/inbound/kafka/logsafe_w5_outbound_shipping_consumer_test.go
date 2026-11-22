package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// crlfReader is a messageReader that commits everything and fetches nothing.
type crlfReader struct{}

func (crlfReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}
func (crlfReader) CommitMessages(context.Context, ...kafkago.Message) error { return nil }
func (crlfReader) Config() kafkago.ReaderConfig                             { return kafkago.ReaderConfig{Topic: "logsafe.topic"} }
func (crlfReader) Close() error                                             { return nil }

// TestOutboundShippingConsumerLogsNoEventDerivedLineBreaks feeds the consumer
// a payload that is not a CloudEvent and a ShipmentConfirmed whose id, type
// context and data carry CR/LF and whose data does not decode, so both are
// dead-lettered and logged: no log attribute may carry the line break.
func TestOutboundShippingConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &OutboundShippingConsumer{reader: crlfReader{}, logger: slog.New(rec)}
	ctx := context.Background()
	_ = c.handleMessage(ctx, kafkago.Message{Topic: "logsafe.topic", Value: []byte("not a CloudEvent" + crlfForged)})
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "id" + crlfForged, "type": ceTypeShipmentConfirmed, "source": "urn:logsafe",
		"subject": "subject" + crlfForged, "datacontenttype": "application/json",
		"data": []string{"not an object" + crlfForged},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.handleMessage(ctx, kafkago.Message{Topic: "logsafe.topic", Key: []byte("key" + crlfForged), Value: b})
	rec.assertNoLineBreaks(t, 2)
}
