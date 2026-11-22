package postgres

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
)

// crlfTx is a pgx.Tx whose failure-recording UPDATE answers the given
// attempts count and whose commit succeeds; nothing else is called.
type crlfTx struct {
	pgx.Tx
	attempts int
}

func (t crlfTx) QueryRow(context.Context, string, ...any) pgx.Row { return crlfRow{n: t.attempts} }
func (crlfTx) Commit(context.Context) error                       { return nil }

type crlfRow struct{ n int }

func (r crlfRow) Scan(dest ...any) error {
	*(dest[0].(*int)) = r.n
	return nil
}

// TestRelayFailureLogsNoEventDerivedLineBreaks: the attempt-cap log of the
// outbox relay must carry no line break from the row's event type and key or
// from the send error.
func TestRelayFailureLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	r := &OutboxRelay{logger: slog.New(rec), maxAttempts: 3}
	_ = r.recordSendFailure(context.Background(), crlfTx{attempts: 3}, pendingRow{id: 1, enc: outboundkafka.Encoded{Topic: "logsafe.topic", EventType: "logsafe.Evil" + crlfForged, Key: []byte("key" + crlfForged)}}, errors.New("broker refused"+crlfForged))
	rec.assertNoLineBreaks(t, 1)
}
