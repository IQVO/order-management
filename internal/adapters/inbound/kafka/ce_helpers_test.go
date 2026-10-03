package kafka_test

import (
	"encoding/json"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
)

// ceSpec describes one CloudEvents 1.0 test message, built with the
// official SDK so tests exercise the exact wire format producers emit.
type ceSpec struct {
	id, source, ceType, subject, dataschema string
	at                                      time.Time
}

func ceEvent(t *testing.T, s ceSpec, data any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(s.id)
	e.SetSource(s.source)
	e.SetType(s.ceType)
	e.SetSubject(s.subject)
	e.SetTime(s.at)
	if s.dataschema != "" {
		e.SetDataSchema(s.dataschema)
	}
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// legacyFlatMessage is the retired pre-CloudEvents flat envelope; every
// consumer must reject it.
func legacyFlatMessage(eventID, eventType, data string) []byte {
	return []byte(`{"event_id":"` + eventID + `","event_type":"` + eventType + `","occurred_at":"2026-09-01T00:00:00Z","source":"legacy","data":` + data + `}`)
}
