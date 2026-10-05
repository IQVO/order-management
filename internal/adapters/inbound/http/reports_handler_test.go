package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/analytics/report"
)

// stubStore is a report.ReportStore returning canned data so the handler can
// be exercised without a database.
type stubStore struct {
	rep      report.FunnelReport
	lag      time.Duration
	queryErr error
	lagErr   error
	gotQuery report.ReportQuery
}

func (s *stubStore) Query(_ context.Context, q report.ReportQuery) (report.FunnelReport, error) {
	s.gotQuery = q
	return s.rep, s.queryErr
}

func (s *stubStore) FreshnessLag(context.Context) (time.Duration, error) {
	return s.lag, s.lagErr
}

func TestReports_GetFunnel_OK(t *testing.T) {
	bucket := time.Date(2026, 6, 1, 14, 0, 0, 0, time.UTC)
	cutoff := bucket.Add(3 * time.Hour)
	store := &stubStore{rep: report.FunnelReport{Rows: []report.Row{{
		Key:            report.RowKey{PathId: "pick", HourBucket: bucket},
		OrdersReceived: 5, OrdersAllocated: 4, OrdersReleased: 3,
		OrdersCancelled: 1, LinesBackordered: 2,
		PromiseBasisCapability: 3, PromiseBasisLeadTime: 1, PromiseBasisNetwork: 2,
		OrdersSplitShipment: 1, OrdersRepromised: 0,
		PromiseToCutoffGapSeconds: cutoff.Sub(bucket).Seconds(),
	}}}}
	srv := inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: store}, nil, "")

	req := httptest.NewRequest(http.MethodGet, "/reports/funnel?from=2026-06-01T00:00:00Z&to=2026-06-02T00:00:00Z&pathId=pick", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	var body struct {
		Rows []struct {
			PathID                    string  `json:"pathId"`
			HourBucket                string  `json:"hourBucket"`
			OrdersReceived            int     `json:"ordersReceived"`
			OrdersReleased            int     `json:"ordersReleased"`
			OrdersCancelled           int     `json:"ordersCancelled"`
			LinesBackordered          int     `json:"linesBackordered"`
			PromiseBasisCapability    int     `json:"promiseBasisCapability"`
			PromiseBasisLeadTime      int     `json:"promiseBasisLeadTime"`
			PromiseBasisNetwork       int     `json:"promiseBasisNetwork"`
			OrdersSplitShipment       int     `json:"ordersSplitShipment"`
			OrdersRepromised          int     `json:"ordersRepromised"`
			PromiseToCutoffGapSeconds float64 `json:"promiseToCutoffGapSeconds"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(body.Rows))
	}
	row := body.Rows[0]
	gotFunnel := [4]int{row.OrdersReceived, row.OrdersReleased, row.OrdersCancelled, row.LinesBackordered}
	if row.PathID != "pick" || gotFunnel != [4]int{5, 3, 1, 2} {
		t.Errorf("row = %+v, unexpected values", row)
	}
	// ADR 0014 §6 / 0019 / 0020: promise basis distribution + split shipment.
	gotKPI := [4]int{row.PromiseBasisCapability, row.PromiseBasisLeadTime, row.PromiseBasisNetwork, row.OrdersSplitShipment}
	if gotKPI != [4]int{3, 1, 2, 1} || row.OrdersRepromised != 0 {
		t.Errorf("promise KPI fields = %+v, unexpected values", row)
	}
	if row.PromiseToCutoffGapSeconds != cutoff.Sub(bucket).Seconds() {
		t.Errorf("PromiseToCutoffGapSeconds = %v, want %v", row.PromiseToCutoffGapSeconds, cutoff.Sub(bucket).Seconds())
	}
	if store.gotQuery.PathId != "pick" {
		t.Errorf("query PathId = %q, want pick", store.gotQuery.PathId)
	}
}

func TestReports_GetFunnel_MissingParams(t *testing.T) {
	srv := inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: &stubStore{}}, nil, "")

	tests := []struct {
		name string
		url  string
	}{
		{"no from", "/reports/funnel?to=2026-06-02T00:00:00Z"},
		{"no to", "/reports/funnel?from=2026-06-01T00:00:00Z"},
		{"bad time", "/reports/funnel?from=nope&to=2026-06-02T00:00:00Z"},
		{"bad granularity", "/reports/funnel?from=2026-06-01T00:00:00Z&to=2026-06-02T00:00:00Z&granularity=day"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("content-type = %q, want application/problem+json", ct)
			}
		})
	}
}

func TestReports_GetFreshness_OK(t *testing.T) {
	store := &stubStore{lag: 90 * time.Second}
	srv := inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: store}, nil, "")

	req := httptest.NewRequest(http.MethodGet, "/reports/funnel/freshness", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		LagSeconds float64 `json:"lagSeconds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.LagSeconds != 90 {
		t.Errorf("lagSeconds = %v, want 90", body.LagSeconds)
	}
}

func TestReports_Healthz(t *testing.T) {
	srv := inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: &stubStore{}}, nil, "")
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
