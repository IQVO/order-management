package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/order-management/internal/analytics/report"
)

// DefaultReportsServiceName labels the reports service in spans/metrics
// when the caller does not supply OTEL_SERVICE_NAME.
const DefaultReportsServiceName = "order-reports"

// ReportsHandlers is the inbound HTTP adapter for the order-management "Order
// Funnel & Allocation Health" data product's READER. It depends only on the
// read-model port (report.ReportStore); it never touches the OLTP use cases or
// the writer.
type ReportsHandlers struct {
	Store report.ReportStore
}

// funnelRowDTO is the wire shape of one report row. It is a dedicated DTO so
// the read-model struct (report.Row) never leaks onto the API.
//
// PromiseBasisCapability/PromiseBasisLeadTime/OrdersRepromised/
// OrdersSplitShipment/PromiseToCutoffGapSeconds are ADR 0014 §6 / ADR
// 0019's additive promise KPI fields. OrdersRepromised is NOT
// pathId-dimensioned (see report.Row.OrdersRepromised's doc comment) — it
// is only non-zero on the row whose pathId is "".
type funnelRowDTO struct {
	PathID                    string  `json:"pathId"`
	HourBucket                string  `json:"hourBucket"`
	OrdersReceived            int     `json:"ordersReceived"`
	OrdersAllocated           int     `json:"ordersAllocated"`
	OrdersPartiallyAllocated  int     `json:"ordersPartiallyAllocated"`
	OrdersAllocationFailed    int     `json:"ordersAllocationFailed"`
	OrdersReleased            int     `json:"ordersReleased"`
	OrdersCancelled           int     `json:"ordersCancelled"`
	LinesAllocated            int     `json:"linesAllocated"`
	LinesBackordered          int     `json:"linesBackordered"`
	LinesReleased             int     `json:"linesReleased"`
	PromiseBasisCapability    int     `json:"promiseBasisCapability"`
	PromiseBasisLeadTime      int     `json:"promiseBasisLeadTime"`
	PromiseBasisNetwork       int     `json:"promiseBasisNetwork"`
	OrdersRepromised          int     `json:"ordersRepromised"`
	OrdersSplitShipment       int     `json:"ordersSplitShipment"`
	PromiseToCutoffGapSeconds float64 `json:"promiseToCutoffGapSeconds"`
}

// funnelReportDTO is the wire shape of a funnel report response.
type funnelReportDTO struct {
	Rows []funnelRowDTO `json:"rows"`
}

// freshnessDTO is the wire shape of the freshness-lag response.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetFunnel serves GET /reports/funnel. from and to (RFC3339) are required;
// pathId and granularity are optional (granularity defaults to hour).
func (h *ReportsHandlers) GetFunnel(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}

	granularity := report.GranularityHour
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityHour) {
			writeReportBadRequest(w, r, "granularity must be 'hour'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		PathId:      q.Get("pathId"),
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	dto := funnelReportDTO{Rows: make([]funnelRowDTO, 0, len(rep.Rows))}
	for _, row := range rep.Rows {
		dto.Rows = append(dto.Rows, funnelRowDTO{
			PathID:                    row.Key.PathId,
			HourBucket:                row.Key.HourBucket.UTC().Format(time.RFC3339),
			OrdersReceived:            row.OrdersReceived,
			OrdersAllocated:           row.OrdersAllocated,
			OrdersPartiallyAllocated:  row.OrdersPartiallyAllocated,
			OrdersAllocationFailed:    row.OrdersAllocationFailed,
			OrdersReleased:            row.OrdersReleased,
			OrdersCancelled:           row.OrdersCancelled,
			LinesAllocated:            row.LinesAllocated,
			LinesBackordered:          row.LinesBackordered,
			LinesReleased:             row.LinesReleased,
			PromiseBasisCapability:    row.PromiseBasisCapability,
			PromiseBasisLeadTime:      row.PromiseBasisLeadTime,
			PromiseBasisNetwork:       row.PromiseBasisNetwork,
			OrdersRepromised:          row.OrdersRepromised,
			OrdersSplitShipment:       row.OrdersSplitShipment,
			PromiseToCutoffGapSeconds: row.PromiseToCutoffGapSeconds,
		})
	}
	writeJSON(w, http.StatusOK, dto)
}

// GetFreshness serves GET /reports/funnel/freshness.
func (h *ReportsHandlers) GetFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetReportsHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetReportsHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400 and
// returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest,
		problemInfo{"invalid-report-query", "The report query is malformed or missing a required parameter"},
		detail, r.URL.Path)
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError,
		problemInfo{"report-store-error", "The report could not be served"},
		err.Error(), r.URL.Path)
}

// NewReportsRouter builds the chi router for the order-reports reader service.
// A nil logger falls back to slog.Default(); an empty serviceName falls back
// to DefaultReportsServiceName.
//
// Every route, including /reports/*, is reachable with no Authorization
// header: the fleet-wide REST/MCP static-bearer auth layer has been
// removed.
//
// Like NewRouter, every request is traced (otelchi) and metered
// (otelchimetric: http.server.request.duration + http.server.active_requests)
// — ADR-0009 Tier 1 HTTP RED. The reports router previously had neither.
func NewReportsRouter(h *ReportsHandlers, logger *slog.Logger, serviceName string) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultReportsServiceName
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.GetReportsHealthz)

	r.Get("/reports/funnel", h.GetFunnel)
	r.Get("/reports/funnel/freshness", h.GetFreshness)

	return r
}
