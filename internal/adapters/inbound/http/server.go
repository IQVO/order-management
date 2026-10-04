package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// DefaultServiceName labels this service in logs when the caller does not
// supply one.
const DefaultServiceName = "order-management"

// Server holds every use case the HTTP adapter depends on.
type Server struct {
	ReceiveOrder    *usecases.ReceiveOrder
	RetryAllocation *usecases.RetryAllocation
	ReleaseHeld     *usecases.ReleaseHeldOrder
	CancelOrder     *usecases.CancelOrder
	GetOrder        *usecases.GetOrder
	// CapacityConstraints annotates order responses with the published
	// warehouse-planning shortage windows their promise overlaps (ADR 0031).
	// Optional: nil (every pre-existing caller/test, and any deployment with
	// no planned-capacity read model) means no annotation.
	CapacityConstraints *usecases.OrderCapacityConstraints
	// PlannedCapacity backs GET /planned-capacity (ADR 0031). Optional: nil
	// leaves the route unregistered (404), as before.
	PlannedCapacity *usecases.GetPlannedCapacity
	// IdempotencyPool, when non-nil, wires RequireIdempotencyKey onto
	// POST /orders (see idempotency.go). A nil pool means "no
	// transactional Postgres backing wired" (in-memory dev/test
	// configuration) — the idempotency middleware needs a real
	// pgxpool.Pool to begin its own transaction, so it is simply not
	// applied in that case, exactly this codebase's existing convention
	// for every other optional Postgres-backed capability (UnitOfWork,
	// the outbox relay).
	IdempotencyPool *pgxpool.Pool
	// Readiness backs GET /readyz (ADR-0025 §graceful shutdown). A nil
	// Readiness (the zero value, and every pre-existing caller/test)
	// means /readyz always reports ready — see Readiness's own doc
	// comment.
	Readiness *Readiness
}

// NewRouter builds the chi router for every endpoint in CLAUDE.md's REST
// API. A nil logger defaults to slog.Default(); an empty serviceName
// defaults to DefaultServiceName.
//
// Every route, including every mutating one, is reachable with no
// Authorization header: the fleet-wide REST/MCP static-bearer auth layer
// has been removed (see the ADR recorded alongside this change).
//
// Middleware order matters here: otelchi runs before RequestLogger so the
// request context already carries a span by the time a line is logged,
// and otelchimetric's duration histogram is wired right after it, mirroring
// inventory-storage's NewRouter and the fleet-standard-metrics ADR's Tier 1
// HTTP RED requirement exactly. WithChiRoutes resolves the route pattern up
// front, so spans/metrics are labeled "/orders/{id}" rather than one
// distinct series per order id.
func NewRouter(s *Server, logger *slog.Logger, serviceName string) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultServiceName
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions; no hand-rolled histogram needed.
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware())

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	// POST /orders is route-scoped (r.With, not r.Use) behind
	// RequireIdempotencyKey — it is the one mutating endpoint that
	// creates a NEW resource with a server-generated id, so a lost
	// response and a client retry would otherwise create a duplicate
	// order. The other mutating routes act on a caller-supplied {id}
	// and are lower priority for v1 (see the ADR). IdempotencyPool nil
	// (in-memory dev/test configuration, no transactional Postgres
	// backing) skips the middleware entirely, mirroring every other
	// optional Postgres-backed capability's nil convention in this repo.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/orders", s.handleReceiveOrder)
	} else {
		r.Post("/orders", s.handleReceiveOrder)
	}
	r.Get("/orders/{id}", s.handleGetOrder)
	r.Post("/orders/{id}/retry-allocation", s.handleRetryAllocation)
	r.Post("/orders/{id}/release", s.handleReleaseHeldOrder)
	r.Delete("/orders/{id}", s.handleCancelOrder)
	if s.PlannedCapacity != nil {
		r.Get("/planned-capacity", s.handleGetPlannedCapacity)
	}

	return r
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReceiveOrder(w http.ResponseWriter, r *http.Request) {
	var req receiveOrderRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Lines) == 0 {
		writeError(w, r, order.ErrNoLines)
		return
	}

	lines := make([]usecases.NewLine, 0, len(req.Lines))
	for _, l := range req.Lines {
		sku, err := shared.NewSKU(l.SKU)
		if err != nil {
			writeError(w, r, err)
			return
		}
		// PathId is never caller-supplied on this public intake DTO — see
		// receiveOrderLineRequest's doc comment. Leave it empty here so
		// ReceiveOrder's PathPolicy resolves it as a real domain policy
		// decision (and validates the result against the live
		// process-path catalogue) instead of this adapter pre-baking a
		// default before the use case ever sees the line.
		lines = append(lines, usecases.NewLine{
			SKU: sku, Quantity: l.Quantity, GiftWrap: l.GiftWrap,
		})
	}

	releaseOnAllocation := true
	if req.ReleaseOnAllocation != nil {
		releaseOnAllocation = *req.ReleaseOnAllocation
	}
	o, err := s.ReceiveOrder.ExecuteWithDeadline(r.Context(), lines, req.AllowPartialShipment, releaseOnAllocation, req.RequiredShipBy)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/orders/"+o.ID().String())
	writeJSON(w, http.StatusCreated, s.orderResponse(r.Context(), o))
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDParam(w, r)
	if !ok {
		return
	}
	o, err := s.GetOrder.Execute(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, s.orderResponse(r.Context(), o))
}

func (s *Server) handleRetryAllocation(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDParam(w, r)
	if !ok {
		return
	}
	o, err := s.RetryAllocation.Execute(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, s.orderResponse(r.Context(), o))
}

// handleReleaseHeldOrder commits an order held at intake
// (releaseOnAllocation=false) to the floor. Idempotent: releasing an
// already-released order returns 200, because the caller may be retrying
// a lost response and must not be punished for it (ADR 0020 §1).
func (s *Server) handleReleaseHeldOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDParam(w, r)
	if !ok {
		return
	}
	o, err := s.ReleaseHeld.Execute(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, s.orderResponse(r.Context(), o))
}

func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDParam(w, r)
	if !ok {
		return
	}
	if _, err := s.CancelOrder.Execute(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// orderResponse builds the order DTO and, when a planned-capacity read model
// is wired, annotates it with the capacity constraint (ADR 0031). A failing
// read model is logged and omitted: an order read must never fail because an
// advisory annotation is unavailable.
func (s *Server) orderResponse(ctx context.Context, o *order.Order) orderResponse {
	resp := toOrderResponse(o)
	if s.CapacityConstraints == nil {
		return resp
	}
	windows, err := s.CapacityConstraints.For(ctx, o)
	if err != nil {
		slog.WarnContext(ctx, "capacity constraint lookup failed; omitting annotation",
			"order_id", o.ID().String(), "error", err)
		return resp
	}
	if len(windows) == 0 {
		return resp
	}
	brief := make([]plannedCapacityWindowBrief, len(windows))
	for i, w := range windows {
		brief[i] = plannedCapacityWindowBrief{
			PlanID: w.PlanID, WindowStart: w.Start.UTC().Format(timeFormat), WindowEnd: w.End.UTC().Format(timeFormat),
			Shortage: w.Shortage, BottleneckStep: w.BottleneckStep,
		}
	}
	resp.CapacityConstraint = &capacityConstraintResponse{Constrained: true, Site: s.CapacityConstraints.SiteID, Windows: brief}
	return resp
}

// handleGetPlannedCapacity serves the local planned-capacity read model for
// one site: GET /planned-capacity?site=SIM1[&from=RFC3339]. It lists windows
// of any status ending after `from` (default: now), so an operator can see
// drafts as well as the published shortages that annotate orders.
func (s *Server) handleGetPlannedCapacity(w http.ResponseWriter, r *http.Request) {
	site := r.URL.Query().Get("site")
	if site == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-query-parameter", "A required query parameter is missing or invalid"},
			"query parameter \"site\" is required", r.URL.Path)
		return
	}
	var from *time.Time
	if r.URL.Query().Has("from") {
		// Presence, not non-emptiness: `from=` is an invalid timestamp (400),
		// never a silent "now".
		t, err := time.Parse(timeFormat, r.URL.Query().Get("from"))
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-query-parameter", "A required query parameter is missing or invalid"},
				"query parameter \"from\" must be an RFC 3339 timestamp", r.URL.Path)
			return
		}
		from = &t
	}
	windows, err := s.PlannedCapacity.Execute(r.Context(), site, from)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]plannedCapacityWindowResponse, len(windows))
	for i, c := range windows {
		out[i] = plannedCapacityWindowResponse{
			PlanID: c.PlanID, WarehouseID: c.WarehouseID, Location: c.Location, PathID: c.PathID,
			WindowStart: c.Start.UTC().Format(timeFormat), WindowEnd: c.End.UTC().Format(timeFormat),
			AssignedDemand: c.AssignedDemand, CapacityOverWindow: c.CapacityOverWindow, Shortage: c.Shortage,
			BottleneckStep: c.BottleneckStep, Status: string(c.Status), AsOf: c.AsOf.UTC().Format(timeFormat),
		}
	}
	writeJSON(w, http.StatusOK, plannedCapacityResponse{Site: site, Windows: out})
}

// orderIDParam extracts and validates the {id} path parameter, writing the
// problem response itself when it is not a valid OrderId.
func orderIDParam(w http.ResponseWriter, r *http.Request) (shared.OrderId, bool) {
	id, err := shared.NewOrderId(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return "", false
	}
	return id, true
}

const timeFormat = time.RFC3339

func toOrderResponse(o *order.Order) orderResponse {
	lines := make([]orderLineResponse, 0, len(o.Lines()))
	for _, l := range o.Lines() {
		lines = append(lines, orderLineResponse{
			LineNo:        l.LineNo(),
			SKU:           l.SKU().String(),
			Quantity:      l.Quantity(),
			PathID:        l.PathID().String(),
			GiftWrap:      l.GiftWrap(),
			Status:        string(l.Status()),
			ReservationID: l.ReservationID(),
		})
	}

	var promiseDate *string
	if d := o.PromiseDate(); d != nil {
		formatted := d.UTC().Format(timeFormat)
		promiseDate = &formatted
	}

	return orderResponse{
		ID:                   o.ID().String(),
		Status:               string(o.Status()),
		AllowPartialShipment: o.AllowPartialShipment(),
		ReleaseOnAllocation:  o.ReleaseOnAllocation(),
		RequiredShipBy:       o.RequiredShipBy(),
		PromiseDate:          promiseDate,
		Lines:                lines,
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dest any) bool {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-request-body", "The request body is not valid JSON"}, err.Error(), r.URL.Path)
		return false
	}
	return true
}

// writeError writes a domain/application error as an RFC 7807
// (application/problem+json) response.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, statusFor(err), problemFor(err), err.Error(), r.URL.Path)
}

func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetails{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// corsMiddleware allows the warehouse-console browser SPA (and this
// service's own future MFE remote dev origin) to call this API directly
// from the browser. Static-bearer-key auth, not cookies, so credentials
// are never needed here. CORS_ALLOWED_ORIGINS overrides the local-dev
// default (comma-separated) for staging/prod deployments.
func corsMiddleware() func(http.Handler) http.Handler {
	origins := []string{"http://localhost:5173", "http://localhost:5181"}
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		origins = strings.Split(v, ",")
	}
	return cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: false,
		MaxAge:           300,
	})
}
