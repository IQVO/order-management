package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/resilience"
)

// wireInventoryClient wires the inventory reservation client plus its
// circuit-breaker gauge. circuitBreakerMetrics wires the breaker's
// OnStateChange into the circuit_breaker.state gauge (ADR-0025), reusing
// the SAME OTel MeterProvider telemetry.Setup already installed rather than
// standing up a second Prometheus registry. Errors here mirror
// NewOrderMetrics' contract (invalid instrument name only, a programming
// error) -- non-fatal: a nil recorder just means this process runs without
// the gauge, never without the breaker itself.
//
// The product-classification lookup is no longer an outbound HTTP client:
// since ADR 0036 it reads a local copy (see product_classification.go).
func wireInventoryClient(logger *slog.Logger) ports.InventoryReservationClient {
	circuitBreakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics unavailable; breakers will run without the circuit_breaker.state gauge", "error", err)
	}
	return buildInventoryClient(getenv("INVENTORY_STORAGE_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), circuitBreakerMetrics, logger)
}

// wireCatalogue resolves the process-path catalogue and its two sibling
// caches from the PATH_CATALOGUE_SOURCE switch (defaulting to "none":
// validation skipped -- a nil ports.ProcessPathCatalogue is this fleet's
// established "not yet wired" convention, see ReceiveOrder's doc
// comment). Set PATH_CATALOGUE_SOURCE=kafka for a real deployment,
// mirroring wes-work-planning/fulfillment-execution/workforce-management's
// identical convention. Unlike those three services, order-management
// never had a file-based catalogue to begin with (see ADR-0013's scope
// decision), so there is no "file" mode here -- only "none" (skip) and
// "kafka" (real validation).
//
// ADR-0014 step A extends this SAME switch: the CPT schedule cache
// (kafkacptschedule) is a SEPARATE consumer instance on the SAME
// topic/broker as the catalogue, so it is gated identically -- it only
// runs when the catalogue does, since both need KAFKA_BROKERS.
//
// ADR-0015 extends it a THIRD time: the path capacity cache
// (kafkapathcapacity) is yet another separate consumer instance, on a
// DIFFERENT topic (wes-work-planning's warehouse.work-planning.events,
// not process-path-management's), but the SAME broker and the SAME
// PATH_CATALOGUE_SOURCE switch -- there is no new env knob for an
// operator to learn. When the switch is "none" (or unset),
// ports.PathCapacity stays wired to UnknownPathCapacity, exactly today's
// behaviour.
//
// wireProcessPathCatalogue also retries every boot-time Kafka dial it
// makes (each NewConsumer call's newTargetOffsets dials the broker
// directly before the real reader exists) with exponential backoff,
// mirroring network-fulfillment PR #7 — this cluster resets every
// injected pod's first outbound dial ~10s after start (Istio native
// sidecars; holdApplicationUntilProxyStarts is a no-op for them), and a
// single attempt turns that transient condition into CrashLoopBackOff.
// See wiring.go's doc comment for the full detail.
func wireCatalogue(ctx context.Context, logger *slog.Logger) (ports.ProcessPathCatalogue, ports.CPTScheduleCache, ports.PathCapacity, func(), error) {
	return wireProcessPathCatalogue(ctx, getenv("PATH_CATALOGUE_SOURCE", "none"), os.Getenv("KAFKA_BROKERS"), logger)
}

// buildPromisePolicy assembles the ADR-0014 promise policy: the primary
// Schedule/Capability/Capacity trio (nil, and therefore unused, when the
// Kafka catalogue source is not configured) over the leadTime fallback,
// unchanged in its own logic.
func buildPromisePolicy(catalogue ports.ProcessPathCatalogue, cptSchedule ports.CPTScheduleCache, capacity ports.PathCapacity, logger *slog.Logger) order.PromisePolicy {
	leadTime := order.NewLeadTimePolicy(
		durationEnv("PROMISE_DEFAULT_LEAD_TIME", order.DefaultLeadTime, logger),
		perPathLeadTimes(os.Getenv("PROMISE_PATH_LEAD_TIMES"), logger),
	)
	return order.PromisePolicy{
		Schedule:   cptSchedule,
		Capability: catalogue,
		Capacity:   capacity,
		Fallback:   leadTime,
		SiteId:     getenv("DEFAULT_SITE_ID", DefaultSiteId),
	}
}

// buildInventoryClient selects the outbound InventoryReservationClient via
// INVENTORY_STORAGE_MODE (http|permissive), defaulting to "permissive" so
// unit tests and CI never reach the network. Permissive does NOT mean
// fail-open: it refuses to allocate rather than fabricating a reservation.
//
// In http mode the real Client is wrapped in a per-dependency circuit
// breaker (ADR-0025): on a trip, calls fall back to the SAME permissive
// (fail-loud) behaviour this client already had, rather than a new
// fallback path. recorder feeds the breaker's state transitions into the
// circuit_breaker.state gauge; nil is fine (see
// resilience.RecordStateChange's doc comment).
func buildInventoryClient(mode, baseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.InventoryReservationClient {
	if !strings.EqualFold(mode, "http") {
		logger.Warn("inventory-storage client in permissive (no-op) mode; allocation will refuse to run",
			"hint", "set INVENTORY_STORAGE_MODE=http and INVENTORY_STORAGE_BASE_URL for a real deployment")
		return inventorystorage.NewPermissiveClient()
	}
	logger.Info("inventory-storage client configured", "mode", "http", "base_url", baseURL, "circuit_breaker", "enabled")
	return inventorystorage.NewBreakerClient(inventorystorage.NewClient(baseURL, nil), recorder)
}
