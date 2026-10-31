---
paths:
  - "internal/domain/**"
  - "internal/application/**"
  - "README.md"
  - ".gremlins.yaml"
---
# Deferred and known gaps (document them, don't skip silently)

Several items originally listed as "v1 scope — explicitly deferred" have
since SHIPPED (Helm chart + CI packaging jobs, gremlins mutation gate,
godog/BDD acceptance suite, MCP inbound adapter, Kafka integration +
analytics fan-out, Postgres integration suite, arch-go fitness tests,
Spectral api-lint, docs-api-drift, standard metrics/telemetry, fleet MFE
console, FulfillmentClass classifier, capability-derived CPT promise with
per-shipment-group promising and the re-promise loop, network-originated
hold/release and deadline feasibility). CI and the README's "Deferred"
section are the current source of truth; check them, not this list, if you
need to confirm something has NOT shipped yet before treating it as future
work.

Still deferred, as of the last verified pass:

- **Real carrier-rate / transit-time promise.** The promise is a CPT
  window derived from fulfillment capability (ADR-0014; `LeadTimePolicy`
  remains the tagged fallback), i.e. the instant the order leaves the
  building — there is no live carrier or transit-time integration, and no
  such service exists in this fleet to call.
- **Per-site path capacity and cycle time (ADR-0047).** The promise and the
  planned-capacity annotation use the order's own `fulfillmentSiteId` (else
  `DEFAULT_SITE_ID` / `PLANNED_CAPACITY_SITE_ID`), but wes-work-planning's
  `PathCapacityChanged` and process-path-management's path catalogue carry no
  site, so remaining capacity (keyed `(pathId, cutoffAt)`) and `cycleTimeP95`
  are per path, fleet-wide. Needs a site on those upstream contracts first.
  Also deferred from ADR-0031: a path-level planned-capacity match (planning's
  `path_id` is its own label), pushing the promise on a shortage (a shortage
  is a quantity, not a delay) and an `OrderCapacityConstrained` event.
- **Sweeping an orphaned hold (ADR-0020).** Nothing here expires an order
  held with `releaseOnAllocation=false` that its caller never releases or
  cancels — it keeps real inventory reservations until someone does.
- **Kafka release-confirmation reply events from wes-work-planning.**
  v1 (ADR-0005) ships fire-and-forget: this service publishes
  `OrderAllocated`/`OrderPartiallyAllocated` and never learns whether
  `wes-work-planning`'s consumer actually processed the event or
  successfully enqueued its own work. Mirrors `inventory-storage`'s own
  ADR-0004 stance on transactional guarantees, not an oversight.
- **Clawing back released work on cancellation.** BR6 (ADR-0004): once any
  line is `Released`, v1 does NOT claw it back on `CancelOrder`.
- **Any change to `inventory-storage` or `wes-work-planning`.** This
  repo's entire build is 100% additive from their point of view — neither
  repository is ever touched from here.

Keep the README's own "Deferred" section as the authoritative, most
current list — this file is a pointer/summary, not a replacement for it.
