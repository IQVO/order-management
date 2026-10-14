---
id: 0042-path-selection-honours-process-path-intake
slug: /adr/0042-path-selection-honours-process-path-intake
title: "0042. Path selection considers only paths whose intake admits customer orders"
sidebar_label: "42. Path selection honours intake"
sidebar_position: 42
description: "ADR 0042 — the process-path catalogue cache decodes process-path-management's additive intake (its ADR 0034) and order.PathSelectionPolicy considers only active paths whose intake contains CUSTOMER_ORDER. When no path declares it (catalogue not seeded, or an older producer) the policy falls back to shared.DefaultPathId, never to the lowest path id. Fixes customer order lines tying across PACK/PICK/REBIN/SLAM and resolving to PACK. Amends ADR 0021."
---

# 0042. Path selection considers only paths whose intake admits customer orders

## Status

**Accepted** (2026-10-14). Decision E5 of the 2026-10-13 decision log.
Amends [ADR 0021](./0021-multi-path-attribute-driven-routing.md) (which
paths are candidates); [ADR 0013](./0013-process-path-selection-as-a-domain-policy.md)'s
fail-open floor is unchanged. Consumer side of process-path-management ADR
0034 "A process path declares its intake".

## Context

ADR 0021 made `order.PathSelectionPolicy` enumerate **every** active path
from `ports.ProcessPathCatalogue.ListActive`, keep the ones whose
`Eligibility` admits the line, and pick the shortest known `CycleTimeP95`,
ties to the lower `PathId`.

`ListActive` returns the whole process-path catalogue, not only order entry
points. The live catalogue holds PACK, PICK, REBIN and SLAM, all at 2h with
permissive eligibility. Every one admits every line and they tie, so a
customer order line resolves to **PACK**, a downstream stage. No order lines
have flowed yet, so nothing has been misrouted. More paths make it worse:
RELOCATE (slot-plan moves) and process-path-management's ADR 0008 families
(`AMNESTY` sorts before `PICK`) would also be candidates. The first draft of
RELOCATE hid itself behind a fake `slot-relocation` eligibility attribute,
which encodes routing policy as a product attribute.

The root cause is that the catalogue never said which demand may enter a
path. process-path-management ADR 0034 adds that concept: every
`ProcessPathCreated`/`ProcessPathUpdated` carries `intake`, the set of demand
sources allowed to route work onto the path (`CUSTOMER_ORDER`,
`STOCK_TRANSFER`, `SLOT_RELOCATION`, `INVENTORY_CONTROL`). Its declared
catalogue gives PICK `["CUSTOMER_ORDER"]`, PACK/REBIN/SLAM `[]` and RELOCATE
`["SLOT_RELOCATION"]`.

## Decision

1. **Decode.** `kafkacatalog.Consumer` decodes `intake` onto
   `processpath.PathDefinition.Intake` and `ListActive` carries it on
   `shared.ActivePathCandidate.Intake`. An event without `intake` (a producer
   older than ADR 0034) decodes as the empty intake. Values are kept as
   strings in `shared.DemandSource`, an open type: this context only ever asks
   about `CUSTOMER_ORDER` and tolerates values it does not know.
2. **Candidates.** `PathSelectionPolicy.Select` keeps only the candidates
   whose intake contains `shared.DemandSourceCustomerOrder`. Eligibility and
   ranking (shortest known cycle time, ties to the lower `PathId`) are
   unchanged, but they run only among those candidates. A faster path with
   another intake never wins.
3. **Fail open to the default, never to the lowest id.** When no active path
   declares `CUSTOMER_ORDER` (the catalogue has not been seeded with ADR
   0034's intake yet, or the producer predates it), `Select` returns
   `shared.DefaultPathId` with `ok=true`, exactly like a nil or empty
   catalogue today (ADR 0013's floor). The line is then validated against
   the catalogue as before.
4. **Rejection is unchanged.** When at least one `CUSTOMER_ORDER` path exists
   but none admits the line, `Select` returns `ok=false` and `ReceiveOrder`
   answers `422 line-ineligible-for-resolved-path`. Intake narrows the
   candidate set. It never turns an eligibility rejection into the default.

No contract of this service changes: no new endpoint, event, column or
configuration. The input contract change is additive and owned by
process-path-management.

## Consequences

**Easier**

- The PACK-wins tie is gone, and so is its root cause. Any path that does not
  declare `CUSTOMER_ORDER` (downstream stages, RELOCATE, inventory-control
  families) is never an order entry point, whatever its cycle time or id.
- Rollout order does not matter. Before process-path-management ships ADR 0034,
  or before its seed runs, no path declares intake and every line falls back
  to `pick`, which is the intended behaviour, not a misroute.

**Harder**

- An operator who wants a second order entry point (a SINGLES path, say) must
  declare `CUSTOMER_ORDER` in that path's intake in process-path-management.
  Eligibility alone no longer makes a path selectable.
- Until the catalogue is seeded, eligibility rules on the real PICK path are
  not consulted by selection (the default is returned). ReceiveOrder's
  catalogue validation of the resolved path still applies.

## Tests

- Domain (`internal/domain/order/path_selection_intake_test.go`): the E5
  regression (PACK/PICK/REBIN/SLAM at 2h, permissive, no intake ->
  `DefaultPathId`, not PACK), only-CUSTOMER_ORDER candidates, ranking among
  them only, declared-but-ineligible rejects, unknown sources ignored. All of
  them failed against the pre-change policy.
- Use case (`internal/application/usecases/path_selection_intake_test.go`):
  the same rule through `ReceiveOrder`.
- Adapter (`internal/adapters/outbound/kafkacatalog/intake_test.go`): decode,
  absent -> empty, Updated replaces it, defensive copy; plus
  `intake_integration_test.go`, which runs a real Kafka broker
  (testcontainers), feeds the replayed consumer to the policy, and checks both
  a seeded catalogue (-> PICK) and a pre-ADR-0034 one (-> `pick`, never PACK).
