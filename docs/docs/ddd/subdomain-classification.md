---
title: Subdomain Classification
sidebar_label: Subdomain Classification
description: Why Order Management is a Generic-leaning-Supporting subdomain, with the justification from the reference model.
---

# Subdomain Classification

## Verdict: **Generic/Supporting subdomain**, upstream front door

`CLAUDE.md`'s own title states the classification directly: *"Order
Management (Generic/Supporting Bounded Context — order intake, allocation,
release)."* `amazon-fulfillment-ddd.md`'s subdomain table places the
matching capability, **Order Management / ERP interface**, in the
**Generic** bucket — "upstream order intake; commodity integration
surface":

| Subdomain | Type | Why |
| --- | --- | --- |
| Fulfillment Orchestration & Optimization | Core | Continuous re-planning to the fastest/cheapest path is the differentiator. |
| Inventory & Slotting | Core | Random stow + bin-accurate tracking is a genuine operational innovation. |
| Picking | Core | Directly drives throughput and accuracy at scale. |
| **Order Management / ERP interface** | **Generic** | **Upstream order intake; commodity integration surface.** |
| Labor & Workforce Management | Supporting | Important, industry-common. |

## Generic-leaning-Supporting: why the softer label

`CLAUDE.md` labels this context **Generic/Supporting** rather than a pure
Generic, and that reflects a real distinction from the reference model's
literal "commodity integration surface" framing. Order intake itself —
accepting a well-formed request and validating it — genuinely is a
commodity concern; nothing about validating "SKU non-empty, quantity > 0"
differentiates a warehouse operator from a competitor.

But this context carries more than intake. **BR2 (fail-closed
allocation)**, **BR3 (ship-complete default)**, and **BR6 (the
cancellation boundary)** are real business rules with real invariants,
enforced in the domain and unit-tested per failing path — the same rigour
a Supporting subdomain gets elsewhere in this fleet (compare
`workforce-management`, also classified Supporting). That is why this
service earns real hexagonal architecture, a 90% domain/application
coverage gate, and dedicated ADRs rather than being treated as a thin CRUD
proxy.

## What "Generic/Supporting" buys and obliges

- **Build enough, not everything.** The orchestration rules (BR2/BR3/BR6)
  are implemented as explicit, tested domain code — they are not
  commodity. The promise stops at the building's door: a CPT window
  derived from fulfillment capability
  ([ADR 0014](/docs/adr/0014-promise-derived-from-fulfillment-capability)),
  with a configurable lead time as fallback — there is no live
  carrier-rate or transit-time integration, because that piece genuinely
  is commodity and does not differentiate this platform.
- **No local ownership of Supplier state.** Because this is not a Core
  subdomain claiming inventory or work-planning truth for itself, it holds
  only the references it needs (`ReservationId`) and calls out to the
  Core subdomains that actually own that truth.
- **Real quality bar.** 90% coverage on domain + application,
  failing-path tests per named invariant, a godog BDD suite, a gremlins
  mutation gate on the domain (thresholds in `.gremlins.yaml`, below the
  fleet's 99/99 target), and architecture fitness tests
  (`internal/architecture`) — see
  [Architecture](/docs/overview/architecture#quality-gates).

## Where the neighbours sit

| Service | Tier | Classification | Reasoning |
| --- | --- | --- | --- |
| **order-management** | upstream front door | **Generic/Supporting** | Order intake is commodity; the allocation/release/cancellation orchestration rules are real, tested business logic layered on top. |
| inventory-storage | WMS | Core | Owns inventory truth: bin-accurate location + usable inventory. |
| product-master | WMS | Supporting | Single source of truth for SKU master data (handling classification, declared vs measured dimensions and weight). This context reads its `ProductClassified` events into a local copy (ADR 0036). |
| wes-work-planning | WES | Core | The conductor — waveless release and flow balance. |
| fulfillment-execution | WES | Core | The Pick/Pack/SLAM task lifecycle; throughput and accuracy at scale. |
| workforce-management | — | Supporting | Labour & workforce allocation: "important, industry-common," not the differentiator. |
| facility-layout | — | Generic | Physical warehouse structure — extracted once rather than duplicated in every consumer. |

## Why this is not folded into an existing service

Order Management could, in principle, have been bolted onto
`fulfillment-execution` or `wes-work-planning` as "just another endpoint."
It is not, for the same reason `facility-layout` was extracted as its own
service rather than a package inside `inventory-storage`:
`warehouse-systems-ddd.md`'s discipline is to extract a concern into its
own bounded context once it has its own lifecycle and its own invariants,
rather than let it leak into a Core subdomain and force that subdomain to
change every time order-intake rules change.

Concretely: `Order` and `OrderLine` have a lifecycle (`Received` →
allocation → release → cancellation) that is genuinely distinct from a
`Reservation`'s lifecycle or a `WorkUnit`'s lifecycle, even though this
context calls both of those Suppliers to advance its own. Collapsing them
would mean a change to ship-complete policy could force a regression in
inventory-storage's reservation logic — exactly the coupling
`warehouse-systems-ddd.md` warns against.

## Same word, different model: allowed and expected

`warehouse-systems-ddd.md` calls this out as a discipline, not a bug, and
it applies directly here: `Status` on this context's `Order` and `Status`
on inventory-storage's `Reservation` are deliberately different state
machines on different aggregates. See the
[Ubiquitous Language](/docs/business-context/ubiquitous-language) page for
the full list of terms that mean something different across this
boundary. The practical enforcement is that this repository shares **no
Go types** with any sibling repository — integration happens entirely
through HTTP JSON and Kafka event payloads decoded into local structs,
never through an imported package (see
[ADR 0002](/docs/adr/0002-http-consumer-of-inventory-and-wes-not-shared-code)).
