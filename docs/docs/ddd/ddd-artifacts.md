---
title: DDD Artifacts (ddd-crew)
sidebar_label: DDD Artifacts
sidebar_position: 0
description: "Index of the ddd-crew DDD artifact pack and UML diagrams for order-management, with the tool each follows and its sources of truth."
---

# DDD Artifacts (ddd-crew)

The strategic and tactical design of `order-management`, drawn as
Mermaid diagrams from the code on `develop`.

| Artifact | Follows | Page |
| --- | --- | --- |
| Core Domain Chart | [ddd-crew Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) | [Core Domain Chart](./core-domain-chart.md) |
| Bounded Context Canvas | [ddd-crew Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) | [Bounded Context Canvas](./bounded-context-canvas.md) |
| Context Map | [ddd-crew Context Mapping](https://github.com/ddd-crew/context-mapping) | [Context Map](/docs/ecosystem/context-map) (kept under Ecosystem) |
| Aggregate Design Canvas | [ddd-crew Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) | [Aggregate Design Canvas](./aggregate-design-canvas.md) |
| Domain Message Flow | [ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) | [Domain Message Flow](./domain-message-flow.md) |
| EventStorming | [ddd-crew EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) | [EventStorming](./eventstorming.md) |
| Ubiquitous Language | ddd-crew canvas glossary section | [Ubiquitous Language](/docs/business-context/ubiquitous-language) (kept under Business Context) |
| UML class diagrams | UML | [Class Diagrams](./class-diagram.md) |
| ER diagram | Crow's foot ER | [ER Diagram](./entity-relationship.md) |
| UML sequence diagrams | UML | [Sequence Diagrams](./sequence-diagrams.md) |
| Domain events | Event catalogue | [Domain Events](./domain-events.md) |

Related pages: [Subdomain Classification](./subdomain-classification.md),
[Use Cases](./use-cases.md).

## Sources of truth

- **Code wins.** Every diagram has a `Source:` line naming the files it was
  drawn from and an `Omits:` note. When a page disagrees with
  `internal/**`, `cmd/**` or `migrations/**`, the code is right and the
  page is stale.
- **Contracts:** `apis/openapi.yaml` (REST) and `apis/asyncapi.yaml`
  (Kafka, CloudEvents 1.0).
- **Decisions:** the [ADRs](/docs/adr). An ADR body records the
  decision at the time; later ADRs that supersede or extend it are noted in
  its Status line.
- **Estimates are labelled.** Throughput, size and chart coordinates are
  judgement, not measurement.
