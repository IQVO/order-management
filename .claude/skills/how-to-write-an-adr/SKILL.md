---
name: how-to-write-an-adr
description: Write an Architecture Decision Record in this repo's numbering and format, including companion ADRs for cross-repo changes. Use when a design decision should be recorded or a change contradicts an existing ADR.
---

# How to write an ADR

Use when a change is architecturally significant — a new bounded-context
integration, a reversal of a prior decision, a cross-repo contract change,
or anything a future reader would otherwise have to reverse-engineer from
the diff. Not every change needs one: a bug fix or a routine feature
addition inside an already-decided architecture doesn't.

## Numbering and location

`docs/docs/adr/NNNN-kebab-case-title.md`, four-digit zero-padded,
sequential — check the highest existing number
(`git ls-tree --name-only origin/develop -- docs/docs/adr/` and pick the
next integer, never reuse or guess). As of this writing the highest is
`0020-network-originated-demand-hold-and-deadline-feasibility.md`, so the
next ADR is `0021`.
`docs/docs/adr/about.md` explains the format to readers; you don't need to
touch it when adding a new ADR.

## Frontmatter (Docusaurus needs all five fields)

```yaml
---
id: NNNN-kebab-case-title
slug: /adr/NNNN-kebab-case-title
title: "NN. Title (a short noun phrase, matching the heading)"
sidebar_label: "NN. Short label for the nav sidebar"
sidebar_position: NN
description: "One or two sentences — this shows up in search and link
  previews, so make it stand alone without the rest of the doc."
---
```

`id`/`slug` are the full kebab-case filename (minus `.md`); `title`/
`sidebar_label` repeat the number as plain text (`"15. ..."`, not `#15`);
`sidebar_position` is the bare integer. Getting these inconsistent is the
most common cause of a broken sidebar entry or 404 after merge — verify
by running the docs build (see below) before opening the PR.

## Format: Michael Nygard's template

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play — technical, business, constraints — that make this
decision necessary. Write in the past tense, as if explaining to someone
who wasn't there. State the alternatives seriously considered, not just
the one chosen; a reader six months from now needs to know a simpler
option was weighed and rejected, not assume nobody thought of it.

## Decision
What was actually decided, stated as an active, present-tense
declaration ("we will...", not "we might..."). Be specific about the
mechanism, not just the intent — this section should let a reader
implement the same decision from scratch without asking follow-up
questions.

## Consequences
What becomes easier, what becomes harder, and what future work this
creates or forecloses. Be honest about the downsides — an ADR that only
lists benefits reads as marketing, not a decision record.
```

The `## Decision` section is the part worth the most editing effort: see
ADR-0015 (`docs/docs/adr/0015-wes-work-planning-path-capacity-changed-wired.md`)
for a model example — it states the exact mechanism (widening
`ports.PathCapacity.Remaining` to accept `cutoffAt`, a third independent
Kafka consumer on a new topic, exact-match correlation on `(PathId,
CutoffAt)`), names the specific alternatives it rejected and why (a
tolerance-window match, resolving `cptId -> cutoffAt` inside the adapter
instead of widening the port), and is specific enough that this repo's
own `how-to-add-an-integration-event.md` consumer-group guidance can
point straight at its three-consumers-on-two-topics implementation.

## Superseding an earlier ADR

Don't edit the old ADR's Decision section. Add a `## Status` line noting
`Superseded by ADR-XXXX` on the OLD one (a one-line patch), and open the
new ADR referencing it. This repo has a real example: ADR-0012
(`docs/docs/adr/0012-remove-rest-mcp-bearer-auth.md`) is
`**Accepted.** ... **Supersedes ADR 0011**`, and ADR-0011
(`docs/docs/adr/0011-adopt-fleet-rest-identity.md`) itself documents that
its static-bearer identity layer was later removed — read both together
for the exact wording pattern one ADR uses to supersede another.

## Cross-repo decisions: use a companion ADR, not one repo's private opinion

When a decision genuinely spans two bounded-context repos, write ONE ADR
per repo, each referencing the other explicitly as "the companion ADR"
with a one-line description of the split of responsibility. This repo has
a real instance: ADR-0015 here
(`docs/docs/adr/0015-wes-work-planning-path-capacity-changed-wired.md`)
is the order-management half of wes-work-planning's own ADR-0018
(`wes-work-planning/docs/docs/adr/0018-path-capacity-changed.md`,
"PathCapacityChanged") — this repo's ADR explicitly names wes-work-planning's
PR (#69) and ADR number as the upstream decision it is wiring against,
rather than re-deriving the wire contract from scratch. Don't write the
decision once in one repo and expect the other repo's readers to find
it; each bounded context's docs site is read independently.

## After writing: regenerate and verify the docs build

```bash
cd docs
npm ci
npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw' — this
                 # WILL fail if the frontmatter/slug is wrong or a
                 # cross-reference link is broken
```

A broken ADR link or malformed frontmatter fails the build with a clear
Docusaurus error, not a silent 404 — always run this locally before
opening the PR. This repo's CI does not currently gate the docs build in
a dedicated job the way `docs-api-drift` gates the REST reference — don't
rely on CI to catch a broken ADR link; verify it yourself.
