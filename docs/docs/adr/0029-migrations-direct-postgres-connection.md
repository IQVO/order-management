---
id: 0029-migrations-direct-postgres-connection
slug: /adr/0029-migrations-direct-postgres-connection
title: 29. Run golang-migrate against a direct Postgres connection, not PgBouncer
sidebar_label: 29. Migrations bypass PgBouncer
description: "ADR 0029 — Phase 4 finding: golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43). Two or more order-management replicas starting concurrently (HPA scale-out, or an ordinary rolling deploy) crash-looped until one won the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL, carries a direct (non-pooled) connection string used ONLY for the migration step; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer."
---

# 29. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record.
Fleet-wide bug found during Phase 4 (k6/HPA load-test validation) cleanup;
this PR is the reference implementation the other 8 OLTP services in the
fleet will copy (their Go-code fan-out is a separate follow-up task — see
warehouse-infra's companion PR, which provisions the new secret key for
all 9 services in one pass).

## Context

warehouse-infra's PgBouncer rollout (PR #43, Phase 3) repointed every one
of the fleet's 9 OLTP services' `DATABASE_URL` secret at PgBouncer
(`terraform/pgbouncer.tf`), in **transaction-pooling** mode
(`pool_mode = "transaction"`). That's the correct mode for this fleet's
steady-state traffic — application code never holds session state across
statements — and PR #43 already carved out one deliberate exception:
analytics DSNs (`analytics_database_urls` in `terraform/locals.tf`) were
left pointed directly at Postgres, because a single low-QPS analytics
consumer gets no pooling benefit and (per that PR's own reasoning) some
analytics access patterns don't tolerate transaction pooling.

What PR #43 did not carve out: **migrations**. All 9 OLTP services run
golang-migrate's postgres driver
(`github.com/golang-migrate/migrate/v4/database/postgres`) against the
same `DATABASE_URL` at process startup, before serving any traffic
(`buildRepoAdapters` / `buildAdapters` in this repo's `cmd/order` and
`cmd/mcp`). golang-migrate's postgres driver calls
`SELECT pg_advisory_lock($1)` to serialize concurrent migration runs —
this is by design: if two processes start at once and both try to run
the same migration, whichever loses the lock should block, not race.

`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it, and is expected to be released by
that same connection (or the session ending). PgBouncer's
transaction-pooling mode does not preserve that mapping — each statement
in a client's logical session can be routed to a different physical
backend connection, because the client's backend connection is returned
to the pool the instant its transaction commits. So:

- Pod A dials PgBouncer, gets backend connection #1, takes the advisory
  lock, runs migrations.
- Pod B dials PgBouncer *concurrently*, gets a **different** backend
  connection, and PgBouncer may freely reuse/rotate backend connections
  for either pod's subsequent statements mid-"session" from the
  application's point of view.
- The advisory lock never behaves as a real mutex across the two pods.
  Whichever pod's statements land on a backend connection with
  unexpected transaction/prepared-statement state gets errors like
  `pq: unnamed prepared statement does not exist` or `pq: canceling
  statement due to statement timeout`, and crash-loops for roughly 1-2
  minutes until the race resolves (one pod happens to hold real
  exclusivity long enough to finish).

This is a **latent, fleet-wide, production-blocking bug**, not a
load-test artifact: it fires on any ordinary rolling ArgoCD deploy with
more than 1 replica, and on every HPA scale-out event for any of these 9
services. It was found during Phase 4 (k6/HPA load-test validation)
cleanup and independently reproduced live: a previously-stable
order-management pod crash-looped the moment a second replica started
concurrently, showing exactly the `pq: unnamed prepared statement does
not exist` signature above. It blocked safely enabling any of the
Phase-3 HPAs (ADR-0026) fleet-wide.

## Decision

Give each of the 9 OLTP services a **second** connection string,
`MIGRATIONS_DATABASE_URL` — a direct (non-pooled, session-mode) Postgres
connection string, same user/password/dbname as `DATABASE_URL`, pointed
at Postgres itself rather than PgBouncer — used **only** for the
golang-migrate startup step. `DATABASE_URL` and the pgxpool built from it
are completely unchanged: every request this service serves still goes
through PgBouncer in transaction-pooling mode, exactly as PR #43 set up.

This is architecturally identical to the analytics-DSN carve-out PR #43
already made, and to universal Postgres/PgBouncer operational guidance:
**migrations need a direct/session connection; steady-state application
traffic goes through the pooler.** We are not weakening or changing
PgBouncer's `pool_mode` (still `transaction`, still correct for this
fleet's traffic) — this fix is entirely about routing one specific,
short-lived, startup-only operation around the pooler, not about
changing how the pooler behaves for anyone else.

`warehouse-infra` provisions `MIGRATIONS_DATABASE_URL` as a new key
alongside the existing `DATABASE_URL` key in each of the 9 services'
`kubernetes_secret.service_db` / the `network-fulfillment` equivalent
(see that repo's companion PR). This repo's `cmd/order/main.go` and
`cmd/mcp/main.go` (both run migrations) now read `MIGRATIONS_DATABASE_URL`
for the migration step:

```go
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
...
buildRepoAdapters(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, ...)
...
postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)  // migrations only
// the pgxpool opened right after this still uses databaseURL, unchanged
```

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset
keeps every environment that doesn't provision the split — local dev,
CI integration tests, or any cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte. Nothing about local dev or CI
changes as a result of this PR.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected. Session pooling would fix the advisory-lock problem but throws
away the entire point of PgBouncer for this fleet: transaction pooling
is what lets many short-lived HTTP-request-scoped OLTP connections share
a small number of physical Postgres backends. Switching to session mode
fleet-wide to accommodate a ~1-2 second startup-time lock call is the
tail wagging the dog.

### Why not just remove the advisory lock / skip migrations on
non-leader replicas?

Rejected. golang-migrate's advisory lock is exactly the right mechanism
*given a session-scoped connection* — the bug is the mismatch between
that mechanism and the pooling mode we run migrations through, not the
mechanism itself. Bypassing it (e.g. an init-container Job that runs
migrations exactly once before any replica starts) was considered but
rejected for this fix: it's a bigger architectural change (a new
Kubernetes resource type per service, coordination with each
Deployment's rollout strategy) for the same outcome this two-line env-var
fallback already achieves, and it would still need a direct-vs-pooled
connection decision for the Job itself.

## Consequences

- **Fixes** the fleet-wide crash-loop bug for order-management; the same
  pattern is ready for the other 8 OLTP services to adopt (separate
  follow-up task, `warehouse-infra`'s companion PR already provisions the
  secret key for all 9 so no infra work blocks that fan-out).
- **No runtime behavior change**: `DATABASE_URL` is untouched, so
  request-serving connection pooling, `pool_mode`, and PgBouncer's own
  configuration are all unaffected by this PR.
- **No behavior change for environments without the split**: the
  `getenv("MIGRATIONS_DATABASE_URL", databaseURL)` fallback means local
  dev and CI integration tests keep using `DATABASE_URL` for everything,
  exactly as before.
- **Unblocks Phase-3 HPAs fleet-wide**: this was the concrete blocker
  keeping any of ADR-0026's HorizontalPodAutoscalers from being safely
  enabled (an HPA scale-out is exactly the "2+ replicas start
  concurrently" trigger for this bug).
- One more secret key to keep in sync per service going forward (9
  services × 1 new key); mechanically generated by Terraform from the
  same `local.services` map as `DATABASE_URL` already is, so there's no
  new per-service manual step.

## Verification

Reproduced live before the fix: order-management crash-looped with `pq:
unnamed prepared statement does not exist` the moment a second replica
started concurrently against the PgBouncer-fronted `DATABASE_URL`.

After deploying both the `warehouse-infra` secret-key change and this
PR's code+chart change to the live `kind-warehouse` cluster:

```
$ kubectl get secret order-management-db -n warehouse-systems -o jsonpath='{.data}'
{"DATABASE_URL":"<base64, unchanged, still pgbouncer:6432>",
 "MIGRATIONS_DATABASE_URL":"<base64, new, postgres-postgresql:5432>"}
```

Confirmed the running pod actually receives both env vars (the chart's
new `database.migrationsExistingSecretKey` wiring):

```
$ kubectl get pod <api-pod> -o jsonpath='{.spec.containers[0].env[*].name}'
... DATABASE_URL MIGRATIONS_DATABASE_URL ...
```

**Forced 3 replicas to start concurrently** twice in a row
(`kubectl scale deployment order-management --replicas=3 -n
warehouse-systems`, run against ArgoCD's `fix/migrations-direct-postgres-dsn`
revision so both the Go fallback and the chart env-var wiring were live
together) and watched them come up with `kubectl get pods -w`:

```
NAME                               READY   STATUS     RESTARTS   AGE
order-management-85b9db8d7-8lsh9   2/2     Running    0          3m4s
order-management-85b9db8d7-kds7t   0/2     Init:1/2   0          2s
order-management-85b9db8d7-tr6j8   0/2     Init:1/2   0          2s
...
order-management-85b9db8d7-kds7t   2/2     Running    0          42s
order-management-85b9db8d7-tr6j8   2/2     Running    0          42s
```

Both new pods' startup logs show migrations running concurrently
(near-identical timestamps, ~9s apart) and proceeding straight to
"event publisher configured" / serving traffic — no dial/retry loop, no
`pq:` errors of any kind:

```
kds7t: 12:33:19 telemetry configured -> 12:33:28 event publisher configured -> ... -> Running 2/2
tr6j8: 12:33:19 telemetry configured -> 12:33:28 event publisher configured -> ... -> Running 2/2
```

`grep -iE "pq:|advisory|panic|fatal"` over both pods' full logs: **no
matches**. **RESTARTS: 0** for both pods throughout — this is the exact
scenario (2+ replicas of the same service starting concurrently) that
reliably reproduced the crash-loop before this fix. Ran the scale-out
twice to rule out a lucky race; both runs were clean.

Reverted the cluster to its normal state afterward: ArgoCD Application
`targetRevision` back to `develop`, `order-management` scaled back to its
default 1 replica, confirmed `Synced`/`Healthy` with 0 restarts.
