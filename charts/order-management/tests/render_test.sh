#!/usr/bin/env bash
# Chart render assertions for order-management (run by `make chart-test`
# and the `chart-tests` CI job). Needs only helm.
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=order-management

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

api_deployment="$(helm template "$release" "$chart_dir" --show-only templates/deployment.yaml)"
configmap="$(helm template "$release" "$chart_dir" --show-only templates/configmap.yaml)"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"

# D14 (2026-10-12): the api Deployment (cmd/order, the only binary that
# reads the ConfigMap's keys) mounts the chart ConfigMap with envFrom, or
# every config.* value silently does nothing.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# A tuned config value reaches the ConfigMap.
tuned="$(helm template "$release" "$chart_dir" --set config.outboxRelayInterval=7s --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_RELAY_INTERVAL: "7s"' <<<"$tuned" || fail "config.outboxRelayInterval is not rendered into the ConfigMap"

# D12 / ADR 0040: the outbox relay attempt cap defaults to 20 and is
# tunable (0 = unlimited) through config.outboxMaxAttempts.
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS default 20 missing from the ConfigMap"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment does not set OUTBOX_MAX_ATTEMPTS"
capped="$(helm template "$release" "$chart_dir" --set config.outboxMaxAttempts=0)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "0"' <<<"$capped" || fail "config.outboxMaxAttempts=0 is not rendered"

# The housekeeping keys exist only with a database (the sweeper is
# Postgres-only).
if grep -q 'HOUSEKEEPING_INTERVAL' <<<"$configmap"; then
  fail "HOUSEKEEPING_INTERVAL rendered without a database"
fi
with_db="$(helm template "$release" "$chart_dir" --set database.url=postgres://x --show-only templates/configmap.yaml)"
grep -q 'HOUSEKEEPING_INTERVAL: "1h"' <<<"$with_db" || fail "HOUSEKEEPING_INTERVAL missing with a database"

# ADR 0043: the release-confirmation loop ships dark. Off by default it renders
# no RELEASE_CONFIRMATION_* key; "kafka" renders the mode, a stable default
# group, the 15m timeout, the 168h retention and KAFKA_BROKERS; tuned values and
# a "0" (which means disabled / keep) reach the ConfigMap unchanged.
if grep -q 'RELEASE_CONFIRMATION' <<<"$configmap"; then
  fail "RELEASE_CONFIRMATION_* rendered while releaseConfirmation.mode is empty"
fi
rc_on="$(helm template "$release" "$chart_dir" --set releaseConfirmation.mode=kafka --show-only templates/configmap.yaml)"
grep -q 'RELEASE_CONFIRMATION_MODE: "kafka"' <<<"$rc_on" || fail "RELEASE_CONFIRMATION_MODE missing with releaseConfirmation.mode=kafka"
grep -q 'RELEASE_CONFIRMATION_CONSUMER_GROUP: "order-management-release-confirmation"' <<<"$rc_on" || fail "default release-confirmation consumer group missing"
grep -q 'RELEASE_CONFIRMATION_TIMEOUT: "15m"' <<<"$rc_on" || fail "RELEASE_CONFIRMATION_TIMEOUT default 15m missing"
grep -q 'RELEASE_CONFIRMATION_RETENTION: "168h"' <<<"$rc_on" || fail "RELEASE_CONFIRMATION_RETENTION default 168h missing"
grep -q 'KAFKA_BROKERS:' <<<"$rc_on" || fail "KAFKA_BROKERS missing with releaseConfirmation.mode=kafka"
rc_tuned="$(helm template "$release" "$chart_dir" --set releaseConfirmation.mode=kafka --set releaseConfirmation.consumerGroup=om-rc-x \
  --set releaseConfirmation.timeout=0 --set releaseConfirmation.retention=0 --show-only templates/configmap.yaml)"
grep -q 'RELEASE_CONFIRMATION_CONSUMER_GROUP: "om-rc-x"' <<<"$rc_tuned" || fail "releaseConfirmation.consumerGroup not rendered"
grep -q 'RELEASE_CONFIRMATION_TIMEOUT: "0"' <<<"$rc_tuned" || fail "releaseConfirmation.timeout=0 was not kept"
grep -q 'RELEASE_CONFIRMATION_RETENTION: "0"' <<<"$rc_tuned" || fail "releaseConfirmation.retention=0 was not kept"
rc_api="$(helm template "$release" "$chart_dir" --set releaseConfirmation.mode=kafka --show-only templates/deployment.yaml)"
grep -A3 'envFrom:' <<<"$rc_api" | grep -q 'configMapRef:' || fail "api Deployment lost its envFrom with releaseConfirmation.mode=kafka"

# The other workloads read their own explicit env and must NOT inherit the
# api's HTTP_ADDR from the ConfigMap.
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml a2a-deployment.yaml; do
  if helm template "$release" "$chart_dir" --set analytics.enabled=true --set mcp.enabled=true --set a2a.enabled=true \
    --set database.url=postgres://app --set analytics.database.projectorUrl=postgres://projector --show-only "templates/$template" | grep -q 'configMapRef:'; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# --- A2A agent (ADR-0046) ----------------------------------------------------
# cmd/a2a is a READ-ONLY, Postgres-only deployable: it needs the OLTP and the
# analytics DSNs and nothing about Kafka.
repo_dir="$(cd "$chart_dir/../.." && pwd)"

# env_value <rendered manifest> <NAME>: the literal `value:` of a container env
# entry whose `name:` line is immediately followed by it.
env_value() {
  awk -v n="$2" '$1=="-" && $2=="name:" && $3==n { getline; sub(/^[[:space:]]*value:[[:space:]]*/, ""); gsub(/"/, ""); print; found=1; exit } END { exit found ? 0 : 1 }' <<<"$1"
}

a2a_args=(--set a2a.enabled=true --set database.existingSecret=order-management-db
  --set analytics.enabled=true --set analytics.database.existingSecret=order-management-analytics-db)
a2a_dep="$(helm template "$release" "$chart_dir" "${a2a_args[@]}" --show-only templates/a2a-deployment.yaml)"

# Off by default: no a2a resource renders unless asked for.
if helm template "$release" "$chart_dir" | grep -q 'component: a2a'; then fail "an a2a resource rendered with default values"; fi

# The agent has no in-memory and no empty-analytics mode: enabling it without
# the databases fails the render with a message instead of a crash-looping pod.
no_db="$(helm template "$release" "$chart_dir" --set a2a.enabled=true --set analytics.enabled=true 2>&1 || true)"
grep -q 'a2a.enabled=true requires database.url or database.existingSecret' <<<"$no_db" || fail "a2a without a database must fail the render with a named reason: $(head -n 3 <<<"$no_db")"
no_analytics="$(helm template "$release" "$chart_dir" --set a2a.enabled=true --set database.url=postgres://app 2>&1 || true)"
grep -q 'a2a.enabled=true requires analytics.enabled=true' <<<"$no_analytics" || fail "a2a without analytics must fail the render with a named reason: $(head -n 3 <<<"$no_analytics")"

grep -q 'command: \["/app/a2a"\]' <<<"$a2a_dep" || fail "the a2a Deployment must run /app/a2a"
[[ "$(env_value "$a2a_dep" A2A_ADDR || true)" == ":8093" ]] || fail "the a2a agent must listen on :8093 (8090 is MCP, 8091 the projector admin, 8092 the reports API)"
[[ "$(helm template "$release" "$chart_dir" "${a2a_args[@]}" --set mcp.enabled=true --set frontend.enabled=true | grep -c 'containerPort: 8093')" == "1" ]] \
  || fail "port 8093 must be used by exactly one container (the a2a agent)"
grep -A4 'name: DATABASE_URL$' <<<"$a2a_dep" | grep -q 'secretKeyRef:' || fail "the a2a DATABASE_URL must come from a Secret"
grep -A6 'name: ANALYTICS_DATABASE_URL$' <<<"$a2a_dep" | grep -q 'name: order-management-analytics-db' || fail "the a2a ANALYTICS_DATABASE_URL must come from the analytics Secret"
grep -A6 'name: ANALYTICS_DATABASE_URL$' <<<"$a2a_dep" | grep -q 'key: ANALYTICS_READER_DATABASE_URL' || fail "the a2a ANALYTICS_DATABASE_URL must use the READ-ONLY reader key"
[[ "$(grep -c 'path: /healthz' <<<"$a2a_dep")" == "3" ]] || fail "the a2a startup, liveness and readiness probes must all hit GET /healthz"

# READ-ONLY: it publishes nothing, so no Kafka/publisher configuration at all.
for var in KAFKA_BROKERS EVENT_PUBLISHER; do
  if grep -q "name: $var\$" <<<"$a2a_dep"; then fail "the read-only a2a Deployment must not render $var"; fi
done

# The Agent Card must advertise a dialable absolute URL: it defaults to the
# release's in-cluster Service DNS (namespace-derived, so pattern-match) and
# stays overridable.
a2a_url="$(env_value "$a2a_dep" A2A_AGENT_URL || true)" # a missing var must reach the fail message below, not abort under set -e
[[ "$a2a_url" =~ ^http://[a-z0-9.-]+-a2a\.[a-z0-9-]+\.svc\.cluster\.local:8093/$ ]] || fail "A2A_AGENT_URL default is not the in-cluster Service DNS: $a2a_url"
a2a_override="$(helm template "$release" "$chart_dir" "${a2a_args[@]}" --set a2a.agentUrl=http://gw.example/api/a2a/order-management/ --show-only templates/a2a-deployment.yaml)"
[[ "$(env_value "$a2a_override" A2A_AGENT_URL || true)" == "http://gw.example/api/a2a/order-management/" ]] || fail "a2a.agentUrl does not override A2A_AGENT_URL"

# Every variable cmd/a2a's boot config names as REQUIRED is rendered, derived
# from the Go source so a new mandatory variable fails until the chart has it.
a2a_vars="$(grep -hoE '[A-Z][A-Z0-9_]{2,} is required' "$repo_dir"/cmd/a2a/main.go | sed -E 's/ is required//' | sort -u || true)"
[[ -n "$a2a_vars" ]] || fail "found no mandatory variables for cmd/a2a; the scan is looking in the wrong place"
for var in $a2a_vars; do
  grep -q "name: $var\$" <<<"$a2a_dep" || fail "the a2a Deployment does not render $var, which cmd/a2a requires"
done

# The route (off unless enabled) rewrites the gateway prefix to / so the
# well-known agent card stays reachable under the same prefix.
a2a_route="$(helm template "$release" "$chart_dir" "${a2a_args[@]}" --set a2a.route.enabled=true \
  --set a2a.route.parentRefs[0].name=gw --set a2a.route.parentRefs[0].namespace=kong --show-only templates/a2a-route.yaml)"
grep -q 'value: /api/a2a/order-management$' <<<"$a2a_route" || fail "the a2a route must serve /api/a2a/order-management"
grep -q 'replacePrefixMatch: /$' <<<"$a2a_route" || fail "the a2a route must rewrite its prefix to /"

echo "order-management chart render assertions passed"
