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
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml; do
  if helm template "$release" "$chart_dir" --set analytics.enabled=true --set mcp.enabled=true \
    --set analytics.database.projectorUrl=postgres://projector --show-only "templates/$template" | grep -q 'configMapRef:'; then
    fail "$template must not mount the api ConfigMap"
  fi
done

echo "order-management chart render assertions passed"
