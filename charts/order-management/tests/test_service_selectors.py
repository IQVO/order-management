#!/usr/bin/env python3
"""Assert this chart's Service selectors isolate each component.

Why this exists: before the frontend workload was added, every Service in this
chart selected on the base selector labels alone (name + instance). Those labels
are identical on the OLTP, analytics projector, analytics reports and MCP pods,
so the OLTP Service's EndpointSlice genuinely contained all four -- verified
live in the kind cluster, where a Kong request for the OLTP /healthz was
answered by the analytics reports pod.

Adding a fifth workload (the frontend) to that set would mean the SPA could
answer an API request and vice versa, which silently breaks the fleet's
Nginx-serves-assets / Kong-serves-APIs separation. This test fails if any two
Services in the chart can select the same pod.

Run: python3 charts/order-management/tests/test_service_selectors.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

CHART = Path(__file__).resolve().parents[1]
REPO = CHART.parents[1]

ENABLE_EVERYTHING = [
    "--set", "frontend.enabled=true",
    "--set", "analytics.enabled=true",
    "--set", "mcp.enabled=true",
]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "order-management", str(CHART), *extra_args],
        capture_output=True, text=True, check=True,
    ).stdout
    # helm emits a multi-document stream; yaml isn't guaranteed installed, so
    # round-trip through a tiny parser via python's json after a yaml->json
    # conversion is avoided by just asking helm for each doc and using PyYAML
    # when present. PyYAML ships with most toolchains here; fall back loudly.
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert selectors", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def selector_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("selector") or {}


def pod_labels_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("template", {}).get("metadata", {}).get("labels") or {}


def matches(selector: dict, labels: dict) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def api_container(docs: list[dict]) -> dict:
    """The OLTP Deployment's container (name == order-management)."""
    for d in docs:
        if d.get("kind") == "Deployment" and d["metadata"]["name"] == "order-management":
            return d["spec"]["template"]["spec"]["containers"][0]
    raise AssertionError("order-management Deployment was not rendered")


def check_startup_probe_gates_boot_work(failures: list[str]) -> None:
    """Boot work must not race the liveness probe.

    This service runs migrations (and, with PATH_CATALOGUE_SOURCE=kafka,
    three Kafka consumer dials) BEFORE it starts listening, and in this
    cluster every injected pod's first outbound dial is reset ~10s after
    start. Without a startupProbe the kubelet SIGTERMs a pod that is still
    booting — observed live as CrashLoopBackOff on order-management
    (order-management-8646d859c7-fkvcz, 129 restarts), which reads like an
    app crash and is not one. See network-fulfillment PR #7, the pattern
    this chart change ports.
    """
    docs = render([])
    c = api_container(docs)

    probe = c.get("startupProbe")
    if not probe:
        failures.append(
            "no startupProbe on the order-management container: boot work "
            "(migrations, plus retried first dials) would race the liveness "
            "probe and lose"
        )
        return

    grace = probe.get("periodSeconds", 10) * probe.get("failureThreshold", 3)
    if grace < 45:
        failures.append(
            f"startupProbe allows only {grace}s; it must outlast the ~31s "
            f"database/Kafka retry budget in cmd/order/main.go"
        )
    if not c.get("livenessProbe"):
        failures.append("startupProbe replaced livenessProbe instead of deferring it")
    if not failures:
        print(f"PASS: startupProbe gates boot for {grace}s, liveness still present")


def main() -> int:
    failures: list[str] = []

    check_startup_probe_gates_boot_work(failures)

    docs = render(ENABLE_EVERYTHING)
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}

    expected = {
        "order-management": "api",
        "order-management-frontend": "frontend",
        "order-management-reports": "analytics-reports",
        "order-management-mcp": "mcp",
    }
    for name, component in expected.items():
        if name not in services:
            failures.append(f"Service {name} was not rendered")
            continue
        sel = selector_of(services[name])
        if sel.get("app.kubernetes.io/component") != component:
            failures.append(
                f"Service {name} selector component is "
                f"{sel.get('app.kubernetes.io/component')!r}, expected {component!r}"
            )

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(
                f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1"
            )

    # A frontend Service must never be host-exposed.
    fe = services.get("order-management-frontend")
    if fe and fe["spec"].get("type") != "ClusterIP":
        failures.append("frontend Service must be ClusterIP")

    # Frontend routing belongs to the Nginx web gateway, not this chart.
    for d in docs:
        if d.get("kind") in {"Ingress", "HTTPRoute"}:
            name = d["metadata"]["name"]
            if "frontend" in name:
                failures.append(f"{d['kind']} {name}: frontend routing must not live in this chart")

    # Default values must not deploy the frontend at all.
    default_docs = render([])
    stray = [
        d["metadata"]["name"]
        for d in default_docs
        if d.get("metadata", {}).get("name", "").endswith("-frontend")
    ]
    if stray:
        failures.append(f"frontend resources rendered with default values: {stray}")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: {len(services)} Services each select exactly one Deployment; "
          "frontend is ClusterIP, unrouted, and off by default")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
