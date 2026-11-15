#!/usr/bin/env python3
"""Fail when apis/asyncapi.yaml changes in a way that breaks its pinning consumers.

Fleet consumers pin this spec byte-for-byte (contracts/pinned.json on their
side, refreshed on drift) and read it as a message catalog: they decode every
documented message through their real handlers and assert each documented
`data` field. This gate compares a PR's head spec with its base and flags
exactly the shapes that break such a consumer; additive evolution passes:

  1. a message documented in BASE is gone in HEAD (removed from the catalog);
  2. a required `data` field of a surviving message is gone in HEAD -- removed
     or renamed, which a consumer cannot follow (a field that merely becomes
     optional stays decodable and is NOT flagged);
  3. a `data` field's `type` narrowed: HEAD accepts fewer values than BASE
     (a type swap is included: it removes values too).

Scope: top-level `data` fields, following local $refs and one allOf level --
the envelope's `data` schema is flat in this spec. Nested field changes are
not judged here.

Usage: asyncapi_breaking.py BASE HEAD
Exit 0 = identical or additive, 1 = breaking change, 2 = a spec could not be read
(a spec that cannot be read is NOT a pass: a gate that cannot look must say so).
"""
import argparse
import sys

import yaml

# Pinning consumers of THIS producer's catalog (consumer-contract wave,
# 2026-11): source of truth is warehouse-docs network/model/contract-edges.json
# plus the consumer PRs (exception-management#54, returns-processing#52).
PINNING_CONSUMERS = (
    "fulfillment-execution, inventory-storage, network-inventory-planning, "
    "returns-processing, slotting-optimization, warehouse-planning, "
    "wes-work-planning, exception-management (events) and order-management "
    "(self-pin)"
)


def load(path):
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    if not isinstance(doc, dict):
        raise RuntimeError(f"{path}: not an AsyncAPI document")
    return doc


def resolve(doc, ref):
    """Follow one local '#/components/...' $ref to its node."""
    node = doc
    for part in ref.lstrip("#/").split("/"):
        if not isinstance(node, dict) or part not in node:
            raise RuntimeError(f"unresolvable $ref {ref}")
        node = node[part]
    return node


def properties(spec, node):
    """A schema node's `properties`, following $ref and flattening one allOf level."""
    if not isinstance(node, dict):
        return {}
    if "$ref" in node:
        node = resolve(spec, node["$ref"])
    props = dict(node.get("properties") or {})
    for entry in node.get("allOf") or []:
        if isinstance(entry, dict):
            if "$ref" in entry:
                entry = resolve(spec, entry["$ref"])
            props.update(entry.get("properties") or {})
    return props


def data_schema(spec, message):
    """The CloudEvents `data` payload schema of one message, {} when absent."""
    data = properties(spec, message.get("payload") or {}).get("data")
    if isinstance(data, dict) and "$ref" in data:
        data = resolve(spec, data["$ref"])
    return data if isinstance(data, dict) else {}


def type_set(schema):
    t = schema.get("type") if isinstance(schema, dict) else None
    return set(t) if isinstance(t, list) else ({t} if t else None)


def breaking(base, head):
    """-> list of human-readable findings (empty = identical or additive-only)."""
    out = []
    bm = (base.get("components") or {}).get("messages") or {}
    hm = (head.get("components") or {}).get("messages") or {}
    for name in sorted(set(bm) - set(hm)):
        out.append(f"REMOVED MESSAGE components/messages/{name}: consumers pinning this "
                   "catalog decode every documented message; removing one breaks their "
                   "subscription-coverage check and their decode path")
    for name in sorted(set(bm) & set(hm)):
        b, h = data_schema(base, bm[name]), data_schema(head, hm[name])
        bprops, hprops = b.get("properties") or {}, h.get("properties") or {}
        for field in sorted(set(b.get("required") or []) - (set(hprops) | set(h.get("required") or []))):
            out.append(f"REMOVED REQUIRED data FIELD {name}.data.{field}: base required it; "
                       "head neither requires nor declares it (removed or renamed)")
        for field in sorted(set(bprops) & set(hprops)):
            bt, ht = type_set(bprops[field]), type_set(hprops[field])
            if bt and ht and not bt <= ht:
                out.append(f"NARROWED data FIELD TYPE {name}.data.{field}: "
                           f"{sorted(bt)} -> {sorted(ht)}; head accepts fewer values than base")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("base", help="the AsyncAPI spec of the PR base")
    ap.add_argument("head", help="the AsyncAPI spec of the PR head")
    a = ap.parse_args(argv)
    try:
        base, head = load(a.base), load(a.head)
    except (OSError, RuntimeError, yaml.YAMLError) as e:
        print(f"asyncapi-breaking-gate: cannot read a spec: {e}")
        return 2
    findings = breaking(base, head)
    if findings:
        print("\n".join(findings))
        print(f"\nBREAKING AsyncAPI change vs {a.base}. This catalog is pinned by "
              f"consumer CI: {PINNING_CONSUMERS}.\nRefresh = coordinate the pin "
              "refresh on their side, never a silent edit here.")
        return 1
    n = len((head.get("components") or {}).get("messages") or {})
    print(f"asyncapi-breaking-gate: {n} message(s), no breaking change vs {a.base}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
