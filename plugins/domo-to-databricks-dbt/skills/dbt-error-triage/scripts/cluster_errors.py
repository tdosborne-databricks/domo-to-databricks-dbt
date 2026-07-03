#!/usr/bin/env python3
"""Cluster dbt build failures by normalized error signature.

Reads a dbt `run_results.json`, groups failed nodes by an identifier/literal-stripped
signature of their error message, and flags clusters whose signature already appears in
`known-patterns.md` (a strong hint this is a converter bug, not a one-off).

Usage:
    python3 cluster_errors.py <run_results.json> [known-patterns.md]
"""
import json
import re
import sys
from pathlib import Path


def normalize(message):
    msg = message.strip()
    msg = re.sub(r"`[^`]+`", "`<ID>`", msg)
    msg = re.sub(r"'[^']*'", "'<LIT>'", msg)
    msg = re.sub(r"\b\d+\b", "<N>", msg)
    msg = re.sub(r"\s+", " ", msg)
    return msg


def main():
    if len(sys.argv) < 2:
        print("usage: cluster_errors.py <run_results.json> [known-patterns.md]", file=sys.stderr)
        sys.exit(1)

    run_results = json.loads(Path(sys.argv[1]).read_text())
    known_text = ""
    if len(sys.argv) > 2 and Path(sys.argv[2]).exists():
        known_text = Path(sys.argv[2]).read_text()

    failures = [
        r for r in run_results.get("results", [])
        if r.get("status") in ("error", "fail")
    ]

    clusters = {}
    for r in failures:
        message = (r.get("message") or "").strip()
        sig = normalize(message)
        c = clusters.setdefault(sig, {
            "signature": sig,
            "example_message": message,
            "models": [],
        })
        c["models"].append(r.get("unique_id", "?"))

    out = []
    for sig, c in clusters.items():
        out.append({
            **c,
            "affected_count": len(c["models"]),
            "recurs_within_run": len(c["models"]) >= 2,
            "matches_known_pattern": sig[:60] in known_text if known_text else False,
        })
    out.sort(key=lambda c: -c["affected_count"])

    print(json.dumps({
        "total_failures": len(failures),
        "cluster_count": len(out),
        "clusters": out,
    }, indent=2))


if __name__ == "__main__":
    main()
