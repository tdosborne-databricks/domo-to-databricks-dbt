#!/usr/bin/env python3
"""Propose per-model materialization config from the ingestion inventory + flow graphs.

Usage:
    python3 materialization_policy.py <inventory.csv> <flows_dir> [--catalog NAME] > materialization.json

Applies the heuristics in references/materialization-rules.md. This is a STARTING PROPOSAL — surface
it for review; do not silently commit storage decisions.

STATUS: minimal working heuristic. Row-count thresholds are placeholders until calibrated against
the customer's real dataset sizes (see references/materialization-rules.md TODO).
"""
import argparse
import csv
import json
import os

BIG_ROWS = 1_000_000  # placeholder — calibrate against real inventory


def propose_for_flow(flow_path, catalog):
    with open(flow_path) as fh:
        flow = json.load(fh)
    out = {}
    for t in flow.get("tiles", []):
        ttype, name = t.get("type"), (t.get("name") or t.get("id"))
        if ttype == "LoadFromVault":
            mat = "view"                      # light staging
        elif ttype == "PublishToVault":
            mat = "table"                     # terminal mart
        else:
            mat = "table"                     # heavy transform default; refine w/ reuse analysis
        out[name] = {
            "materialized": mat,
            "cluster_by": [],                 # TODO: pull filter/join cols from tile config when big
            "unity_catalog_name": f"{catalog}.<domain>.{name}",  # TODO: map schema from Domo domain
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inventory")
    ap.add_argument("flows_dir")
    ap.add_argument("--catalog", default="domo_migration")
    args = ap.parse_args()

    # inventory currently unused beyond existence; row counts feed incremental/clustering once present
    with open(args.inventory) as fh:
        _ = list(csv.DictReader(fh))

    proposal = {}
    for fn in sorted(os.listdir(args.flows_dir)):
        if fn.endswith(".json"):
            proposal.update(propose_for_flow(os.path.join(args.flows_dir, fn), args.catalog))
    print(json.dumps(proposal, indent=2))


if __name__ == "__main__":
    main()
