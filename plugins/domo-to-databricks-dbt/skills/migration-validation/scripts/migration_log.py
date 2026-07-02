#!/usr/bin/env python3
"""Append a per-flow migration-log entry recording the highest validation tier achieved.

Usage:
    python3 migration_log.py <flow_id> --tier {1,2,3} [--todos N] [--note TEXT] \\
        [--log migration_log.jsonl]

The migration log is the audit-trail deliverable (one JSONL line per flow migration): decisions,
hand-translated tile count, and validation tier achieved.
"""
import argparse
import json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("flow_id")
    ap.add_argument("--tier", type=int, choices=[1, 2, 3], required=True)
    ap.add_argument("--todos", type=int, default=0, help="count of hand-translated / TODO tiles")
    ap.add_argument("--note", default="")
    ap.add_argument("--log", default="migration_log.jsonl")
    args = ap.parse_args()

    entry = {
        "flow_id": args.flow_id,
        "tier_achieved": args.tier,
        "hand_translated_tiles": args.todos,
        "note": args.note,
    }
    with open(args.log, "a") as fh:
        fh.write(json.dumps(entry) + "\n")
    print(f"Logged flow {args.flow_id}: Tier {args.tier}, {args.todos} hand-translated tile(s)")


if __name__ == "__main__":
    main()
