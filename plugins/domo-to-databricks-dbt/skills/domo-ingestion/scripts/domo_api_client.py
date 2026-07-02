#!/usr/bin/env python3
"""Mode B (live Domo API) client — STUB.

Only used when Domo credentials exist. This engagement defaults to Mode A (provided export), so
Mode B is stubbed. Verified endpoints are in references/domo-api-endpoints.md; the working Step-1
extraction lives in the domo-migration repo (01_extract_domo_inventory.py).

When implemented, this must emit the SAME normalized graph + inventory as ingest_export.py so
downstream skills can't tell which mode produced them.
"""
import sys


def main():
    sys.exit(
        "Mode B (live Domo API) not implemented — this engagement uses Mode A (provided export).\n"
        "Use ingest_export.py. See references/domo-api-endpoints.md to implement Mode B."
    )


if __name__ == "__main__":
    main()
