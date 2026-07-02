#!/usr/bin/env python3
"""Mode B (live Domo API) extractor — runs locally, no Databricks required.

Pulls the targeted Magic ETL DataFlow definitions (+ datasets, Beast Modes, connector
streams) from a Domo instance over read-only HTTP GETs and writes them to a **local**
directory as JSON. This is the same export a customer produces in Mode A, so the output
feeds `ingest_export.py` unchanged — downstream skills can't tell which mode produced it.

Modeled on the Step-1 notebook (`01_extract_domo_inventory.py` in the domo-migration
repo), but with every Databricks dependency removed: no `dbutils`, no secret scope, no
Unity Catalog Volume. The token comes from `--token`/`$DOMO_DEV_TOKEN` and files land in
a plain local folder, so it runs on a laptop that has never seen Databricks.

⚠️ Read-only: every call is an HTTP GET. Nothing in Domo is run, edited, or deleted.

Endpoints are the **verified** internal `/api/*` paths (see references/domo-api-endpoints.md).
Domo's public `/v1/*` paths return an HTML login page for a developer token, so we avoid them.

Usage
-----
    export DOMO_DEV_TOKEN=xxxxxxxx          # Domo → Admin → Authentication → Access tokens
    python3 domo_api_client.py \
        --instance appdirect \
        --flow-name "Advisor Services" \
        --out ./domo_extract

    # then normalize it (identical to a customer-provided Mode A export):
    python3 ingest_export.py ./domo_extract/extract_<ts> ./normalized

Select a single flow by id instead of name filter with `--flow-id 67`.
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime

try:
    import requests
except ImportError:
    sys.exit(
        "This script needs the `requests` package.\n"
        "  pip install requests   (or: python3 -m pip install requests)"
    )

TIMEOUT = 60
MAX_RETRIES = 3
_PAGE = 50


class DomoClient:
    """Thin read-only client over Domo's internal `/api/*` endpoints."""

    def __init__(self, instance, dev_token):
        self.base_url = f"https://{instance}.domo.com"
        self.headers = {
            "x-domo-developer-token": dev_token,
            "Accept": "application/json",
        }

    def _get(self, path, params=None):
        """GET {base}{path} with retry/backoff. Returns parsed JSON, raises on failure."""
        url = f"{self.base_url}{path}"
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=TIMEOUT)
                if resp.status_code == 200:
                    return resp.json()
                # 4xx (except 429) won't improve on retry — fail fast with the body.
                if 400 <= resp.status_code < 500 and resp.status_code != 429:
                    raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")
                last_err = RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")
            except requests.RequestException as e:
                last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)  # 2s, 4s
        raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {last_err}")

    # --- verified endpoints (references/domo-api-endpoints.md) ---

    def list_dataflows(self):
        """Every DataFlow: [{id, name, ...}]."""
        return self._get("/api/dataprocessing/v1/dataflows")

    def get_dataflow(self, flow_id):
        """Full tile graph for one DataFlow: {name, id, actions:[...]}."""
        return self._get(
            f"/api/dataprocessing/v2/dataflows/{flow_id}",
            params={"validationType": "PREVIEW"},
        )

    def list_datasources(self):
        """Paginate /api/data/v3/datasources -> flat list of {id, name, ...}."""
        out, offset = [], 0
        while True:
            page = self._get("/api/data/v3/datasources",
                             params={"limit": _PAGE, "offset": offset})
            batch = page.get("dataSources", []) if isinstance(page, dict) else (page or [])
            if not batch:
                break
            out.extend(batch)
            if len(batch) < _PAGE:
                break
            offset += _PAGE
        return out

    def list_streams(self):
        """Paginate /api/data/v1/streams (connector schedules) -> flat list."""
        out, offset = [], 0
        while True:
            batch = self._get("/api/data/v1/streams",
                              params={"limit": _PAGE, "offset": offset})
            if not batch:
                break
            out.extend(batch)
            if len(batch) < _PAGE:
                break
            offset += _PAGE
        return out

    def list_beast_modes(self):
        """Beast Mode calculated fields. Endpoint is unverified — best-effort, may 404."""
        return self._get("/api/content/v2/beast-modes")


def _dataset_name(d):
    return d.get("name") or d.get("displayName") or d.get("dataSourceName")


def analyze_dataflow_complexity(dataflow):
    """Complexity report for one dataflow definition ({name, id, actions:[...]})."""
    actions = dataflow.get("actions", []) or []
    total_tiles = len(actions)
    types = [(a.get("type") or "") for a in actions]

    tile_score = 1 if total_tiles <= 5 else 3 if total_tiles <= 15 else 5
    join_count = sum(1 for t in types if "Join" in t or "Merge" in t)
    join_score = 1 if join_count <= 1 else 3 if join_count <= 4 else 5
    script_count = sum(1 for t in types if t in ("PythonScript", "RScript", "ScriptTile"))
    if script_count or any(t in ("Pivot", "Unpivot", "RankWindow") for t in types):
        transform_score = 5
    elif any(t in ("GroupBy", "AddFormula", "Aggregate") for t in types):
        transform_score = 3
    else:
        transform_score = 1
    input_datasets = sum(1 for t in types if t in ("LoadFromVault", "InputDataSet"))
    dep_score = 1 if input_datasets <= 2 else 3 if input_datasets <= 5 else 5

    total = tile_score + join_score + transform_score + dep_score
    complexity = "Simple" if total <= 8 else "Medium" if total <= 15 else "Complex"
    return {
        "name": dataflow.get("name"),
        "id": dataflow.get("id"),
        "total_tiles": total_tiles,
        "tile_breakdown": dict(Counter(types)),
        "join_count": join_count,
        "script_count": script_count,
        "input_datasets": input_datasets,
        "score": total,
        "complexity": complexity,
    }


def extract(client, flow_name_filter="", flow_id_override="",
            include_beast_modes=True, include_streams=True, log=print):
    """Run the read-only extraction. Returns a dict of all artifacts (nothing written)."""
    log("Listing dataflows ...")
    all_flows = client.list_dataflows()
    log(f"  found {len(all_flows)} dataflows total")

    if flow_id_override:
        selected = [f for f in all_flows if str(f.get("id")) == str(flow_id_override)]
        if not selected:  # id may be absent from the list response; try it directly
            selected = [{"id": flow_id_override, "name": "(id override)"}]
        log(f"  id override -> flow {flow_id_override}")
    elif flow_name_filter:
        needle = flow_name_filter.lower()
        selected = [f for f in all_flows if needle in (f.get("name") or "").lower()]
        log(f"  name filter {flow_name_filter!r} -> {len(selected)} match(es)")
    else:
        selected = list(all_flows)
        log("  no filter -> selecting ALL flows")

    dataflows = []
    for f in selected:
        fid = f.get("id")
        try:
            definition = client.get_dataflow(fid)
            dataflows.append(definition)
            log(f"  ✓ {fid} ({len(definition.get('actions', []))} tiles) {f.get('name', '')}")
        except Exception as e:  # per-flow errors are non-fatal
            log(f"  ✗ FAILED {fid}: {e}")

    log("Pulling datasets ...")
    datasets = client.list_datasources()
    log(f"  {len(datasets)} datasets")
    dataset_mapping = {str(d.get("id")): _dataset_name(d) for d in datasets}

    beast_modes = []
    if include_beast_modes:
        try:
            log("Pulling Beast Modes ...")
            beast_modes = client.list_beast_modes()
            log(f"  {len(beast_modes)} beast modes")
        except Exception as e:
            log(f"  ✗ Beast Modes failed (non-fatal, unverified endpoint): {e}")

    streams = []
    if include_streams:
        try:
            log("Pulling Streams ...")
            streams = client.list_streams()
            log(f"  {len(streams)} streams")
        except Exception as e:
            log(f"  ✗ Streams failed (non-fatal): {e}")

    complexity_report = sorted(
        (analyze_dataflow_complexity(df) for df in dataflows),
        key=lambda r: r["score"], reverse=True,
    )
    return {
        "dataflows": dataflows,
        "datasets": datasets,
        "dataset_mapping": dataset_mapping,
        "beast_modes": beast_modes,
        "streams": streams,
        "complexity_report": complexity_report,
    }


def write_export(artifacts, out_dir, meta=None, log=print):
    """Write artifacts to a local folder as the Mode-A-compatible export file set."""
    os.makedirs(out_dir, exist_ok=True)

    def _write(name, obj):
        with open(os.path.join(out_dir, name), "w") as fh:
            json.dump(obj, fh, indent=2, default=str)
        n = len(obj) if hasattr(obj, "__len__") else "?"
        log(f"  wrote {name}  ({n} items)")

    _write("dataflows.json", artifacts["dataflows"])
    _write("datasets.json", artifacts["datasets"])
    _write("dataset_mapping.json", artifacts["dataset_mapping"])
    _write("complexity_report.json", artifacts["complexity_report"])
    if artifacts.get("beast_modes"):
        _write("beast_modes.json", artifacts["beast_modes"])
    if artifacts.get("streams"):
        _write("streams.json", artifacts["streams"])

    manifest = dict(meta or {})
    manifest["counts"] = {k: len(artifacts.get(k, []))
                          for k in ("dataflows", "datasets", "beast_modes", "streams")}
    manifest["files"] = sorted(os.listdir(out_dir)) + ["_manifest.json"]
    manifest["note"] = ("Mode B (live Domo API) read-only extract. Feed this folder to "
                        "ingest_export.py — identical contract to a Mode A provided export.")
    _write("_manifest.json", manifest)
    return out_dir


def main(argv=None):
    ap = argparse.ArgumentParser(description="Mode B — live Domo API extractor (local, read-only).")
    ap.add_argument("--instance", required=True, help="Domo subdomain, e.g. 'appdirect'")
    ap.add_argument("--token", default=os.environ.get("DOMO_DEV_TOKEN"),
                    help="Domo developer token (or set $DOMO_DEV_TOKEN)")
    ap.add_argument("--flow-name", default="", help="Case-insensitive name substring filter")
    ap.add_argument("--flow-id", default="", help="Flow id (wins over --flow-name)")
    ap.add_argument("--out", default="./domo_extract", help="Output directory (default ./domo_extract)")
    ap.add_argument("--no-beast-modes", action="store_true")
    ap.add_argument("--no-streams", action="store_true")
    ap.add_argument("--no-timestamp", action="store_true",
                    help="Write straight into --out instead of a timestamped subfolder")
    args = ap.parse_args(argv)

    if not args.token:
        ap.error("no token — pass --token or set $DOMO_DEV_TOKEN")

    client = DomoClient(args.instance, args.token)
    print(f"Target instance : {client.base_url}")
    print(f"Filter          : "
          + (f"id={args.flow_id}" if args.flow_id
             else f"name~{args.flow_name!r}" if args.flow_name else "ALL flows"))

    artifacts = extract(
        client,
        flow_name_filter=args.flow_name,
        flow_id_override=args.flow_id,
        include_beast_modes=not args.no_beast_modes,
        include_streams=not args.no_streams,
    )

    if not artifacts["dataflows"]:
        print("⚠️  No dataflow definitions pulled — check your filter / id / token.")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out if args.no_timestamp else os.path.join(args.out, f"extract_{ts}")
    print("Writing export ...")
    write_export(artifacts, out_dir, meta={
        "extracted_at": ts,
        "domo_instance": args.instance,
        "flow_name_filter": args.flow_name or None,
        "flow_id_override": args.flow_id or None,
        "mode": "B (live Domo API)",
    })

    print(f"\n✅ Extraction complete: {out_dir}")
    print(f"   Next: python3 ingest_export.py {out_dir} ./normalized")


if __name__ == "__main__":
    main()
