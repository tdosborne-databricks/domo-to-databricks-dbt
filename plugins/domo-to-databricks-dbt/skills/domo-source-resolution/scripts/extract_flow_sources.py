#!/usr/bin/env python3
"""Extract Domo connector sources for one Magic ETL flow from a Step-1 export.

Joins LoadFromVault inputs in dataflows.json to streams.json (and optional
datasets.json), classifies each source, and writes inventory artifacts for
interactive UC resolution.

Usage:
    python3 extract_flow_sources.py <export_dir> <out_dir> --flow-id 67
    python3 extract_flow_sources.py <export_dir> <out_dir> --flow-name "Advisor_Services_ETL"

Writes:
    <out_dir>/source_inventory.json
    <out_dir>/source_inventory.md
    <out_dir>/upstream_dataflows.json
    <out_dir>/overrides.template.json   # keys only; agent fills UC paths after resolution
    <out_dir>/source_resolution_status.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

_DATASET_ID_KEYS = ("dataSourceId", "datasetId", "dataSetId", "dataset_id", "sourceId")

DATABASE_CONNECTORS = {
    "ms-sql-server", "postgresql", "mysql", "mysql-federated",
    "snowflake", "amazon-redshift", "oracle-adwc", "aws-athena", "databricks",
    "workbench-odbc",
}
FILE_CONNECTORS = {
    "google-spreadsheets", "google-sheets", "google-sheets-writeback",
    "file-upload-new", "file-upload", "large-file-upload", "workbench-csv",
    "csv-tile", "excel-tile", "sftp",
}
DOMO_NATIVE_CONNECTORS = {
    "dataset-view", "domostats", "domo-dimensions", "api", "webform", "emailer",
    "sample-data", "publicsampledata", "modovault",
}

# Rough table/view name harvest from SQL (T-SQL and generic).
_TABLE_RE = re.compile(
    r"(?:FROM|JOIN)\s+"
    r"(?:(?:dbo|public)\.)?"
    r"[\[`\"]?"
    r"([A-Za-z_][\w$#]*)"
    r"[\]`\"]?",
    re.IGNORECASE,
)
_SHEET_ID_RE = re.compile(r"^[A-Za-z0-9_-]{20,}$")


def _load_json(path):
    with open(path) as fh:
        return json.load(fh)


def _find_flow(dataflows, flow_id=None, flow_name=None):
    if flow_id is not None:
        fid = str(flow_id)
        for f in dataflows:
            if str(f.get("id")) == fid:
                return f
        raise SystemExit(f"flow id {flow_id} not found in dataflows.json")
    if flow_name:
        matches = [f for f in dataflows if f.get("name") == flow_name]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise SystemExit(f"flow name {flow_name!r} not found in dataflows.json")
        raise SystemExit(f"multiple flows named {flow_name!r}; use --flow-id")
    raise SystemExit("provide --flow-id or --flow-name")


def _dataset_id(tile):
    for k in _DATASET_ID_KEYS:
        if tile.get(k):
            return str(tile[k])
    return None


def _config_dict(stream):
    return {c["name"]: c.get("value") for c in (stream.get("configuration") or [])}


def _sheets_url(cfg):
    for key in ("spreadsheetIDFileName", "fileName", "searchedFileName", "sheetId", "spreadsheetId"):
        val = cfg.get(key)
        if not val:
            continue
        val = str(val).strip()
        if val.startswith("http"):
            return val
        if _SHEET_ID_RE.match(val):
            return f"https://docs.google.com/spreadsheets/d/{val}/edit"
        return val
    return None


def _sql_text(cfg):
    return (cfg.get("query") or cfg.get("generatedQuery") or "").strip()


def _sql_tables(sql):
    if not sql:
        return []
    seen, out = set(), []
    for m in _TABLE_RE.finditer(sql):
        name = m.group(1)
        if name.upper() not in ("SELECT", "WHERE", "WITH", "AS", "ON", "AND", "OR", "INNER", "LEFT", "RIGHT", "OUTER"):
            key = name.lower()
            if key not in seen:
                seen.add(key)
                out.append(name)
    return out


def _classify_connector(key):
    if key in DATABASE_CONNECTORS:
        return "database"
    if key in FILE_CONNECTORS:
        return "file"
    if key in DOMO_NATIVE_CONNECTORS:
        return "domo_native"
    if key:
        return "saas_or_other"
    return "unknown"


def _classify_source(connector_key, has_stream, dataset_type):
    if not has_stream and (dataset_type or "").lower() == "dataflow":
        return "upstream_dataflow"
    return _classify_connector(connector_key)


def _load_inputs(flow, dataset_mapping, stream_by_ds, ds_meta):
    inputs = []
    seen = set()
    for action in flow.get("actions", []):
        if action.get("type") != "LoadFromVault":
            continue
        ds_id = _dataset_id(action)
        if not ds_id or ds_id in seen:
            continue
        seen.add(ds_id)
        stream = stream_by_ds.get(ds_id)
        meta = ds_meta.get(ds_id, {})
        cfg = _config_dict(stream) if stream else {}
        connector = (stream or {}).get("dataProvider", {}).get("key")
        sql = _sql_text(cfg)
        source_kind = _classify_source(connector, bool(stream), meta.get("type"))
        inputs.append({
            "tile_name": action.get("name"),
            "dataset_name": dataset_mapping.get(ds_id, action.get("name", ds_id)),
            "data_source_id": ds_id,
            "dataset_type": meta.get("type"),
            "stream_id": stream.get("id") if stream else None,
            "connector_key": connector,
            "source_kind": source_kind,
            "transport": (stream or {}).get("transport", {}).get("type"),
            "update_method": (stream or {}).get("updateMethod"),
            "schedule_state": (stream or {}).get("scheduleState"),
            "description": (cfg.get("_description_") or "").strip(),
            "sql": {
                "query": sql or None,
                "query_type": cfg.get("queryType"),
                "table_name": (cfg.get("tableName") or "").strip() or None,
                "referenced_tables": _sql_tables(sql),
            },
            "file": {
                "spreadsheet_url": _sheets_url(cfg),
                "sheet_name": (cfg.get("spreadsheetIDSheetName") or cfg.get("sheetName")
                                or cfg.get("searchedSheetName") or "").strip() or None,
                "file_selection": (cfg.get("fileSelection") or "").strip() or None,
                "file_search": (cfg.get("fileSearch") or "").strip() or None,
            },
            "resolution": {
                "status": "upstream_dataflow" if source_kind == "upstream_dataflow" else "pending",
                "uc_table": None,
                "ingestion_approach": None,
                "notes": None,
            },
        })
    return inputs


def _md_report(flow, inputs):
    lines = [
        f"# Source inventory — {flow.get('name')} (flow id {flow.get('id')})",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.",
        "",
        "Resolve each `pending` source to a Unity Catalog table (`catalog.schema.table`)",
        "before tile-translation. Upstream DataFlow inputs use `ref()`, not `source()`.",
        "",
        "## Summary",
        "",
        f"- **Inputs:** {len(inputs)}",
    ]
    counts = Counter(i["source_kind"] for i in inputs)
    for kind, n in sorted(counts.items()):
        lines.append(f"- **{kind}:** {n}")
    lines.append("")

    by_kind = {}
    for inp in inputs:
        by_kind.setdefault(inp["source_kind"], []).append(inp)

    for kind in ("database", "file", "saas_or_other", "domo_native", "upstream_dataflow"):
        group = by_kind.get(kind)
        if not group:
            continue
        lines.append(f"## {kind.replace('_', ' ').title()}")
        lines.append("")
        for inp in group:
            lines.append(f"### {inp['dataset_name']}")
            lines.append("")
            lines.append(f"- Tile: {inp['tile_name']}")
            lines.append(f"- Dataset ID: `{inp['data_source_id']}`")
            if inp.get("connector_key"):
                lines.append(f"- Connector: `{inp['connector_key']}`")
            if inp.get("description"):
                lines.append(f"- Description: {inp['description']}")
            sql = inp.get("sql") or {}
            if sql.get("table_name"):
                lines.append(f"- Table/view: `{sql['table_name']}`")
            if sql.get("referenced_tables"):
                lines.append(f"- Referenced tables: {', '.join(f'`{t}`' for t in sql['referenced_tables'][:12])}")
            if sql.get("query"):
                lines.append("")
                lines.append("<details><summary>SQL query</summary>")
                lines.append("")
                lines.append("```sql")
                lines.append(sql["query"][:8000])
                if len(sql["query"]) > 8000:
                    lines.append("-- ... truncated ...")
                lines.append("```")
                lines.append("")
                lines.append("</details>")
            fmeta = inp.get("file") or {}
            if fmeta.get("spreadsheet_url"):
                lines.append(f"- Spreadsheet: {fmeta['spreadsheet_url']}")
            if fmeta.get("sheet_name"):
                lines.append(f"- Sheet tab: `{fmeta['sheet_name']}`")
            lines.append(f"- Resolution status: **{inp['resolution']['status']}**")
            lines.append("")
    return "\n".join(lines)


def main(export_dir, out_dir, flow_id=None, flow_name=None):
    export_dir = os.path.abspath(export_dir)
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    dataflows_path = os.path.join(export_dir, "dataflows.json")
    if not os.path.isfile(dataflows_path):
        raise SystemExit(f"missing {dataflows_path}")

    dataflows = _load_json(dataflows_path)
    flow = _find_flow(dataflows, flow_id=flow_id, flow_name=flow_name)

    mapping_path = os.path.join(export_dir, "dataset_mapping.json")
    dataset_mapping = _load_json(mapping_path) if os.path.isfile(mapping_path) else {}

    streams_path = os.path.join(export_dir, "streams.json")
    streams = _load_json(streams_path) if os.path.isfile(streams_path) else []
    stream_by_ds = {
        s["dataSource"]["id"]: s
        for s in streams
        if s.get("dataSource", {}).get("id")
    }

    datasets_path = os.path.join(export_dir, "datasets.json")
    ds_meta = {}
    if os.path.isfile(datasets_path):
        for d in _load_json(datasets_path):
            if d.get("id"):
                ds_meta[d["id"]] = d

    inputs = _load_inputs(flow, dataset_mapping, stream_by_ds, ds_meta)
    upstream = [i for i in inputs if i["source_kind"] == "upstream_dataflow"]
    resolvable = [i for i in inputs if i["source_kind"] != "upstream_dataflow"]

    inventory = {
        "flow_id": flow.get("id"),
        "flow_name": flow.get("name"),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "export_dir": export_dir,
        "input_count": len(inputs),
        "connector_counts": dict(Counter(i["connector_key"] for i in inputs if i.get("connector_key"))),
        "source_kind_counts": dict(Counter(i["source_kind"] for i in inputs)),
        "inputs": inputs,
    }

    overrides_template = {
        i["data_source_id"]: i["resolution"]["uc_table"]
        for i in resolvable
    }
    # Also include human-readable keys for convenience (converter accepts either).
    for i in resolvable:
        overrides_template.setdefault(i["dataset_name"], i["resolution"]["uc_table"])

    status = {
        "flow_id": flow.get("id"),
        "flow_name": flow.get("name"),
        "pending": [i["data_source_id"] for i in resolvable if i["resolution"]["status"] == "pending"],
        "upstream_dataflows": [i["data_source_id"] for i in upstream],
        "resolved": [],
    }

    paths = {
        "source_inventory.json": inventory,
        "upstream_dataflows.json": upstream,
        "overrides.template.json": overrides_template,
        "source_resolution_status.json": status,
    }
    written = []
    for name, payload in paths.items():
        path = os.path.join(out_dir, name)
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2)
            fh.write("\n")
        written.append(path)

    md_path = os.path.join(out_dir, "source_inventory.md")
    with open(md_path, "w") as fh:
        fh.write(_md_report(flow, inputs))
    written.append(md_path)

    print(f"flow: {flow.get('name')} (id={flow.get('id')})")
    print(f"inputs: {len(inputs)} ({len(resolvable)} need UC resolution, {len(upstream)} upstream DataFlows)")
    for p in written:
        print(f"  wrote {p}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("export_dir", help="Domo Step-1 extract directory")
    ap.add_argument("out_dir", help="Output directory for source resolution artifacts")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--flow-id", help="Magic ETL flow id (numeric)")
    g.add_argument("--flow-name", help="Magic ETL flow name (exact match)")
    args = ap.parse_args()
    fid = int(args.flow_id) if args.flow_id is not None else None
    sys.exit(main(args.export_dir, args.out_dir, flow_id=fid, flow_name=args.flow_name))
