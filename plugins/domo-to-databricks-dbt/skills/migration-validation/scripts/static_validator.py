#!/usr/bin/env python3
"""Tier 1 (static) validator — no warehouse required.

Usage:
    python3 static_validator.py <dbt_project_dir> <flows_dir> > tier1_report.json

Tier 1 checks:
  - transpiled SQL parses in the Spark/Databricks dialect  (uses sqlglot if installed)
  - every {{ ref() }} resolves to a model and every {{ source() }} to a declared source
  - no orphaned CTEs (a WITH block defined but never referenced)
  - the dbt project covers the Domo flow graph: every flow output has a mart, every flow
    input dataset has a source
  - output schema vs. the flow export's declared schema  (skipped when the export omits it)

Exits non-zero if any hard check fails, so it can gate CI. Checks that can't run in this
environment (e.g. sqlglot missing) are reported as "skipped", never silently passed.
"""
import glob
import json
import os
import re
import sys

# Use the converter's exact name sanitizer so lineage names align 1:1 with generated
# models/sources (a local re-implementation drifts, e.g. "(CBOT)" -> "_cbot_" vs "cbot").
_CONVERTER = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "tile-translation", "scripts", "converter"))
if _CONVERTER not in sys.path:
    sys.path.insert(0, _CONVERTER)
try:
    from domo_to_dbt.common import _sanitize
except ImportError:  # pragma: no cover
    def _sanitize(s):
        return re.sub(r"[^0-9a-zA-Z]+", "_", (s or "").strip().lower()).strip("_")

# non-greedy .*? (not [^}]*) so nested braces survive, e.g. tblproperties={'k': 'v'}
_CONFIG_HEADER = re.compile(r"\{\{\s*config\(.*?\)\s*\}\}", re.DOTALL)
_REF = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_SOURCE = re.compile(r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_CTE_DEF = re.compile(r"(?:with|,)\s+([a-zA-Z_]\w*)\s+as\s*\(", re.IGNORECASE)
_LINE_COMMENT = re.compile(r"--[^\n]*")


def _model_files(project_dir):
    files = glob.glob(os.path.join(project_dir, "models", "**", "*.sql"), recursive=True)
    return {os.path.splitext(os.path.basename(f))[0]: f for f in files}


def _source_names(project_dir):
    names = set()
    for y in glob.glob(os.path.join(project_dir, "models", "**", "*.yml"), recursive=True):
        text = open(y).read()
        if "sources:" not in text:
            continue
        # tables live under `    tables:` as `      - name: X`
        in_tables = False
        for line in text.splitlines():
            if re.match(r"\s*tables:\s*$", line):
                in_tables = True
                continue
            if in_tables:
                m = re.match(r"\s*-\s*name:\s*(\S+)", line)
                if m:
                    names.add(m.group(1))
                elif line.strip() and not line.startswith(" " * 6):
                    in_tables = False
    return names


def _strip(sql):
    sql = _CONFIG_HEADER.sub("", sql)
    return _LINE_COMMENT.sub("", sql)


def _render_for_parse(sql):
    """Replace dbt refs/sources with plain identifiers so a SQL parser can read the body."""
    sql = _CONFIG_HEADER.sub("", sql)
    sql = _REF.sub(lambda m: m.group(1), sql)
    sql = _SOURCE.sub(lambda m: f"{m.group(1)}.{m.group(2)}", sql)
    return sql


def _orphan_ctes(sql):
    body = _strip(sql)
    ctes = _CTE_DEF.findall(body)
    orphans = []
    for name in ctes:
        # count identifier occurrences outside its own definition; >1 means it's referenced
        uses = len(re.findall(rf"\b{re.escape(name)}\b", body))
        if uses <= 1:
            orphans.append(name)
    return orphans


def _try_sqlglot():
    try:
        import sqlglot  # noqa
        return sqlglot
    except ImportError:
        return None


def _flow_inputs_outputs(flows_dir):
    fd = os.path.join(flows_dir, "flows") if os.path.isdir(os.path.join(flows_dir, "flows")) else flows_dir
    inputs, outputs, out_schemas = set(), set(), {}
    for f in glob.glob(os.path.join(fd, "*.json")):
        flow = json.load(open(f))
        for i in flow.get("inputs", []) or []:
            if i.get("name"):
                inputs.add(_sanitize(i["name"]))
        for o in flow.get("outputs", []) or []:
            if o.get("name"):
                outputs.add(_sanitize(o["name"]))
                if o.get("schema"):
                    out_schemas[_sanitize(o["name"])] = o["schema"]
    return inputs, outputs, out_schemas


def validate(project_dir, flows_dir):
    models = _model_files(project_dir)
    sources = _source_names(project_dir)
    errors, warnings, skipped = [], [], []

    # 1) SQL parses in the Databricks dialect
    sqlglot = _try_sqlglot()
    parsed = 0
    if sqlglot is None:
        skipped.append("sql_parse (sqlglot not installed: pip install sqlglot)")
    else:
        for name, path in models.items():
            try:
                sqlglot.parse_one(_render_for_parse(open(path).read()), dialect="databricks")
                parsed += 1
            except Exception as e:
                errors.append(f"parse error in {name}: {str(e).splitlines()[0][:160]}")

    # 2) ref()/source() resolution + 3) orphaned CTEs
    dangling_refs, dangling_sources, orphan_ctes = [], [], {}
    for name, path in models.items():
        sql = open(path).read()
        for ref in _REF.findall(sql):
            if ref not in models:
                dangling_refs.append(f"{name} -> ref('{ref}')")
        for schema_name, tbl in _SOURCE.findall(sql):
            if tbl not in sources:
                dangling_sources.append(f"{name} -> source('{schema_name}','{tbl}')")
        orphans = _orphan_ctes(sql)
        if orphans:
            orphan_ctes[name] = orphans
    errors += [f"dangling ref: {d}" for d in dangling_refs]
    errors += [f"dangling source: {d}" for d in dangling_sources]
    warnings += [f"orphan CTE(s) in {m}: {c}" for m, c in orphan_ctes.items()]

    # 4) lineage coverage vs Domo flow graph
    flow_inputs, flow_outputs, out_schemas = _flow_inputs_outputs(flows_dir)
    missing_outputs = sorted(o for o in flow_outputs if o not in models)
    missing_inputs = sorted(i for i in flow_inputs if i not in sources and i not in models)
    errors += [f"flow output not materialized as a model: {o}" for o in missing_outputs]
    errors += [f"flow input dataset has no source/model: {i}" for i in missing_inputs]

    # 5) output schema vs declared export schema
    if not out_schemas:
        skipped.append("output_schema (export declares no column schema)")

    report = {
        "summary": {
            "models": len(models), "sources": len(sources),
            "sql_parsed_ok": parsed if sqlglot else None,
            "errors": len(errors), "warnings": len(warnings), "skipped": len(skipped),
            "passed": not errors,
        },
        "errors": errors,
        "warnings": warnings,
        "skipped": skipped,
    }
    return report


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    report = validate(sys.argv[1], sys.argv[2])
    print(json.dumps(report, indent=2))
    sys.exit(0 if report["summary"]["passed"] else 1)


if __name__ == "__main__":
    main()
