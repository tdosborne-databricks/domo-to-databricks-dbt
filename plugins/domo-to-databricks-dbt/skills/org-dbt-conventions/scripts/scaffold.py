#!/usr/bin/env python3
"""Scaffolding generators for the migrated dbt project — STUB.

Usage:
    python3 scaffold.py <flows_dir> <dbt_project_dir> [overrides.json]

Generates: sources.yml (from Domo connector datasets), model file stubs, schema.yml (column docs
from Domo metadata), and ref-rewiring (flow-to-flow dependencies -> ref()).

STATUS: scaffold. Reuse the validated emitters in
tile-translation/scripts/converter/domo_to_dbt/{sources.py,project.py} rather than reimplementing:
    - sources.py  -> sources.yml + LoadFromVault -> UC table resolution (overrides.json)
    - project.py  -> model files, dbt_project.yml, layering
Apply the layering / naming / dedupe / required-tests rules in references/conventions.md, and
defer to the official `dbt` skill for general structure and testing.
"""
import sys


def main():
    sys.exit(
        "scaffold.py is a stub. Reuse converter/domo_to_dbt/{sources,project}.py from the "
        "tile-translation engine and apply references/conventions.md. See SKILL.md."
    )


if __name__ == "__main__":
    main()
