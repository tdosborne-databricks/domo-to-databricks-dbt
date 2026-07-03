#!/usr/bin/env python3
"""Create a fresh, isolated local workspace for one Domo->dbt migration engagement.

Every downstream skill in this plugin (domo-ingestion, tile-translation, org-dbt-conventions, ...)
takes directory paths as arguments rather than assuming a fixed layout — this script exists so
those paths always point at a clean root created *for this migration*, never at a directory a
previous flow, a previous session, or an unrelated project already wrote into.

Refuses to run against a non-empty directory (use --force to re-init deliberately, e.g. resuming a
known-good workspace) so a fresh session can never silently mix its output with stale artifacts.

For the Databricks-workspace equivalent (Repos / Workspace folder), see
`references/isolated-workspace-setup.md` in this skill dir — that's a CLI/UI operation, not
something this script can do from a local shell.

Usage:
    python3 init_migration_workspace.py <root_dir> <flow_name> [--target local|databricks] [--force]

Creates:
    <root_dir>/
      MIGRATION.md          # flow name, created-at marker, chosen target — read by downstream skills
      .gitignore
      ingestion/            # domo-ingestion writes flows/<id>.json + inventory.csv here
      dbt/                  # org-dbt-conventions scaffolds the dbt project here
    and runs `git init` in <root_dir> if it isn't already inside a git repo.
"""
import argparse
import os
import subprocess
import sys

GITIGNORE = """\
target/
dbt_packages/
logs/
__pycache__/
*.pyc
.user.yml
"""


def is_inside_git_repo(path):
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=path, capture_output=True, text=True,
    )
    return result.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root_dir")
    ap.add_argument("flow_name")
    ap.add_argument("--target", choices=["local", "databricks"], default="local")
    ap.add_argument("--force", action="store_true", help="allow reusing a non-empty root_dir")
    args = ap.parse_args()

    root = os.path.abspath(args.root_dir)

    if os.path.exists(root) and os.listdir(root) and not args.force:
        sys.exit(
            f"refusing to init: {root} already exists and is non-empty. "
            "Pass --force only if you deliberately intend to resume this exact workspace "
            "(e.g. continuing a migration from a prior session) — otherwise pick a new root_dir "
            "so this flow's output can't mix with whatever is already there."
        )

    os.makedirs(os.path.join(root, "ingestion"), exist_ok=True)
    os.makedirs(os.path.join(root, "dbt"), exist_ok=True)

    gitignore_path = os.path.join(root, ".gitignore")
    if not os.path.exists(gitignore_path):
        with open(gitignore_path, "w") as f:
            f.write(GITIGNORE)

    migration_md_path = os.path.join(root, "MIGRATION.md")
    with open(migration_md_path, "w") as f:
        f.write(
            f"# Migration workspace: {args.flow_name}\n\n"
            f"- Target: {args.target}\n"
            f"- Root: {root}\n"
            f"- ingestion/ -> domo-ingestion output (flows/<id>.json, inventory.csv)\n"
            f"- dbt/       -> org-dbt-conventions scaffolds the dbt project here\n\n"
            "Downstream skills should read the `Target` line above instead of re-asking the "
            "local-vs-Databricks question.\n"
        )

    if not is_inside_git_repo(root):
        subprocess.run(["git", "init", "-q", root], check=True)

    print(f"initialized migration workspace at {root}")
    print(f"  ingestion dir: {os.path.join(root, 'ingestion')}")
    print(f"  dbt dir:       {os.path.join(root, 'dbt')}")
    print(f"  target:        {args.target}")


if __name__ == "__main__":
    main()
