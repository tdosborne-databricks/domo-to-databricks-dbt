# Optimization Log

Append-only log of per-flow architectural decisions made by `dbt-project-optimization`. Unlike
`dbt-error-triage/references/known-patterns.md` (converter bugs that generalize across flows and
get promoted into `tile-translation`), entries here are intentionally **not** pushed upstream — the
converter stays faithful and 1:1 with the Domo flow graph; these are readability/maintainability
calls for a specific flow's long-term owners.

Each entry: what changed, why, the model-count/column-count before → after, and the diff result
that proved it safe.

---

_No entries yet. Add one per optimization pass, per flow._
