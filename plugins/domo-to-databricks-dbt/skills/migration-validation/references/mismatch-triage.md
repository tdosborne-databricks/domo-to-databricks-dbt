# Mismatch pattern → gotcha triage map (Tier 3)

Maps a Tier-3 data-diff symptom to its likely cause in `tile-translation/references/semantic-gotchas.md`.
Shipped inside the customer diff kit so returned mismatch reports map straight to a fix.

| Mismatch symptom | Likely cause | Fix location |
|---|---|---|
| Row count higher in Databricks after a join | Domo drops null-key join rows; Spark keeps them (or vice-versa) | null-handling in joins — semantic-gotchas.md |
| Row count differs after Group By | Domo groups nulls differently than Spark | Group By null grouping — semantic-gotchas.md |
| Extra/duplicate columns, or `AMBIGUOUS_REFERENCE` | column-replace semantics (Domo replaces, Spark duplicates) | column-replace section — semantic-gotchas.md |
| Numeric column off by rounding | implicit type coercion (string↔number) differs | type coercion — semantic-gotchas.md |
| Dates shifted by hours | timezone default mismatch | date/timezone defaults — semantic-gotchas.md |
| String replace behaves differently | Replace Text regex dialect (MySQL vs. Java/Spark) | regex dialect — semantic-gotchas.md |
| UNION result columns misaligned | positional UNION vs. Domo align-by-name | positional UNION (flagged) — semantic-gotchas.md |
| Whole-column wrong / parse error | untranslated Beast Mode function slipped through | add a `transpile_expr` rule test-first |

**Feedback loop**: every triaged mismatch that reveals a new gotcha gets folded back into
`semantic-gotchas.md` and, if deterministic, into the transpiler as a test-first rule.
