# Data-diff tolerance rules (Tier 3)

Applied when comparing Domo outputs to Databricks tables. Ship inside the customer kit.

## Numeric

- **Floats**: equal if `abs(a-b) <= max(1e-9, 1e-6 * max(|a|,|b|))` (relative + absolute epsilon).
  Domo and Spark accumulate floating-point differently in aggregates.
- **Decimals/money**: compare at the declared scale; round both to the mart's decimal scale first.

## Temporal

- **Timestamps**: normalize both to UTC before comparing (Domo defaults and Spark session
  timezone differ — see semantic-gotchas.md date/timezone section).
- **Dates**: exact after timezone normalization.

## Strings

- Trim trailing whitespace and normalize case ONLY if the source column was case-insensitive in
  Domo; otherwise exact. Do not silently fold case — flag instead.

## Nulls

- Compare null **rates** per column, not just row-level equality — a systematic null-rate delta is
  the fingerprint of a null-handling gotcha (join/filter/group-by), not random drift.

## Row counts & aggregates

- Row count: exact (any delta is a real defect — investigate before tolerating).
- Aggregate distributions (sum/min/max/mean per numeric column): within the float tolerance above.

Any mismatch outside tolerance → look it up in `mismatch-triage.md`.
