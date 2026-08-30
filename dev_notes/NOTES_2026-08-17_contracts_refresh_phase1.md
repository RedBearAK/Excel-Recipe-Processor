# Contracts refresh phase 1: clean base + diff report (2026-08-17)

New recipe `recipe_files/contracts_clean_base.yaml`, run by the
`erp_contracts_refresh.sh` stub against the latest Contracts_*.xlsx
download. Produces `{lookup_dir}/clean_contracts_base.xlsx` (replaced,
max 2 backups) and a timestamped diff report beside the download with
New_Contracts / Changed_Contracts / Conflicts tabs. The VMS recipe
pulls the clean base into a visible Contracts tab (after SOs, gradient
659765), fail-safe when the file has never been produced.

## Filter rules (proven, zero false drops vs the enriched base)

1. SO Classification = System-Generated dropped (401/1879 in the
   2026-08-17 download; the working base has never contained one).
2. Named housekeeping buckets dropped via the ONLY predicate consistent
   with every observed decision: third Order ID segment does not start
   with a digit (26-SBS-BAIT, -DONATION, -HOMEPACKS-FLEET, -Air Cargo
   Claim Fresh...). Sales Type and Customer both misclassify at least
   one real contract.
3. Line items collapse to contract grain on Order ID; disagreeing
   contracts (26-SBS-30A067: product-less header line, different
   payment terms, blank customer) land on the Conflicts tab with
   kept/discarded rows and the disputed columns named.
4. Whitespace stripped everywhere INCLUDING Order ID: 34 keys carry a
   trailing space from the source system; joins currently survive only
   because both sides pad identically.

## Framework changes this session

- **import_file `on_missing_file`**: 'error' (default) or
  'create_empty' with mandatory `create_empty_columns`, for files that
  legitimately may not exist yet (sibling-recipe outputs, first-run
  baselines). Guarded both ways; stray `create_empty_columns` under
  'error' also refuses.
- **diff_data blank equivalence**: None/NaN, '', and whitespace-only
  are one absent value in comparisons. Excel cannot represent the
  difference ('' written to a cell reads back NaN), so the old
  behavior manufactured a phantom CHANGED row on every run whose
  baseline round-tripped through a file. Found live: a Notes cell
  containing a single space flagged its contract forever.
- **select_columns accepts 0-row frames**: column selection is fully
  defined on an empty frame, and pipelines legitimately produce empty
  stages (a diff with no NEW rows - the common, good case). Only a
  frame with no COLUMNS refuses. The old validate_data_not_empty call
  killed the no-news run.

## Verified in sandbox

First run: fail-safe empty baseline, 1467 contracts all NEW. Repeat
runs: "no differences" (idempotent after the blank-equivalence fix),
backups capped at exactly 2. Mutation run (edited term + new contract
+ injected line conflict): each landed on exactly its own report tab,
Change_Details showing old -> new per field. No-news run: report tabs
export headers-only, recipe green.

## Out of scope, parked

DELETED rows (contracts leaving the download view) are in the full
diff stage and named in the run log but have no report tab - ruled out
2026-08-17. Enrichment columns (DDI cascade etc.) come next, on top of
this clean base.

# End of file #
