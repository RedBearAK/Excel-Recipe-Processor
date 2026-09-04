# `deduplicate_data`

**Family:** `transform`

Collapse rows to one per key, keeping all columns, reporting value conflicts

## Notes

- **column handling**: all columns kept - nothing to enumerate, nothing to silently lose
- **empty input**: empty output, not an error

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `key_columns`: list of str; REQUIRED - Column names
- `keep`: str; default "first"; one of first, last, none
- `conflicts_file`: str
- `save_conflicts_to_stage`: stage_out

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

One row per key, first occurrence wins

```yaml
# All columns are kept - unlike dedupe-via-aggregation, nothing has to be
# enumerated, so a column added upstream cannot silently vanish here.
# Duplicate rows that are pure repetition collapse without comment.

settings:
  description: "Collapse a line-item export to order grain"
  stages:
    - stage_name: "stg_line_items"
      description: "One row per order x product"
      protected: false
    - stage_name: "stg_orders"
      description: "One row per order"
      protected: false

recipe:
  - step_description: "Collapse to one row per order"
    processor_type: "deduplicate_data"
    source_stage: "stg_line_items"
    # REQ - Columns that define uniqueness
    key_columns: ["Order ID"]
    # OPT - Which duplicate survives: "first" (default) or "last"
    keep: "first"
    save_to_stage: "stg_orders"
```

### conflict reporting

Surface keys whose duplicates disagreed - the reason this processor exists

```yaml
# A join export SHOULD leave identical rows per key once item columns are
# dropped. When it does not - a header line carrying different order
# attributes than the real lines - blind dedupe silently picks a winner.
# Here the winner is picked AND reported three ways: warning log lines
# with the disputed values, an optional stage of the conflicted rows, and
# an optional report file written ONLY when conflicts exist, so the
# file's presence is itself the signal. Point the file beside the source
# download and it becomes the evidence list for cleaning the source
# database.

settings:
  description: "Order collapse with conflict evidence"
  variables:
    report_file: "output/contracts_collapse_conflicts.xlsx"
  stages:
    - stage_name: "stg_line_items"
      description: "Line-item grain input"
      protected: false
    - stage_name: "stg_orders"
      description: "Order grain output"
      protected: false
    - stage_name: "stg_collapse_conflicts"
      description: "Rows of keys that failed to collapse cleanly"
      protected: false

recipe:
  - step_description: "Collapse to order grain, reporting dirty keys"
    processor_type: "deduplicate_data"
    source_stage: "stg_line_items"
    key_columns: ["Order ID"]
    # OPT - Stage receiving every row of every conflicted key, kept and
    # discarded alike, annotated with 'Dedupe Status' and
    # 'Conflicting Columns'
    save_conflicts_to_stage: "stg_collapse_conflicts"
    # OPT - Report file with the same contents; NOT written when the
    # collapse is clean
    conflicts_file: "{report_file}"
    save_to_stage: "stg_orders"
```

### multi column key

Uniqueness defined by more than one column

```yaml
# The key is the combination: one row per (booking, origin, species).

settings:
  description: "Deduplicate on a compound key"
  stages:
    - stage_name: "stg_shipments"
      description: "Shipment rows, possibly repeated"
      protected: false
    - stage_name: "stg_unique_shipments"
      description: "One row per booking-origin-species"
      protected: false

recipe:
  - step_description: "One row per booking, origin and species"
    processor_type: "deduplicate_data"
    source_stage: "stg_shipments"
    key_columns: ["Booking", "Product Origin", "Major Species"]
    save_to_stage: "stg_unique_shipments"
```

## Parameter notes

- `key_columns` (required): Columns defining uniqueness. One row per distinct combination survives.
- `keep` (default `first`): Which duplicate row survives: first or last, in input order
- `save_conflicts_to_stage`: Stage receiving all rows of conflicted keys, annotated with Dedupe Status (kept/discarded) and Conflicting Columns
- `conflicts_file`: Report file with the conflicted rows, written only when conflicts exist. Variables substitute as usual.
- `save_to_stage` (required): Stage receiving the deduplicated frame, all original columns intact

