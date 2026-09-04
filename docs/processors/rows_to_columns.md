# `rows_to_columns`

**Family:** `transform`

Promote a column's values into headers: long to wide, silent aggregation refused

## Notes

- **losslessness**: each (id, label) pair must map to at most one value; duplicates halt naming the offending pairs, because pivot_table would silently aggregate them and this processor exists to refuse that
- **inverse**: columns_to_rows produces this processor's input shape; the round trip restores the original table
- **column order**: new columns appear in first-appearance order of the labels (months stay in arrival order, not alphabetized)

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `id_columns`: list of str - Omit to imply every column not named by labels_from / values_from
- `labels_from`: str; REQUIRED
- `values_from`: str; REQUIRED
- `fill_missing_with`: any

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Spread month-per-row amounts back into one column per month

```yaml
settings:
  description: "Re-crosstab the long amounts table for the report layout"
  stages:
    - stage_name: "stg_amounts_long"
      description: "One row per customer-month"
      protected: false
    - stage_name: "stg_amounts_wide"
      description: "Customer per row, one column per month"
      protected: false

recipe:
  - # New columns appear in FIRST-APPEARANCE order of the labels, so
    # months stay in the order the data carries them, not alphabetized.
    # Combinations absent from the data become blank cells.
    step_description: "Spread months into columns"
    processor_type: "rows_to_columns"
    source_stage: "stg_amounts_long"
    # REQ - Column whose VALUES become the new headers
    labels_from: "Month"
    # REQ - Column supplying the cell contents
    values_from: "Amount"
    # OPT - Row identity. Default: every other column. An explicit list
    # must claim every remaining column - leftovers would silently
    # vanish from the output, so they halt instead.
    id_columns: ["Customer", "Region"]
    # OPT - Value for absent (id, label) combinations
    # Default value: blank cells
    fill_missing_with: 0
    save_to_stage: "stg_amounts_wide"
```

## Parameter notes

- `labels_from` (required): Column whose distinct values become the new column headers; blank values halt (a column needs a name)
- `values_from` (required): Column whose values fill the new columns' cells
- `id_columns` (default `every column other than labels_from/values_from`): The row identity. Explicit lists must claim all remaining columns.
- `fill_missing_with` (default `blank cells`): Fill for (id, label) combinations absent from the data

