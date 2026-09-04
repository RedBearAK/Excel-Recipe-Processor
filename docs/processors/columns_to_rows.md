# `columns_to_rows`

**Family:** `transform`

Demote header columns into label/value rows - wide to long, nothing lost

## Notes

- **not a transpose**: transpose (slice_data) rotates the grid keeping one dimension in the headers; this ELIMINATES the header dimension by turning header names into data
- **inverse**: rows_to_columns restores the wide layout (losslessly, because that processor verifies uniqueness)
- **column split**: give id_columns, value_columns, or both; the missing one is the complement, and with both given every column must be claimed - pandas.melt silently drops unclaimed columns and this processor refuses instead

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `id_columns`: list of str - Column names
- `value_columns`: list of str - Column names
- `labels_to`: str; default "Field"
- `values_to`: str; default "Value"
- `drop_empty_values`: bool; default false

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Stack twelve month columns into month-per-row form

```yaml
settings:
  description: "De-crosstab the monthly amounts download"
  stages:
    - stage_name: "stg_amounts_wide"
      description: "Customer per row, one column per month"
      protected: false
    - stage_name: "stg_amounts_long"
      description: "One row per customer-month"
      protected: false

recipe:
  - # Give id_columns OR value_columns; the other is the complement.
    # With BOTH given, every column must be claimed - a column in
    # neither would be silently dropped by a raw pandas melt, and this
    # processor halts instead.
    step_description: "Stack the month columns"
    # REQ - Must be "columns_to_rows" for this processor type
    processor_type: "columns_to_rows"
    # REQ - Stage to read the wide grid from
    source_stage: "stg_amounts_wide"
    # OPT (this or value_columns) - Columns that stay put and repeat
    id_columns: ["Customer", "Region"]
    # OPT - Name for the column holding the old header names.
    # Default value: "Field" - name the demoted dimension for real.
    labels_to: "Month"
    # OPT - Name for the column holding the cell contents
    # Default value: "Value"
    values_to: "Amount"
    # OPT - Drop rows whose value is blank/NaN (wide grids are often
    # sparse and the blanks rarely mean anything as rows)
    # Default value: false
    drop_empty_values: true
    save_to_stage: "stg_amounts_long"
```

## Parameter notes

- `id_columns`: Columns that stay put; omitted means the complement of value_columns. At least one of the two lists is required.
- `value_columns`: Columns to demote; omitted means the complement of id_columns. Both given: every column must appear in exactly one.
- `labels_to` (default `Field`): Name for the new column carrying the old header names
- `values_to` (default `Value`): Name for the new column carrying the cell contents
- `drop_empty_values` (default `False`): Remove stacked rows whose value is blank or NaN

