# `verify_sheet_data`

**Family:** `file_ops`

Check a written sheet row values against rules; warn or halt per rule

## Notes

- **formula caveat**: written formula cells have no cached values; verify formula inputs in stages
- **family**: file_ops: target_file and sheet_name

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
- `rules`: list_of_mappings; REQUIRED
  - `column`: str; REQUIRED
  - `condition`: str; REQUIRED - Any filter_data condition
  - `value`: any
  - `case_sensitive`: bool; default false
  - `stage_name`: stage_in - For in_stage / not_in_stage conditions
  - `stage_column`: str
  - `stage_key_column`: str
  - `stage_value_column`: str
  - `key_column`: str
  - `comparison_operator`: str
  - `severity`: str; default "warn"; one of warn, halt
  - `description`: str - Replaces the generated expectation line

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Verify a written sheet's values

```yaml
settings:
  description: "Verify the output file before hand-off"

recipe:
  - # File mode reads the sheet's VALUES. A session-held file is read
    # live (pre-save). CAVEAT: formula cells in files this framework
    # wrote have no cached values - openpyxl computes nothing - so
    # rules aimed at injected formula columns see blanks. Verify
    # formula INPUTS in stages; verify written VALUE columns in files.
    step_description: "Verify the written detail sheet"
    processor_type: "verify_sheet_data"
    # REQ - Workbook to verify (session-held files are read live, pre-save)
    target_file: "{output_dir}/{output_basename}.xlsx"
    # REQ (file mode) - Tab to read
    # REQ - Tab name, number, or ?sheet_NNN? token
    sheet_name: "Detail"
    # REQ - Rules in filter_data condition vocabulary
    rules:
      - column: "Invoice No"
        condition: "not_empty"
        # OPT - Default value: "warn" (logs count and sample); "halt" raises
        severity: "halt"
```

## Parameter notes

- `target_file`: Workbook to verify (with 'sheet'). Session-held files are read live, pre-save.
- `sheet_name`: Tab to read in file mode
- `rules` (required): Expectations every row must satisfy. Any filter_data condition works, with that condition's own parameters (value, case_sensitive, stage_name/stage_column for in_stage, ...).

