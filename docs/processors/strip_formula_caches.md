# `strip_formula_caches`

**Family:** `file_ops`

strip_formula_caches step processor

## Notes

- **note**: this processor has not published detailed capabilities

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `files`: list of str; REQUIRED
- `create_backup`: bool; default true
- `scope`: list_of_mappings - Sheets (and at most one of cells/columns/rows) to strip; absent = whole workbook
  - `sheet_names`: list of str; REQUIRED
  - `cells`: any
  - `columns`: any
  - `rows`: any

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Strip a whole workbook back to formulas-only, with a backup

```yaml
settings:
  description: "Strip an archived output back to its authored size"

recipe:
  - step_description: "Strip cached formula results from the archive copy"
    processor_type: "strip_formula_caches"
    # REQ - xlsx files to strip, in place (variables substituted)
    files:
      - "archive/260815_VMS_processed.xlsx"
    # OPT - write file.xlsx.stripbak beside each file first
    # Default value: true
    create_backup: true
```

### scoped

Limit the surgery to particular sheets, columns, rows or cells. Each scope entry names sheet_names (a LIST, even for one sheet) and at most ONE of cells / columns / rows - use separate entries to combine restrictions. Without scope, the whole workbook.

```yaml
settings:
  description: "Scoped strip: named sheets and columns only"

recipe:
  - step_description: "Strip only the formula columns on the VMS sheet"
    processor_type: "strip_formula_caches"
    files:
      - "archive/processed.xlsx"
    scope:
      - sheet_names: ["VMS"]
        columns: ["AV:BB"]
      - sheet_names: ["Cust_Summ", "Cust_List"]
```

## Parameter notes

- `files` (required): xlsx paths to strip in place; variable substitution applies
- `create_backup` (default `True`): Copy each file to file.xlsx.stripbak before surgery
- `scope`: Restrict the surgery; each entry has sheet_names (LIST) plus at most one of cells / columns / rows. An array anchor inside scope strips its whole spill; an anchor outside scope leaves the spill untouched.

