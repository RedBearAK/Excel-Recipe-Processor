# `conditional_format`

**Family:** `file_ops`

Write native Excel conditional-formatting rules that stay live in the file

## Notes

- **vocabulary**: canonical ERP condition names (filter_data's equals, greater_than, is_empty, ... plus between, duplicates, unique in the same style); one spelling per condition
- **formula convention**: same as inject_formulas - write for data row 2 with {col:Header Name} placeholders - except placeholders resolve $-LOCKED here so row-wise tests cannot drift sideways as Excel translates the rule across its range; modern function names get the _xlfn storage prefix automatically
- **targeting**: apply_to: entire_row, columns: [names], or a literal range
- **ordering**: list order is Excel priority order; stop_if_true supported
- **per column anchoring**: text, blank, duplicate and unique conditions emit one Excel rule per named column, each anchored to its own range (and duplicates are per-column domains by design)

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
- `rules`: list_of_mappings; REQUIRED
  - `when_cell`: mapping
    - `column_names`: list of str; REQUIRED - Column names
    - `condition`: str; REQUIRED; one of between, contains, duplicates, ends_with, equals, greater_equal, greater_than, is_empty, less_equal, less_than, not_between, not_contains, not_empty, not_equals, starts_with, unique
    - `value`: any - Operand; a two-item list for between / not_between
  - `when_formula`: str - Excel formula written for the top-left cell of the target
  - `color_scale`: mapping
    - `min_color`: str
    - `mid_color`: str
    - `max_color`: str
    - `column_names`: list of str - Column names
    - `range`: str
  - `data_bar`: mapping
    - `color`: str
    - `column_names`: list of str - Column names
    - `range`: str
  - `style`: mapping
    - `fill`: str
    - `font_color`: str
    - `bold`: bool
    - `italic`: bool
  - `apply_to`: str; one of entire_row
  - `column_names`: list of str - Target columns for when_* rules
  - `range`: str - Literal target range like A2:B99
  - `stop_if_true`: bool; default false
  - at least one of: `when_cell`, `when_formula`, `color_scale`, `data_bar`

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Highlight problem rows and flag values with canonical conditions

```yaml
settings:
  description: "Highlight contract rows missing a price"

recipe:
  - # Rules are written in list order, which is Excel's priority order.
    step_description: "Highlight pricing gaps on the output"
    processor_type: "conditional_format"
    # REQ - Workbook to modify (session-aware; run after the export step)
    target_file: "reports/output.xlsx"
    # REQ - One tab name
    sheet_name: "Data"
    rules:
      - # Formula rules use the SAME convention as inject_formulas:
        # write for data row 2 with {col:Header Name} placeholders.
        # Here placeholders resolve $-LOCKED ($AX2) so the row-wise
        # test cannot drift sideways as Excel translates the rule
        # across the range. Modern function names get their _xlfn
        # storage prefix automatically.
        when_formula: '=AND({col:Contracts}2<>"", {col:Price}2="")'
        # REQ (formula rules) - exactly one target: apply_to:
        # "entire_row", column_names: [names], or range: "F2:F500"
        apply_to: "entire_row"
        style:
          fill: "FFC7CE"
          font_color: "9C0006"
        # OPT - Stop evaluating later rules where this one matched
        # Default value: false
        stop_if_true: true

      - # Canonical condition names, same vocabulary as filter_data.
        when_cell:
          column_names: ["Test Dest"]
          condition: "greater_than"
          value: 1
        style:
          fill: "FFEB9C"
```

### conditions tour

One of each condition family, canonical names throughout

```yaml
settings:
  description: "Tour of the when_cell condition families"

recipe:
  - step_description: "Flag data quality signals"
    processor_type: "conditional_format"
    target_file: "reports/output.xlsx"
    sheet_name: "Data"
    rules:
      - # Comparisons: equals, not_equals, greater_than, greater_equal,
        # less_than, less_equal - plus between / not_between which take
        # value: [low, high]
        when_cell: { column_names: ["Weight"], condition: "between", value: [500, 40000] }
        style: { fill: "C6EFCE" }
      - # Text: contains, not_contains, starts_with, ends_with.
        # Excel's SEARCH semantics: case-insensitive.
        when_cell: { column_names: ["Product Name"], condition: "contains", value: "Canned" }
        style: { font_color: "9C5700", bold: true }
      - # Emptiness: is_empty / not_empty (whitespace-only counts empty)
        when_cell: { column_names: ["Price"], condition: "is_empty" }
        style: { fill: "FFC7CE" }
      - # Value sets: duplicates / unique. One rule PER COLUMN, so each
        # column is its own duplicate domain; use range: for a joint one.
        when_cell: { column_names: ["Booking"], condition: "duplicates" }
        style: { fill: "FFC7CE", font_color: "9C0006" }
      - # Gradient kinds take colors instead of a style
        color_scale: { column_names: ["Weight"], min_color: "FFFFFF", max_color: "63BE7B" }
      - data_bar: { column_names: ["Van Count"], color: "638EC6" }
```

### canonical spelling

One spelling per condition: the canonical snake_case names, never Excel dialog spellings

```yaml
settings:
  description: "Rules transcribed from Excel's dialog, written in the canonical names"

recipe:
  - # Excel's dialog says greaterThan; the recipe says greater_than.
    # There is one spelling per condition, so a dialog spelling is
    # refused at load with the canonical set named.
    step_description: "Rule transcribed straight from Manage Rules"
    processor_type: "conditional_format"
    target_file: "reports/output.xlsx"
    sheet_name: "Data"
    rules:
      - when_cell: { column_names: ["Test Dest"], condition: "greater_than", value: 1 }
        style: { fill: "FFEB9C" }
```

## Parameter notes

- `target_file` (required): Workbook to modify; opened through the workbook session, so run after the export that creates it and before any flush
- `sheet_name` (required): Tab to write rules on (one per step)
- `rules` (required): Rules in Excel priority order (list order). Each carries exactly one kind plus an optional style and stop_if_true.

