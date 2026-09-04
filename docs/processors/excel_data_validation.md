# `excel_data_validation`

**Family:** `file_ops`

Native Excel data-validation rules: dropdowns, bounds, formula gates, prompts

## Notes

- **list sources**: exactly one of values_list (inline), list_from_named_range (pairs with lookup tabs and manage_named_objects), or list_from_spill_ref ("$Z$2#" anchors)
- **bounds vocabulary**: operator + minimum/maximum for between/not_between; operator + compare_to for the six comparisons; bounds accept numbers, "=formulas", ISO dates, and clock times (auto-converted to DATE()/TIME())
- **targeting**: apply_to_ranges is ALWAYS a list of A1 cells/ranges; one rule may cover several areas (multi-area sqref)
- **behaviors**: allow_blank (default true), show_dropdown (list only, default true), input_prompt {title, message}, error_alert {style: stop/warning/information, title, message}
- **enforcement scope**: fires on MANUAL entry only - paste, fill-down and formulas bypass Excel data validation; use verify_data for pipeline integrity
- **session**: file operation on the workbook session, like conditional_format - run after export, before flush

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `validations`: list_of_mappings; REQUIRED
  - `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
  - `apply_to_ranges`: list of str; REQUIRED - A1 ranges the validation covers
  - `validation_type`: str; REQUIRED; one of list, whole_number, decimal, date, time, text_length, custom
  - `values_list`: list of any - list: literal choices
  - `list_from_named_range`: str - list: a defined name
  - `list_from_spill_ref`: str - list: a spill anchor like Cust_List!$A$2#
  - `excel_formula`: str - custom: the validation formula
  - `operator`: str; one of between, not_between, equal, not_equal, greater_than, less_than, greater_than_or_equal, less_than_or_equal
  - `minimum`: any
  - `maximum`: any
  - `compare_to`: any
  - `allow_blank`: bool; default true
  - `show_dropdown`: bool; default true
  - `input_prompt`: mapping
    - `title`: str
    - `message`: str
    - `style`: str; one of stop, warning, information
  - `error_alert`: mapping
    - `title`: str
    - `message`: str
    - `style`: str; one of stop, warning, information

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

In-cell dropdown from an inline list of values

```yaml
settings:
  description: "Status dropdown on the output workbook"

recipe:
  - step_description: "Add a Status dropdown to the data-entry column"
    processor_type: "excel_data_validation"
    # REQ - Workbook to modify (session-aware; run after the export step)
    target_file: "reports/output.xlsx"
    # REQ - One or more validation entries, each on one sheet
    validations:
      - # REQ - One tab name, or '?sheet_001?' by position
        sheet_name: "Data_Entry"
        # REQ - ALWAYS a list of A1 cells/ranges, even for one range.
        # One rule may legitimately cover several areas.
        apply_to_ranges: ["E2:E500"]
        # REQ - list / whole_number / decimal / date / time /
        # text_length / custom
        validation_type: "list"
        # List sources: exactly ONE of values_list /
        # list_from_named_range / list_from_spill_ref
        values_list: ["Open", "Closed", "Pending"]
```

### dropdown sources

The three list sources, including the live re-pivot pattern

```yaml
settings:
  description: "Interactive summary driven by dropdown selection"

recipe:
  - step_description: "Wire the interactive control cells"
    processor_type: "excel_data_validation"
    target_file: "reports/output.xlsx"
    validations:
      - # Named range source - pairs with manage_named_objects and
        # lookup tabs. This is the recipe-authored interactive report
        # pattern: a dropdown cell selects, formulas re-pivot live.
        sheet_name: "Interactive_Summary"
        apply_to_ranges: ["B2"]
        validation_type: "list"
        list_from_named_range: "rng_customers"

      - # Spill reference source - the dropdown tracks a dynamic
        # array (e.g. SORT(UNIQUE(...))) as it grows and shrinks.
        # Write the natural '#' form; the processor STORES Excel's
        # _xlfn.ANCHORARRAY(...) form, because a stored literal '#'
        # triggers Excel's repair (harvested 2026-08-14).
        sheet_name: "Interactive_Summary"
        apply_to_ranges: ["C2"]
        validation_type: "list"
        list_from_spill_ref: "Lookups!$Z$2#"

      - # Inline source. Items may not contain commas or quotes, and
        # the joined form must fit Excel's 255-character formula
        # limit - the processor fails loud with guidance otherwise.
        sheet_name: "Interactive_Summary"
        apply_to_ranges: ["D2"]
        validation_type: "list"
        values_list: ["This Year", "Last Year", "All Years"]
        # OPT - list only; in-cell arrow shown by default
        # Default value: true
        show_dropdown: true
```

### bounded types

Numeric, date and text-length bounds with semantic keys

```yaml
settings:
  description: "Entry bounds on the data columns"

recipe:
  - step_description: "Bound the quantity, ship date and notes columns"
    processor_type: "excel_data_validation"
    target_file: "reports/output.xlsx"
    validations:
      - # Interval operators (between / not_between) take
        # minimum + maximum.
        sheet_name: "Data_Entry"
        apply_to_ranges: ["F2:F500"]
        validation_type: "whole_number"
        operator: "between"
        minimum: 1
        maximum: 10000

      - # Comparison operators take compare_to. Bounds accept
        # numbers, "=formulas" (cell refs, named ranges), ISO dates
        # and clock times - dates/times convert to DATE()/TIME()
        # automatically because Excel does not evaluate raw strings.
        sheet_name: "Data_Entry"
        apply_to_ranges: ["G2:G500"]
        validation_type: "date"
        operator: "greater_than_or_equal"
        compare_to: "2026-01-01"

      - # A bound can reference a cell so the workbook stays
        # self-adjusting: entries must be on or after the date in B1.
        sheet_name: "Data_Entry"
        apply_to_ranges: ["H2:H500"]
        validation_type: "date"
        operator: "greater_than_or_equal"
        compare_to: "=$B$1"

      - # text_length bounds apply to LEN() of the entry.
        sheet_name: "Data_Entry"
        apply_to_ranges: ["I2:I500"]
        validation_type: "text_length"
        operator: "less_than_or_equal"
        compare_to: 80
```

### prompts and alerts

Input prompts, error alert styles, and multi-area targeting

```yaml
settings:
  description: "Guided data entry with styled enforcement"

recipe:
  - step_description: "Guide and enforce the status columns"
    processor_type: "excel_data_validation"
    target_file: "reports/output.xlsx"
    validations:
      - # One rule covering several areas (multi-area sqref).
        sheet_name: "Data_Entry"
        apply_to_ranges: ["A2:A500", "C2:C500"]
        validation_type: "list"
        list_from_named_range: "rng_status_codes"
        # OPT - Empty entry bypasses the rule
        # Default value: true
        allow_blank: true
        # OPT - Tooltip shown when a cell is SELECTED, before entry
        input_prompt:
          title: "Status code"
          message: "Pick from the current status list."
        # OPT - Shown on invalid entry. Styles: stop (entry
        # rejected), warning (user may override), information
        # (notify only)
        # Default style: "stop"
        error_alert:
          style: "stop"
          title: "Invalid status"
          message: "Entry must match a code on the Lookups tab."

      - # An input prompt with no enforcement: 'custom' with an
        # always-true formula is unnecessary - just annotate via a
        # permissive rule. Here a warning-style alert nudges without
        # blocking.
        sheet_name: "Data_Entry"
        apply_to_ranges: ["J2:J500"]
        validation_type: "text_length"
        operator: "less_than_or_equal"
        compare_to: 200
        error_alert:
          style: "warning"
          title: "Long note"
          message: "Notes over 200 characters get truncated downstream."
```

### custom formula

Custom-formula gate - the escape hatch for everything else

```yaml
settings:
  description: "No-duplicate entry enforcement"

recipe:
  - step_description: "Refuse duplicate invoice references"
    processor_type: "excel_data_validation"
    target_file: "reports/output.xlsx"
    validations:
      - # The formula must evaluate TRUE for acceptable entries,
        # written relative to the range's first cell (Excel
        # translates it across the range). Leading '=' optional.
        sheet_name: "Data_Entry"
        apply_to_ranges: ["B2:B500"]
        validation_type: "custom"
        excel_formula: "=COUNTIF($B$2:$B$500,B2)=1"
        error_alert:
          style: "stop"
          title: "Duplicate reference"
          message: "That invoice reference is already in the sheet."
```

## Parameter notes

- `target_file` (required): Workbook to modify; session-aware like conditional_format
- `validations` (required): One entry per rule; entries may target different sheets
- `sheet_name` (required): Tab name, or '?sheet_001?' index pseudo-name (per entry)
- `apply_to_ranges` (required): ALWAYS a list, even for one range. A1-style cells ("B2", "$B$2") or ranges ("A2:A500"). Several entries in one rule become a multi-area sqref, matching Excel's own file format.

- `validation_type` (required): list / whole_number / decimal / date / time / text_length / custom
- `values_list`: Inline dropdown values. Items may not contain commas or double quotes; joined form must fit Excel's 255-character formula limit.

- `list_from_named_range`: Defined name (e.g. rng_customers); pairs with manage_named_objects
- `list_from_spill_ref`: Spill anchor like "$Z$2#" or "Lookups!$Z$2#"
- `operator`: between / not_between (need minimum + maximum) or equal / not_equal / greater_than / less_than / greater_than_or_equal / less_than_or_equal (need compare_to)

- `minimum`: Lower bound; accepts numbers, "=formulas", ISO dates, clock times
- `maximum`: Upper bound; same accepted forms as minimum
- `compare_to`: Single bound; same accepted forms as minimum
- `excel_formula`: Must evaluate TRUE for acceptable entries; leading '=' optional
- `allow_blank`: Empty entry bypasses the rule (default true, matching Excel)
- `show_dropdown`: List only; in-cell arrow (default true). Enforcement stays either way.
- `input_prompt`: Tooltip shown when the cell is selected, before any entry
- `error_alert`: Shown on invalid entry; style: stop / warning / information

