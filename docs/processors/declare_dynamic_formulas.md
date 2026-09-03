# `declare_dynamic_formulas`

**Family:** `file_ops`

Declare dynamic-era formulas in a finished xlsx to retire the implicit @

## Notes

- **when to use**: repairing a file already on disk (an inherited donor, a flushed output); for recipe outputs prefer the settings key declare_dynamic_formulas: true
- **safety**: only formulas containing post-dynamic-array functions are marked by default, so legacy implicit-intersection semantics cannot be changed; extend with extra_functions only for files whose formulas are known recipe-authored
- **relation to injection**: cells written by inject_formulas need neither this processor nor extra_functions: the session registers them by provenance and the at-save pass (settings declare_dynamic_formulas: true) declares them automatically, whatever functions they use

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `input_file`: str; REQUIRED
- `output_file`: str
- `extra_functions`: list of str

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Repair an on-disk workbook whose XLOOKUP formulas display with @

```yaml
settings:
  description: "Declare the dynamic-era formulas in a finished report"

recipe:
  - # Marks formulas containing functions that POSTDATE dynamic arrays
    # (XLOOKUP, FILTER, LET, ...). Such formulas were necessarily
    # authored in modern Excel, so the declaration states a fact and
    # cannot change results. Pre-dynamic-array array-capable functions
    # (INDEX, OFFSET) are deliberately NOT marked: a legacy formula
    # depending on implicit intersection would start spilling.
    step_description: "Declare dynamic formulas in the report"
    processor_type: "declare_dynamic_formulas"
    # REQ - Finished xlsx to process (must not be held by the session;
    # place a flush_workbooks step first if it is)
    input_file: "reports/quarterly_report.xlsx"
```

### after flush

Declare formulas in a recipe output after an explicit flush

```yaml
settings:
  description: "Flush the session, then declare the flushed file's formulas"

recipe:
  - # The processor refuses to run on a file the session still holds in
    # memory - the disk bytes would be stale and the rewrite would be
    # overwritten at the next save. Flush first.
    step_description: "Write pending workbook changes to disk"
    processor_type: "flush_workbooks"

  - step_description: "Declare dynamic formulas in the flushed output"
    processor_type: "declare_dynamic_formulas"
    input_file: "output/processed_report.xlsx"
    # OPT - Write the result elsewhere instead of in place
    # Default value: same path as input_file
    output_file: "output/processed_report_declared.xlsx"
```

### extra functions

Extend the vocabulary for a file whose formulas are known recipe-authored

```yaml
settings:
  description: "Declare recipe-authored IFS formulas alongside the safe defaults"

recipe:
  - # extra_functions asserts AUTHORSHIP: only add a pre-dynamic-array
    # function here when every formula using it in this file was
    # written recently by you or your recipe - a legacy formula relying
    # on implicit intersection would change behavior if declared.
    # (Cells written by inject_formulas need none of this: the session
    # registers them by provenance and the at-save pass declares them
    # automatically, whatever functions they use.)
    step_description: "Declare including known recipe-authored IFS"
    processor_type: "declare_dynamic_formulas"
    input_file: "output/classification_report.xlsx"
    # OPT - Additional bare function names to treat as dynamic-era
    # Default value: [] (only the safe post-dynamic-array set)
    extra_functions: ["IFS"]
```

## Parameter notes

- `input_file` (required): Finished xlsx/xlsm to process; must not be open in the workbook session
- `output_file` (default `same as input_file (in-place, via a temp file then move)`): Where to write the declared workbook
- `extra_functions` (default `[]`): Additional bare function names to treat as dynamic-era. Asserts these formulas were authored in modern Excel; only safe when that is known true (e.g. the recipe wrote them). The default vocabulary is safe by construction and needs no extension for XLOOKUP, FILTER, LET, etc.


