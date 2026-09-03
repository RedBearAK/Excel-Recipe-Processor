# `verify_excel_storage`

**Family:** `file_ops`

Audit workbooks' stored formula grammar and dynamic-array declarations

## Notes

- **session aware**: in-flight workbooks audit their WILL-BE-WRITTEN bytes via an in-memory serialize

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `files`: list of str; REQUIRED
- `on_violation`: str; default "halt"; one of halt, warn

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Audit the output workbook's stored grammar

```yaml
# Checks performed:
#   stored_grammar - forbidden leading '=' in definedNames/DV
#     formulas, literal '#' outside strings, chained storage
#     prefixes, _xlfn on names outside the validated future-function
#     map, LAMBDA/LET declaration slots lacking _xlpm.
#   legacy_cse - any t="array" cell missing its cm dynamic-array
#     declaration - the class where a spill shows {braces} and
#     collapses to one value.

settings:
  description: "Audit the freshly written output before handing it off"

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed verify_excel_storage step"
    step_description: "Audit the output workbook's stored grammar"
    # REQ - Must be "verify_excel_storage" for this processor type
    processor_type: "verify_excel_storage"
    # REQ - Non-empty list of workbook paths. SESSION-AWARE: a file
    # currently held by the run's workbook session audits its
    # WILL-BE-WRITTEN bytes (serialized through the same declaration
    # pipeline as the run-end save, in memory) - so a late-recipe
    # audit step sees what Excel will see, not the stale disk copy.
    # Files outside the session (previous outputs, foreign workbooks)
    # audit from disk.
    files:
      - "{output_dir}/{output_basename}.xlsx"
```

### warn

Audit a previous output without failing the run

```yaml
settings:
  description: "Audit an older output for the record, warn-only"

recipe:
  - step_description: "Audit a previous output without failing the run"
    # REQ - Must be "verify_excel_storage" for this processor type
    processor_type: "verify_excel_storage"
    # REQ - Workbook paths to audit
    files:
      - "{output_dir}/some_older_output.xlsx"
    # OPT - 'halt' fails the run listing every violation; 'warn' logs
    # and continues
    # Default value: "halt"
    on_violation: "warn"
```

