# `profile_named_objects`

**Family:** `import`

Per-name workbook object discovery (ranges, lambdas, formulas, tables)

## Notes

- **shares helpers with**: manage_named_objects (named_objects_extraction)
- **anchor consumer**: name-drift alarm: diff_data on profiles of consecutive run outputs

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `workbooks`: list of str; REQUIRED

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Catalog the output workbook's named objects

```yaml
# Output contract (columns by NAME; future facts APPEND, never rename).
# One row per defined name (workbook- AND sheet-scoped) plus one per
# worksheet table:
#   Workbook          - path as configured
#   Name              - object name
#   Object_Type       - lambda / formula / range / constant / table
#   Scope             - 'global', or the owning sheet for sheet-scoped
#                       names and tables
#   Hidden            - definedName hidden flag (tables: false)
#   Definition        - STORED text verbatim - the byte truth, right
#                       for drift comparison
#   Human_Definition  - display syntax: lambdas translated, storage
#                       prefixes stripped
#   Parameters        - comma-joined lambda parameters, '' otherwise

settings:
  description: "Catalog the output's named objects as a stage"
  stages:
    - stage_name: "stg_named_object_profiles"
      description: "Name-keyed catalog of the output workbook"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed profile_named_objects step"
    step_description: "Catalog the output workbook's named objects"
    # REQ - Must be "profile_named_objects" for this processor type
    processor_type: "profile_named_objects"
    # REQ - Non-empty list of workbook paths; reads from disk
    # (variables substitute as usual)
    workbooks:
      - "{output_dir}/{output_basename}.xlsx"
    # REQ - Stage receiving the catalog frame
    save_to_stage: "stg_named_object_profiles"
```

### name drift alarm

Catalog previous and current outputs, then diff on Name

```yaml
# The anchor consumer: the 2026-08-14 incident where Excel's repair
# silently DELETED a named lambda (orphaning callers into #NAME?)
# would surface here as a "name vanished" diff row instead of a user
# report. Drop the Workbook column before diffing - paths differ
# between runs.

settings:
  description: "Named-object drift alarm between the previous and current outputs"
  stages:
    - stage_name: "stg_names_previous"
      description: "Name catalog of the previous output"
      protected: false
    - stage_name: "stg_names_current"
      description: "Name catalog of the current output"
      protected: false
    - stage_name: "stg_name_drift"
      description: "Itemized name-level differences"
      protected: false

recipe:
  - step_description: "Catalog the previous output's names"
    # REQ - Must be "profile_named_objects" for this processor type
    processor_type: "profile_named_objects"
    # REQ - Workbook paths to catalog
    workbooks: ["{output_dir}/previous.xlsx"]
    # REQ - Stage receiving the catalog frame
    save_to_stage: "stg_names_previous"

  - step_description: "Catalog the current output's names"
    processor_type: "profile_named_objects"
    workbooks: ["{output_dir}/{output_basename}.xlsx"]
    save_to_stage: "stg_names_current"

  - step_description: "Name drift alarm"
    processor_type: "diff_data"
    source_stage: "stg_names_current"
    reference_stage: "stg_names_previous"
    key_columns: ["Name"]
    save_to_stage: "stg_name_drift"
```

