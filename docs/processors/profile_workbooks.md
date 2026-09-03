# `profile_workbooks`

**Family:** `import`

Per-sheet workbook metadata discovery (state, color, extents, counts)

## Notes

- **named objects**: COUNT only - the catalog is Name-keyed and belongs to the planned profile_named_objects
- **anchor consumer**: drift alarm: diff_data on profiles of consecutive run outputs

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

Profile the previous run's output

```yaml
# Output contract (columns by NAME; future facts APPEND, never rename).
# One row per sheet per workbook:
#   Workbook            - path as configured
#   Sheet               - sheet name
#   Position            - 1-based tab position
#   State               - visible / hidden / veryHidden
#   Tab_Color           - RGB string, '' when uncolored
#   Max_Row             - openpyxl-reported row extent
#   Max_Col             - openpyxl-reported column extent
#   Frozen_Panes        - e.g. 'A2', '' when unfrozen
#   Zoom_Percent        - sheet view zoom (100 when unset)
#   DV_Count            - dataValidation rules on the sheet
#   Named_Object_Count  - workbook-level tripwire, repeated per row
#   Has_VBA             - workbook-level tripwire, repeated per row

settings:
  description: "Profile the previous output's sheet metadata"
  stages:
    - stage_name: "stg_previous_profile"
      description: "Per-sheet metadata of the previous output"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed profile_workbooks step"
    step_description: "Profile the previous run's output"
    # REQ - Must be "profile_workbooks" for this processor type
    processor_type: "profile_workbooks"
    # REQ - Non-empty list of workbook paths (variables substitute as
    # usual). v1 reads from disk - the drift shape reads files that
    # only exist on disk.
    workbooks:
      - "{output_dir}/previous_output.xlsx"
    # REQ - Stage receiving the profile frame
    save_to_stage: "stg_previous_profile"
```

### drift alarm

Profile previous and current outputs, then diff on Sheet

```yaml
# The anchor consumer shape: profile the previous run's file and the
# freshly written one, then diff_data on Sheet. A vanished tab, an
# unhidden sheet, a row-count jump, or a zoom drift becomes an
# itemized diff row instead of a surprise. Drop the Workbook column
# before diffing - paths legitimately differ between runs.

settings:
  description: "Workbook drift alarm between the previous and current outputs"
  stages:
    - stage_name: "stg_profile_previous"
      description: "Per-sheet metadata of the previous output"
      protected: false
    - stage_name: "stg_profile_current"
      description: "Per-sheet metadata of the current output"
      protected: false
    - stage_name: "stg_workbook_drift"
      description: "Itemized sheet-level differences"
      protected: false

recipe:
  - step_description: "Profile the previous output"
    # REQ - Must be "profile_workbooks" for this processor type
    processor_type: "profile_workbooks"
    # REQ - Workbook paths to profile
    workbooks: ["{output_dir}/previous.xlsx"]
    # REQ - Stage receiving the profile frame
    save_to_stage: "stg_profile_previous"

  - step_description: "Profile the current output"
    processor_type: "profile_workbooks"
    workbooks: ["{output_dir}/{output_basename}.xlsx"]
    save_to_stage: "stg_profile_current"

  - step_description: "Drift alarm"
    processor_type: "diff_data"
    source_stage: "stg_profile_current"
    reference_stage: "stg_profile_previous"
    key_columns: ["Sheet"]
    save_to_stage: "stg_workbook_drift"
```

