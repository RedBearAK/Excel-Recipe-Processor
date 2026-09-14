# `sever_external_ties`

**Family:** `file_ops`

Sever every external tie in closed xlsx files, writing a new timestamped file

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `files`: list of str; REQUIRED - xlsx paths to sever; never modified
- `output_dir`: str - Directory for the new files; default beside each source
- `output_suffix`: str; default "_severed" - Appended to the stem before the timestamp
- `timestamp_format`: str; default "%y%m%d_%H%M%S" - strftime format for the output timestamp
- `retarget_if_possible`: bool; default true - Rewrite [N]Sheet! to a local Sheet! when that sheet exists here
- `unresolved_formula_policy`: str; default "freeze"; one of freeze, refuse - Formula cells that do not retarget: freeze to the cached value, or refuse
- `unresolved_defined_name_policy`: str; default "drop"; one of drop, refuse - Defined names that do not retarget: drop the name, or refuse
- `unresolved_conditional_formatting_policy`: str; default "drop"; one of drop, refuse - CF rules that do not retarget: drop the rule, or refuse
- `unresolved_data_validation_policy`: str; default "drop"; one of drop, refuse - DV rules that do not retarget: drop the rule, or refuse
- `fail_on_limbo`: bool; default false - Halt when file hyperlinks, connections, query tables or OLE objects remain
- `report_file`: str - Optional path for the JSON report of fixes, refusals and limbo
- `name_cap`: int; default 10 - Max named items per class in the log

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Sever with the defaults: retarget what can be, freeze the rest, drop unresolvable names and rules

```yaml
settings:
  description: "Sever links in a workbook the user copied tabs into"

recipe:
  - step_description: "Sever external ties into a new file"
    processor_type: "sever_external_ties"
    # REQ - xlsx files to sever; never modified (variables substituted)
    files:
      - "review/merged_report.xlsx"
    # writes review/merged_report_severed_260914_153000.xlsx
```

### strict

Refuse anything that cannot be retargeted, so the file is only written when every reference resolved to a local sheet

```yaml
settings:
  description: "Retarget-only severing with a report"

recipe:
  - step_description: "Sever by retargeting only"
    processor_type: "sever_external_ties"
    files:
      - "review/merged_report.xlsx"
    # OPT - directory for the new file; default beside the source
    output_dir: "review/severed"
    # OPT - appended to the stem before the timestamp
    # Default value: "_severed"
    output_suffix: "_local"
    # OPT - strftime format for the timestamp
    # Default value: "%y%m%d_%H%M%S"
    timestamp_format: "%y%m%d_%H%M%S"
    # OPT - rewrite [N]Sheet! to Sheet! when that sheet exists locally
    # Default value: true
    retarget_if_possible: true
    # OPT - formula cells that do not retarget: freeze | refuse
    # Default value: freeze
    unresolved_formula_policy: "refuse"
    # OPT - defined names that do not retarget: drop | refuse
    # Default value: drop
    unresolved_defined_name_policy: "refuse"
    # OPT - CF rules that do not retarget: drop | refuse
    # Default value: drop
    unresolved_conditional_formatting_policy: "refuse"
    # OPT - DV rules that do not retarget: drop | refuse
    # Default value: drop
    unresolved_data_validation_policy: "refuse"
    # OPT - halt when file hyperlinks, connections, query tables or OLE remain
    # Default value: false
    fail_on_limbo: false
    # OPT - JSON report of fixes, refusals and limbo; directory must exist
    report_file: "review/severed/merged_report_sever.json"
    # OPT - max named items per class in the log
    # Default value: 10
    name_cap: 25
```

## Parameter notes

- `files` (required): xlsx paths to sever; variable substitution applies; never modified
- `output_dir`: Directory for the new files; must exist; default is the source directory
- `output_suffix` (default `_severed`): Filename fragment between the stem and the timestamp
- `timestamp_format` (default `%y%m%d_%H%M%S`): strftime format; an existing output of the same name is refused, never overwritten
- `retarget_if_possible` (default `True`): Attempt local retargeting before applying any unresolved policy
- `unresolved_formula_policy` (default `freeze`): freeze or refuse
- `unresolved_defined_name_policy` (default `drop`): drop or refuse
- `unresolved_conditional_formatting_policy` (default `drop`): drop or refuse
- `unresolved_data_validation_policy` (default `drop`): drop or refuse
- `fail_on_limbo` (default `False`): Treat limbo items as a failure after the file is written
- `report_file`: JSON report (one object for one file, a list for several); written even when the step refuses
- `name_cap` (default `10`): Maximum named items per class in the log; the report is never capped

