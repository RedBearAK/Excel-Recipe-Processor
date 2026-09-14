# `audit_external_ties`

**Family:** `file_ops`

Inventory every external tie in closed xlsx files, read-only

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `files`: list of str; REQUIRED - xlsx paths to audit; never modified
- `report_file`: str - Optional path for the full inventory as JSON
- `fail_on_ties`: bool; default false - Halt the recipe when any file has a tie
- `name_cap`: int; default 10 - Max named items per class in the log

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Audit one file and print the inventory to the log

```yaml
settings:
  description: "Audit a workbook the user copied tabs into"

recipe:
  - step_description: "Inventory external ties in the merged workbook"
    processor_type: "audit_external_ties"
    # REQ - xlsx files to audit; never modified (variables substituted)
    files:
      - "review/merged_report.xlsx"
```

### report

Write the full inventory as sorted JSON for diffing across runs, and halt if anything is tied

```yaml
settings:
  description: "Gate an archive copy on having no external ties"

recipe:
  - step_description: "Audit and gate the archive copy"
    processor_type: "audit_external_ties"
    files:
      - "archive/260914_VMS_processed.xlsx"
    # OPT - path for the JSON inventory; directory must exist
    report_file: "archive/260914_VMS_processed_ties.json"
    # OPT - halt the recipe when any audited file has a tie
    # Default value: false
    fail_on_ties: true
    # OPT - max named items per class in the log
    # Default value: 10
    name_cap: 25
```

## Parameter notes

- `files` (required): xlsx paths to audit; variable substitution applies; never modified
- `report_file`: Write the inventory as sorted JSON (one object for one file, a list for several); the directory must already exist
- `fail_on_ties` (default `False`): Raise after the report when any audited file has an external tie
- `name_cap` (default `10`): Maximum named items per class in the log; the JSON report is never capped

