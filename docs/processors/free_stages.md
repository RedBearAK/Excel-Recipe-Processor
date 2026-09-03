# `free_stages`

**Family:** `base`

Delete named stages mid-run to reclaim memory once their consumers have run

## Notes

- **reporting**: logs stage count and approximate MB returned

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `stages`: stage_release; REQUIRED - Stages to release from memory
- `on_missing`: str; default "error"; one of error, warn, skip

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Free stages after their last reader

```yaml
# Stages normally live until the run ends. For ordinary data that history
# is cheap; for large inputs the retained full-size frames become the
# bulk of the process footprint. Place a free_stages step AFTER the last
# step that reads each listed stage. A misplaced deletion cannot corrupt
# data - a later step needing a freed stage fails loudly, naming it.
#
# --dump-stage is unaffected: dumps are written the moment a stage is
# saved, long before any free_stages step runs.

settings:
  description: "Pipeline that trims its own memory"
  stages:
    - stage_name: "stg_import_raw"
      description: "Raw import"
      protected: false
    - stage_name: "stg_cleaned"
      description: "Cleaned data"
      protected: false

recipe:
  - step_description: "Import"
    processor_type: "import_file"
    input_file: "big_download.xlsx"
    save_to_stage: "stg_import_raw"

  - step_description: "Clean"
    processor_type: "clean_data"
    source_stage: "stg_import_raw"
    rules:
      - columns: "*"
        action: "strip_whitespace"
    save_to_stage: "stg_cleaned"

  - step_description: "Free the raw import"
    processor_type: "free_stages"
    # REQ - Stages to delete; each must exist (typo protection)
    stages:
      - "stg_import_raw"
```

### tolerant

Tolerating a stage that legitimately may not exist

```yaml
# on_missing: skip is for recipes where a stage is only sometimes
# created. The default (error) treats an absent stage as a typo.

settings:
  description: "Conditional stage cleanup"
  stages:
    - stage_name: "stg_maybe_created"
      description: "Only exists on some runs"
      protected: false

recipe:
  - step_description: "Free if present"
    processor_type: "free_stages"
    stages:
      - "stg_maybe_created"
    # OPT - "error" (default) or "skip"
    on_missing: "skip"
```

## Parameter notes

- `stages` (required): Stage names to delete. Protected stages refuse deletion.
- `on_missing` (default `error`): error halts on an absent stage (typo guard); skip tolerates it with a log note

