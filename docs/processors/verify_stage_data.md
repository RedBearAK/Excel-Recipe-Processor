# `verify_stage_data`

**Family:** `transform` - a check: reads a stage, writes nothing

Check a stage row values against rules; warn or halt per rule

## Notes

- **vocabulary**: the full filter_data condition set, borrowed live so the two cannot drift
- **family**: transform check: reads source_stage, writes nothing

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `rules`: list_of_mappings; REQUIRED
  - `column`: str; REQUIRED
  - `condition`: str; REQUIRED - Any filter_data condition
  - `value`: any
  - `case_sensitive`: bool; default false
  - `stage_name`: stage_in - For in_stage / not_in_stage conditions
  - `stage_column`: str
  - `stage_key_column`: str
  - `stage_value_column`: str
  - `key_column`: str
  - `comparison_operator`: str
  - `severity`: str; default "warn"; one of warn, halt
  - `description`: str - Replaces the generated expectation line

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Warn on data gaps after enrichment, halt on the unforgivable one

```yaml
settings:
  description: "Verify the enriched data before export"
  stages:
    - stage_name: "stg_enriched"
      description: "Fully enriched rows"
      protected: false

recipe:
  - # Each rule: column + condition (+ value where the condition takes
    # one) + optional severity and description. warn is the default:
    # the run continues, the log shows the count and a sample, and the
    # run-end summary line tallies it.
    step_description: "Verify the enrichment held"
    # REQ - Must be "verify_data" for this processor type
    processor_type: "verify_stage_data"
    # REQ - Stage this step reads
    source_stage: "stg_enriched"
    rules:
      - column: "SHIP REF"
        condition: "not_empty"
        # OPT - "warn" (default) or "halt"
        severity: "halt"
        # OPT - The line pass/warn/halt messages lead with
        description: "Every row must have a resolved SHIP REF"
      - column: "Price"
        condition: "greater_than"
        value: 0
      - column: "Booking"
        condition: "not_empty"
```

### referential

Values must exist in a lookup stage (unresolved-lookup guard)

```yaml
settings:
  description: "Guard against carriers absent from the carriers table"
  stages:
    - stage_name: "stg_enriched"
      description: "Enriched rows"
      protected: false
    - stage_name: "stg_lookup_carriers"
      description: "The carriers lookup table"
      protected: false

recipe:
  - # in_stage is filter_data's referential condition: every value in
    # the column must appear in the named stage's column. The classic
    # use: after a lookup-based enrichment, prove nothing fell through.
    step_description: "Every carrier must exist in the lookup"
    processor_type: "verify_stage_data"
    source_stage: "stg_enriched"
    rules:
      - column: "Carrier"
        condition: "in_stage"
        stage_name: "stg_lookup_carriers"
        stage_column: "Carrier"
        description: "Carrier must exist in the carriers lookup"
```

## Parameter notes

- `source_stage` (required): Stage this step reads (a check: nothing is written). Rule-level stage references use filter_data's 'stage_name' - a different role, a different key.
- `rules` (required): Expectations every row must satisfy. Any filter_data condition works, with that condition's own parameters (value, case_sensitive, stage_name/stage_column for in_stage, ...).

