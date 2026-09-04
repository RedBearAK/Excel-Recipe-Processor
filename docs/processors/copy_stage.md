# `copy_stage`

**Family:** `transform`

Copy a stage to another named stage

## Notes

- **keys**: source_stage in, save_to_stage out - the standard data-flow pair; plus optional description and overwrite

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `description`: str; default ""
- `overwrite`: bool; default false

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple stage duplication for backup purposes

```yaml
settings:
  description: "Create backup of imported data before processing"
  stages:
    - stage_name: "stg_raw_import"
      description: "Original imported data"
      protected: false
    - stage_name: "stg_raw_backup"
      description: "Backup copy of raw data"
      protected: true

recipe:
  # Import data
  - step_description: "Import customer data"
    processor_type: "import_file"
    input_file: "data/customers.xlsx"
    save_to_stage: "stg_raw_import"
  
  # Create backup copy
  - # OPT - Human-readable description of the copy operation
    # Default value: "Unnamed copy_stage step"
    step_description: "Create backup of raw data"
    # REQ - Must be "copy_stage" for this processor type
    processor_type: "copy_stage"
    # REQ - Source stage to copy data from
    source_stage: "stg_raw_import"
    # REQ - Destination for the copy
    save_to_stage: "stg_raw_backup"
```

### workflow branching

Create copies for different processing branches

```yaml
settings:
  description: "Test different filtering approaches on same data"
  stages:
    - stage_name: "stg_cleaned_data"
      description: "Data after cleaning operations"
      protected: false
    - stage_name: "stg_branch_strict"
      description: "Copy for strict filtering"
      protected: false
    - stage_name: "stg_branch_lenient"
      description: "Copy for lenient filtering"
      protected: false
    - stage_name: "stg_strict_results"
      description: "Results from strict filtering"
      protected: false
    - stage_name: "stg_lenient_results"
      description: "Results from lenient filtering"
      protected: false

recipe:
  # Assume cleaned_data already exists
  
  # Create copy for strict filtering branch
  - # OPT - Step description
    step_description: "Copy data for strict filtering approach"
    # REQ - Processor type
    processor_type: "copy_stage"
    # REQ - Stage to copy from
    source_stage: "stg_cleaned_data"
    # REQ - New stage name for the copy
    save_to_stage: "stg_branch_strict"
    # OPT - Detailed description for the copied stage
    # Default value: "Copy of {source_stage}"
    description: "Working copy for testing strict business rules"
  
  # Create copy for lenient filtering branch
  - step_description: "Copy data for lenient filtering approach"
    processor_type: "copy_stage"
    source_stage: "stg_cleaned_data"
    save_to_stage: "stg_branch_lenient"
    description: "Working copy for testing relaxed business rules"
  
  # Process each branch differently
  - step_description: "Apply strict filters"
    processor_type: "filter_data"
    source_stage: "stg_branch_strict"
    save_to_stage: "stg_strict_results"
    filters:
      - column: "Score"
        condition: "greater_than"
        value: 90
  
  - step_description: "Apply lenient filters"
    processor_type: "filter_data"
    source_stage: "stg_branch_lenient"
    save_to_stage: "stg_lenient_results"
    filters:
      - column: "Score"
        condition: "greater_than"
        value: 70
```

### checkpoint recovery

Create checkpoints for complex multi-step processing

```yaml
settings:
  description: "Complex data processing with recovery checkpoints"
  variables:
    min_value: "1000"
    category: "premium"
  stages:
    - stage_name: "stg_initial_data"
      description: "Starting dataset"
      protected: false
    - stage_name: "stg_checkpoint_1"
      description: "After initial cleaning"
      protected: true
    - stage_name: "stg_checkpoint_2"
      description: "After enrichment"
      protected: true
    - stage_name: "stg_checkpoint_3"
      description: "After filtering"
      protected: true
    - stage_name: "stg_final_output"
      description: "Completed processing"
      protected: false

recipe:
  # Step 1: Initial processing
  - step_description: "Clean and validate data"
    processor_type: "clean_data"
    source_stage: "stg_initial_data"
    save_to_stage: "stg_cleaned"
    rules:
      - columns: "*"
        action: "strip_whitespace"
  
  # Checkpoint 1: Save after cleaning
  - # OPT - Step description
    step_description: "Checkpoint: Post-cleaning"
    # REQ - Processor type
    processor_type: "copy_stage"
    # REQ - Source stage
    source_stage: "stg_cleaned"
    # REQ - Target checkpoint stage
    save_to_stage: "stg_checkpoint_1"
    # OPT - Allow overwriting during development
    # Default value: false
    overwrite: true
    # OPT - Document checkpoint purpose
    description: "Recovery point after data cleaning phase"
  
  # Step 2: Data enrichment
  - step_description: "Add calculated fields"
    processor_type: "add_calculated_column"
    source_stage: "stg_cleaned"
    save_to_stage: "stg_enriched"
    new_column: "Total"
    calculation:
      pandas_formula: "{col:Price} * {col:Quantity}"
  
  # Checkpoint 2: Save after enrichment
  - step_description: "Checkpoint: Post-enrichment"
    processor_type: "copy_stage"
    source_stage: "stg_enriched"
    save_to_stage: "stg_checkpoint_2"
    overwrite: true
    description: "Recovery point after enrichment phase"
```

### versioning

Create versioned copies of processed data

```yaml
settings:
  description: "Maintain versions of processed data"
  variables:
    report_date: "2024-12-15"
    version: "v2"
  stages:
    - stage_name: "stg_current_report"
      description: "Latest processed report data"
      protected: false
    - stage_name: "stg_report_v1"
      description: "Version 1 of report"
      protected: true
    - stage_name: "stg_report_v2"
      description: "Version 2 of report"
      protected: true
    - stage_name: "stg_report_archive_{report_date}"
      description: "Date-stamped archive"
      protected: true

recipe:
  # Save current version before updates
  - # OPT - Step description with variable
    step_description: "Archive current report as {version}"
    # REQ - Processor type
    processor_type: "copy_stage"
    # REQ - Current report stage
    source_stage: "stg_current_report"
    # REQ - Versioned target using variable
    save_to_stage: "stg_report_{version}"
    # OPT - Prevent overwriting existing versions
    # Default value: false
    overwrite: false
    # OPT - Version description
    description: "Report {version} - includes {report_date} updates"
  
  # Create date-stamped archive
  - step_description: "Create date-stamped archive"
    processor_type: "copy_stage"
    source_stage: "stg_current_report"
    save_to_stage: "stg_report_archive_{report_date}"
    description: "Permanent archive for {report_date}"
```

### iterative development

Support iterative recipe development with overwritable copies

```yaml
settings:
  description: "Development workflow with reusable stage names"
  stages:
    - stage_name: "stg_test_input"
      description: "Test data for development"
      protected: false
    - stage_name: "stg_working_copy"
      description: "Reusable working stage"
      protected: false
    - stage_name: "stg_test_output"
      description: "Results of test processing"
      protected: false

recipe:
  # Create working copy that can be overwritten
  - # OPT - Step description
    step_description: "Create fresh working copy for testing"
    # REQ - Processor type
    processor_type: "copy_stage"
    # REQ - Test data source
    source_stage: "stg_test_input"
    # REQ - Working stage (overwritable)
    save_to_stage: "stg_working_copy"
    # OPT - Enable overwrite for iterative development
    # Default value: false
    # Set to true during development, false in production
    overwrite: true
    # OPT - Development note
    description: "Temporary working copy - safe to overwrite"
```

## Parameter notes

- `source_stage` (required): Stage to copy data from (must exist and be declared in settings.stages)
- `target_stage` (required): Name for the new stage copy (must be declared in settings.stages)
- `overwrite` (default `False`): Allow overwriting existing stage with same name
- `description` (default `Copy of {source_stage}`): Human-readable description of the copied stage's purpose

