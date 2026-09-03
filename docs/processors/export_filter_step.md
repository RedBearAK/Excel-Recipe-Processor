# `export_filter_step`

**Family:** `export`

Generate copy-paste ready filter_data steps from reviewed filter terms

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `output_file`: str; REQUIRED
- `output_format`: str; default "yaml"; one of yaml, json
- `target_stage`: str; default "stg_data_to_filter" - source_stage of the generated step
- `output_stage`: str; default "stg_data_filtered" - save_to_stage of the generated step
- `acceptance_column`: str; default "User_Verified"
- `acceptance_values`: list of any; default ["KEEP", "YES", "TRUE"]
- `column_name_field`: str; default "Column_Name"
- `filter_term_field`: str; default "Filter_Term"
- `term_type_field`: str; default "Term_Type"
- `include_full_recipe`: bool; default true

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Generate complete YAML recipe from reviewed filter terms

```yaml
# Generate complete recipe from Excel-reviewed filter terms

settings:
  description: "Export filter step from manually reviewed filter terms"
  stages:
    - stage_name: "stg_filter_terms_reviewed"
      description: "Filter terms reviewed in Excel with KEEP/REJECT decisions"
      protected: false

recipe:
  # Previous steps would populate filter_terms_reviewed stage
  - # OPT - Human-readable step description
    # Default value: "Unnamed export_filter_step step"
    step_description: "Generate filter recipe from reviewed terms"
    # REQ - Must be "export_filter_step" for this processor type
    processor_type: "export_filter_step"
    # REQ - Stage containing reviewed filter terms with acceptance decisions
    source_stage: "stg_filter_terms_reviewed"
    # REQ - Output file path for generated recipe
    output_file: "generated_filter_recipe.yaml"
    # OPT - Stage name that filter should read from
    # Default value: "stg_data_to_filter"
    target_stage: "stg_raw_data_imported"
    # OPT - Stage name that filter should save to
    # Default value: "stg_data_filtered"
    output_stage: "stg_data_cleaned"
```

### step only

Generate just the filter step for insertion into existing recipes

```yaml
# Generate filter step only (not complete recipe) for copy-paste

settings:
  description: "Export filter step only for insertion into existing recipes"
  stages:
    - stage_name: "stg_reviewed_terms"
      description: "Filter terms with user acceptance decisions"
      protected: false

recipe:
  - step_description: "Export filter step for insertion into existing recipe"
    processor_type: "export_filter_step"
    source_stage: "stg_reviewed_terms"
    output_file: "filter_step_only.yaml"
    # OPT - Generate complete recipe or step only
    # Default value: true
    include_full_recipe: false
    # OPT - Description for the generated filter step
    # Default value: "Filter data using detected terms"
    step_description: "Remove cancelled and problematic records"
    target_stage: "stg_processed_data"
    output_stage: "stg_clean_data"
```

### json format

Generate JSON format for programmatic recipe creation

```yaml
# Generate JSON format for programmatic use or JSON-based recipes

settings:
  description: "Export filter configuration in JSON format"
  stages:
    - stage_name: "stg_filter_analysis_results"
      description: "Reviewed filter terms ready for export"
      protected: false

recipe:
  - step_description: "Generate JSON filter configuration"
    processor_type: "export_filter_step"
    source_stage: "stg_filter_analysis_results"
    output_file: "filter_config.json"
    # OPT - Output format selection
    # Default value: "yaml"
    output_format: "json"
    # OPT - Custom acceptance column name
    # Default value: "User_Verified"
    acceptance_column: "Keep_Filter"
    # OPT - Values that indicate acceptance
    # Default value: ["KEEP", "YES", "TRUE"]
    acceptance_values: ["KEEP", "ACCEPT", "Y"]
```

### custom column mapping

Handle custom column names in reviewed filter terms

```yaml
# Handle cases where reviewed data has different column names

settings:
  description: "Export filters with custom column mappings for different review formats"
  stages:
    - stage_name: "stg_custom_review_results"
      description: "Filter terms reviewed with custom column structure"
      protected: false

recipe:
  - step_description: "Export filters with custom column mapping"
    processor_type: "export_filter_step"
    source_stage: "stg_custom_review_results"
    output_file: "customized_filter_step.yaml"
    # OPT - Column containing user decisions
    # Default value: "User_Verified"
    acceptance_column: "Review_Decision"
    # OPT - Column containing source column names
    # Default value: "Column_Name"
    column_name_field: "Source_Column"
    # OPT - Column containing filter terms/values
    # Default value: "Filter_Term"
    filter_term_field: "Term_Value"
    # OPT - Column containing term classification
    # Default value: "Term_Type"
    term_type_field: "Filter_Type"
    # Custom acceptance values
    acceptance_values: ["APPROVED", "KEEP", "GOOD"]
```

### complete workflow

Complete workflow from detection through review to export

```yaml
# Complete workflow: detect → review → export → apply

settings:
  description: "Complete filter development workflow from detection to application"
  variables:
    review_file: "filter_terms_for_review.xlsx"
    reviewed_file: "filter_terms_reviewed.xlsx"
  stages:
    - stage_name: "stg_raw_data"
      description: "Original unfiltered data"
      protected: false
    - stage_name: "stg_filtered_reference"
      description: "Manually filtered reference data"
      protected: false
    - stage_name: "stg_detected_terms"
      description: "Auto-detected filter term candidates"
      protected: false
    - stage_name: "stg_reviewed_terms"
      description: "Human-reviewed filter terms"
      protected: false
    - stage_name: "stg_auto_filtered_data"
      description: "Data filtered using generated rules"
      protected: false

recipe:
  # Step 1: Auto-detect filter terms
  - step_description: "Auto-detect potential filter terms"
    processor_type: "filter_terms_detector"
    source_stage: "stg_raw_data"
    filtered_stage: "stg_filtered_reference"
    auto_detect_columns: true
    save_to_stage: "stg_detected_terms"
  
  # Step 2: Export for human review
  - step_description: "Export terms for Excel review"
    processor_type: "export_file"
    source_stage: "stg_detected_terms"
    output_file: "{review_file}"
  
  # Step 3: Import human-reviewed terms
  - step_description: "Import reviewed filter terms"
    processor_type: "import_file"
    input_file: "{reviewed_file}"
    save_to_stage: "stg_reviewed_terms"
  
  # Step 4: Generate filter recipe
  - step_description: "Generate filter recipe from reviewed terms"
    processor_type: "export_filter_step"
    source_stage: "stg_reviewed_terms"
    output_file: "auto_generated_filter.yaml"
    target_stage: "raw_data"
    output_stage: "stg_auto_filtered_data"
    step_description: "Auto-generated filter from human-reviewed terms"
```

## Parameter notes

- `source_stage` (required): Stage containing reviewed filter terms with acceptance decisions
- `output_file` (required): Path for generated YAML or JSON file (creates directories if needed)
- `target_stage` (default `stg_data_to_filter`): Stage name that generated filter should read from
- `output_stage` (default `stg_data_filtered`): Stage name that generated filter should save to
- `step_description` (default `Filter data using detected terms`): Human-readable description for the generated filter step
- `output_format` (default `yaml`): Output format for generated configuration
- `include_full_recipe` (default `True`): Generate complete recipe with settings, or just the filter step
- `acceptance_column` (default `User_Verified`): Column name containing user acceptance decisions
- `acceptance_values` (default `['KEEP', 'YES', 'TRUE']`): Values indicating user accepted the filter term
- `column_name_field` (default `Column_Name`): Column containing source column names for filtering
- `filter_term_field` (default `Filter_Term`): Column containing the filter terms/values
- `term_type_field` (default `Term_Type`): Column containing term classification (categorical_value or text_ngram)

