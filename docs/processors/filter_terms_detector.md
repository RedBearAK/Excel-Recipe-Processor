# `filter_terms_detector`

**Family:** `transform`

Detect candidate filter terms by n-gram comparison of raw vs filtered data

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `filtered_stage`: stage_in; REQUIRED
- `text_columns`: list of str - Column names
- `categorical_columns`: list of str - Column names
- `exclude_columns`: list of str - Column names
- `auto_detect_columns`: bool; default false
- `min_frequency`: int; default 2
- `max_features`: int; default 10000
- `ngram_range`: any; default [1, 4] - [min_n, max_n]
- `score_threshold`: number; default 0.1
- `custom_stop_words`: list of str

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple analysis of text columns to identify filter terms

```yaml
settings:
  description: "Detect filter terms used in manual data processing"
  stages:
    - stage_name: "stg_raw_data_imported_from_database"
      description: "Original unfiltered data from database export"
      protected: false
    - stage_name: "stg_filtered_data_manual_processing_complete"
      description: "Manually filtered final dataset"
      protected: false
    - stage_name: "stg_filter_terms_analysis_candidates_ranked"
      description: "Detected filter terms ranked by confidence"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_terms_detector step"
    step_description: "Analyze manual filtering to identify reusable filter terms"
    # REQ - Must be "filter_terms_detector" for this processor type
    processor_type: "filter_terms_detector"
    # REQ - Stage containing original unfiltered data
    source_stage: "stg_raw_data_imported_from_database"
    # REQ - Stage containing manually filtered final data
    filtered_stage: "stg_filtered_data_manual_processing_complete"
    # REQ - Text columns to analyze for filter patterns
    text_columns: ["notes", "description", "comments"]
    # REQ - Output stage for filter term candidates
    save_to_stage: "stg_filter_terms_analysis_candidates_ranked"
```

### advanced

Comprehensive analysis with categorical and text columns, custom parameters

```yaml
settings:
  description: "Advanced filter terms detection with custom parameters and multiple column types"
  stages:
    - stage_name: "stg_raw_sales_data_complete_export"
      description: "Complete raw sales data export"
      protected: false
    - stage_name: "stg_filtered_sales_data_final_report_ready"
      description: "Sales data after manual filtering for quarterly report"
      protected: false
    - stage_name: "stg_sales_filter_terms_comprehensive_analysis"
      description: "Comprehensive filter terms analysis for sales data"
      protected: false

recipe:
  - step_description: "Comprehensive filter terms detection for sales report automation"
    processor_type: "filter_terms_detector"
    source_stage: "stg_raw_sales_data_complete_export"
    filtered_stage: "stg_filtered_sales_data_final_report_ready"
    # REQ - Text columns for n-gram analysis
    text_columns: ["sales_notes", "customer_feedback", "deal_description"]
    # OPT - Categorical columns for simple value comparison
    # Default value: [] (no categorical analysis)
    categorical_columns: ["deal_status", "priority_level", "sales_stage"]
    # OPT - N-gram range for text analysis
    # Default value: (1, 4) (1-4 word phrases)
    ngram_range: [1, 5]
    # OPT - Minimum frequency for term consideration
    # Default value: 2 (must appear at least twice)
    min_frequency: 3
    # OPT - Maximum number of n-grams to analyze
    # Default value: 10000
    max_features: 15000
    # OPT - Minimum score threshold for inclusion in results
    # Default value: 0.1
    score_threshold: 0.2
    # OPT - Additional stop words to ignore
    # Default value: [] (only standard English stop words)
    custom_stop_words: ["corp", "inc", "ltd", "llc", "company"]
    save_to_stage: "stg_detected_terms"
```

### text only

Focus only on text column analysis for notes fields

```yaml
settings:
  description: "Analyze only text fields to find filtering patterns in notes"
  stages:
    - stage_name: "stg_raw_project_data_with_extensive_notes"
      description: "Raw project data with detailed notes fields"
      protected: false
    - stage_name: "stg_filtered_project_data_notes_cleaned"
      description: "Project data after manual note-based filtering"
      protected: false
    - stage_name: "stg_project_filter_terms_text_analysis_only"
      description: "Filter terms detected from text analysis only"
      protected: false

recipe:
  - step_description: "Detect filter terms from project notes fields only"
    processor_type: "filter_terms_detector"
    source_stage: "stg_raw_project_data_with_extensive_notes"
    filtered_stage: "stg_filtered_project_data_notes_cleaned"
    text_columns: ["project_notes", "status_comments", "risk_assessment"]
    # No categorical_columns specified - text analysis only
    ngram_range: [1, 3]  # Shorter phrases for project notes
    min_frequency: 2
    score_threshold: 0.15
    save_to_stage: "stg_detected_terms"
```

### categorical only

Focus only on categorical columns for simple filter detection

```yaml
settings:
  description: "Analyze categorical fields to identify filtered values"
  stages:
    - stage_name: "stg_raw_customer_data_all_statuses"
      description: "Complete customer data with all status values"
      protected: false
    - stage_name: "stg_filtered_customer_data_active_customers_only"
      description: "Customer data filtered to active customers"
      protected: false
    - stage_name: "stg_customer_filter_terms_categorical_analysis"
      description: "Categorical filter terms detected"
      protected: false

recipe:
  - step_description: "Identify categorical filters applied to customer data"
    processor_type: "filter_terms_detector"
    source_stage: "stg_raw_customer_data_all_statuses"
    filtered_stage: "stg_filtered_customer_data_active_customers_only"
    # Empty text_columns since we're only doing categorical analysis
    text_columns: []
    categorical_columns: ["customer_status", "account_type", "region", "priority"]
    # Categorical analysis doesn't use n-gram parameters
    score_threshold: 0.0  # Include all categorical differences
    save_to_stage: "stg_detected_terms"
```

## Parameter notes

- `source_stage` (required): Stage containing the original unfiltered dataset
- `filtered_stage` (required): Stage containing the manually filtered final dataset
- `text_columns` (required): List of text column names to analyze for filter terms using n-gram analysis
- `categorical_columns` (default `[]`): List of categorical column names to analyze for removed values
- `ngram_range` (default `[1, 4]`): Range of n-gram sizes to analyze [min_n, max_n], e.g. [1,4] analyzes 1-4 word phrases (converted to tuple internally)
- `min_frequency` (default `2`): Minimum frequency required for a term to be considered (reduces noise)
- `max_features` (default `10000`): Maximum number of n-grams to analyze (performance tuning)
- `score_threshold` (default `0.1`): Minimum confidence score for inclusion in results (0.0-1.0, converted to 0-100% in output)
- `custom_stop_words` (default `[]`): Additional words to ignore beyond standard English stop words

