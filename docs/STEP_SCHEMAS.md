# Recipe step schemas

Generated from the processors' declared schemas. Keys not listed here are refused at recipe load.

Convention: An evaluated string never sits under a bare key: pandas_formula, pandas_rules, pandas_default, excel_formula name their dialect. Column-name lists are strings, never positions. column_names / column_refs pairs exist only in file_ops.

## Families

- **transform** - Reads a stage, returns a stage; knows columns only by name. Contributes: `confirm_stage_replacement`, `on_error`, `processor_type`, `save_to_stage`, `source_stage`, `step_description`. Selector constructs offered: name_list
- **import** - Creates a stage from outside the pipeline. Contributes: `confirm_stage_replacement`, `on_error`, `processor_type`, `save_to_stage`, `step_description`. Selector constructs offered: name_list
- **export** - Consumes a stage into a file. Contributes: `on_error`, `processor_type`, `source_stage`, `step_description`. Selector constructs offered: name_list
- **file_ops** - Operates on a workbook in place; positional refs are legal here. Contributes: `on_error`, `processor_type`, `step_description`. Selector constructs offered: name_list, name_ref_pair, typed_item_list
- **base** - Steps that fit no family (stage utilities); declare their stage keys themselves. Contributes: `on_error`, `processor_type`, `step_description`. Selector constructs offered: name_list

## Kinds

- `str`: text
- `int`: integer (not bool)
- `number`: int or float
- `bool`: true/false
- `list`: list of item_kind
- `mapping`: closed mapping with its own keys
- `open_mapping`: mapping whose keys are the author's data, not vocabulary
- `list_of_mappings`: list, each element a closed mapping
- `any`: unchecked
- `stage_in`: name of a stage this step reads
- `stage_out`: name of a stage this step writes
- `stage_release`: list of stage names this step frees

## Processors

### `add_calculated_column`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `new_column`: str; REQUIRED - The calculated column
- `spill_columns`: list of str - Further columns the same calculation fills, in order
- `calculation_type`: str; default "expression"; one of expression, first_match, concat, conditional, math, date, text, constant, row_number
- `overwrite`: bool; default false
- when `calculation_type` = `expression`:
  - `calculation`: mapping; REQUIRED
    - `pandas_formula`: str - pandas text with {col:Name} references
    - `formula_components`: list of any - Structured column / operator / value parts
    - at least one of: `pandas_formula`, `formula_components`
- when `calculation_type` = `first_match`:
  - `calculation`: mapping; REQUIRED
    - `pandas_rules`: list_of_mappings; REQUIRED
      - `when`: str; REQUIRED - pandas predicate, one boolean per row
      - `then`: list of any; REQUIRED - One slot per declared column: expression, quoted literal, number, or ""
    - `pandas_default`: list of any; REQUIRED - Slots when no rule matches; same shape as a then
- when `calculation_type` = `concat`:
  - `calculation`: mapping; REQUIRED
    - `columns`: list of str; REQUIRED - Column names
    - `separator`: str; default ""
- when `calculation_type` = `conditional`:
  - `calculation`: mapping; REQUIRED
    - `condition_column`: str; REQUIRED
    - `condition`: str; REQUIRED; one of equals, greater_than, less_than, contains, is_null, not_null
    - `condition_value`: any
    - `value_if_true`: any; REQUIRED
    - `value_if_false`: any; REQUIRED
- when `calculation_type` = `math`:
  - `calculation`: mapping; REQUIRED
    - `operation`: str; REQUIRED; one of add, subtract, multiply, divide, sum, mean, min, max
    - `column1`: str
    - `column2`: str
    - `columns`: list of str - Column names
- when `calculation_type` = `date`:
  - `calculation`: mapping; REQUIRED
    - `operation`: str; REQUIRED; one of days_between
    - `start_date_column`: str; REQUIRED
    - `end_date_column`: str; REQUIRED
- when `calculation_type` = `text`:
  - `calculation`: mapping; REQUIRED
    - `operation`: str; REQUIRED; one of length, upper, lower, extract_numbers, substring
    - `column`: str; REQUIRED
    - `start`: int; default 0
    - `length`: int
- when `calculation_type` = `constant`:
  - `calculation`: mapping; REQUIRED
    - `value`: any; REQUIRED
- when `calculation_type` = `row_number`:
  - `calculation`: mapping; REQUIRED
    - `start`: int; default 1

### `add_subtotals`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `group_by`: list of str; REQUIRED - Column names
- `subtotal_columns`: list of str; REQUIRED - Column names
- `subtotal_functions`: list of str; default ["sum"]
- `subtotal_label`: str; default "Subtotal"
- `position`: str; default "after_group"; one of after_group, before_group
- `preserve_totals`: bool; default true

### `aggregate_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `group_by`: list of str - Column names
- `aggregations`: list_of_mappings
  - `column`: str; REQUIRED
  - `function`: str; REQUIRED; one of sum, count, mean, median, min, max, std, var, nunique
  - `output_name`: str - Default: column_function
- `aggregation_source`: mapping
  - `type`: str; REQUIRED; one of file, stage, lookup, table
  - `filename`: str
  - `sheet`: any
  - `format`: str
  - `encoding`: str
  - `separator`: str
  - `stage_name`: stage_in
  - `lookup_stage`: stage_in
  - `lookup_key`: str
  - `data_key`: str
  - `group_by_column`: str
  - `aggregations_column`: str
  - `filter_condition`: any
- `keep_group_columns`: bool; default true
- `reset_index`: bool; default true
- `sort_by_groups`: bool; default true
- at least one of: `aggregations`, `aggregation_source`
- at least one of: `group_by`, `aggregation_source`

### `clean_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `rules`: list_of_mappings; REQUIRED
  - `columns`: any; REQUIRED - Column name list, or "*" for every column
  - `action`: str; REQUIRED; one of replace, regex_replace, uppercase, lowercase, title_case, strip_whitespace, remove_special_chars, fix_numeric, fix_dates, coerce_datetime, fill_empty, remove_duplicates, standardize_values, remove_invisible_chars, normalize_whitespace, blank_repeats
  - `old_value`: any
  - `new_value`: any
  - `pattern`: str
  - `replacement`: str; default ""
  - `mapping`: open_mapping
  - `case_sensitive`: bool; default false
  - `fill_value`: any
  - `fill_na`: any
  - `parse_format`: str
  - `format`: str
  - `method`: str
  - `preserve_original_on_failure`: bool
  - `subset_column`: bool
  - `condition_column`: str
  - `condition`: str
  - `condition_value`: any
  - `column`: str - Group column for blank_repeats

### `columns_to_rows`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `id_columns`: list of str - Column names
- `value_columns`: list of str - Column names
- `labels_to`: str; default "Field"
- `values_to`: str; default "Value"
- `drop_empty_values`: bool; default false

### `combine_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `combine_type`: str; REQUIRED; one of vertical_stack, horizontal_concat
- `column_handling`: str; one of require_matching_columns, allow_mismatched_columns
- `data_sources`: list_of_mappings; REQUIRED
  - `insert_from_stage`: stage_in
  - `insert_blank_rows`: int
  - `insert_blank_cols`: int
  - `retain_column_names`: bool
  - at least one of: `insert_from_stage`, `insert_blank_rows`, `insert_blank_cols`

### `conditional_format`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
- `rules`: list_of_mappings; REQUIRED
  - `when_cell`: mapping
    - `column_names`: list of str; REQUIRED - Column names
    - `condition`: str; REQUIRED; one of between, contains, duplicates, ends_with, equals, greater_equal, greater_than, is_empty, less_equal, less_than, not_between, not_contains, not_empty, not_equals, starts_with, unique
    - `value`: any - Operand; a two-item list for between / not_between
  - `when_formula`: str - Excel formula written for the top-left cell of the target
  - `color_scale`: mapping
    - `min_color`: str
    - `mid_color`: str
    - `max_color`: str
    - `column_names`: list of str - Column names
    - `range`: str
  - `data_bar`: mapping
    - `color`: str
    - `column_names`: list of str - Column names
    - `range`: str
  - `style`: mapping
    - `fill`: str
    - `font_color`: str
    - `bold`: bool
    - `italic`: bool
  - `apply_to`: str; one of entire_row
  - `column_names`: list of str - Target columns for when_* rules
  - `range`: str - Literal target range like A2:B99
  - `stop_if_true`: bool; default false
  - at least one of: `when_cell`, `when_formula`, `color_scale`, `data_bar`

### `copy_stage`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `description`: str; default ""
- `overwrite`: bool; default false

### `create_stage`  - family `import`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `data`: mapping; REQUIRED
  - `format`: str; REQUIRED; one of list, table, dictionary
  - `values`: any
  - `columns`: any
  - `rows`: any
  - `data`: any
  - `column`: str
  - `key_column`: str
  - `value_column`: str
- `description`: str; default ""
- `overwrite`: bool; default false

### `debug_breakpoint`  - family `export`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `message`: str
- `output_path`: str
- `filename_prefix`: str
- `include_timestamp`: bool; default true
- `show_sample`: bool; default true
- `sample_rows`: int; default 5

### `declare_dynamic_formulas`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `input_file`: str; REQUIRED
- `output_file`: str
- `extra_functions`: list of str

### `deduplicate_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `key_columns`: list of str; REQUIRED - Column names
- `keep`: str; default "first"; one of first, last, none
- `conflicts_file`: str
- `save_conflicts_to_stage`: stage_out

### `diff_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `reference_stage`: stage_in; REQUIRED
- `key_columns`: list of str; REQUIRED - Column names
- `exclude_columns`: list of str - Column names
- `handle_deleted_rows`: str; default "include"; one of include, exclude
- `include_json_details`: bool; default false
- `create_filtered_stages`: bool; default false
- `filtered_stage_prefix`: str; default "stg_diff" - Prefix for the filtered stages created when create_filtered_stages is true

### `excel_data_validation`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `validations`: list_of_mappings; REQUIRED
  - `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
  - `apply_to_ranges`: list of str; REQUIRED - A1 ranges the validation covers
  - `validation_type`: str; REQUIRED; one of list, whole_number, decimal, date, time, text_length, custom
  - `values_list`: list of any - list: literal choices
  - `list_from_named_range`: str - list: a defined name
  - `list_from_spill_ref`: str - list: a spill anchor like Cust_List!$A$2#
  - `excel_formula`: str - custom: the validation formula
  - `operator`: str; one of between, not_between, equal, not_equal, greater_than, less_than, greater_than_or_equal, less_than_or_equal
  - `minimum`: any
  - `maximum`: any
  - `compare_to`: any
  - `allow_blank`: bool; default true
  - `show_dropdown`: bool; default true
  - `input_prompt`: mapping
    - `title`: str
    - `message`: str
    - `style`: str; one of stop, warning, information
  - `error_alert`: mapping
    - `title`: str
    - `message`: str
    - `style`: str; one of stop, warning, information

### `export_file`  - family `export`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `output_file`: str; REQUIRED
- `sheet_name`: str; default "Data"
- `sheets_to_create`: list_of_mappings
  - `sheet_name`: str; REQUIRED
  - `data_source`: stage_in; REQUIRED
- `template_file`: str
- `format`: str; one of xlsx, csv, tsv
- `encoding`: str; default "utf-8"
- `separator`: str; default ","
- `create_backup`: bool; default true
- `delete_backups_beyond`: int

### `export_filter_step`  - family `export`

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

### `fill_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `columns`: list of str; REQUIRED - Column names
- `fill_method`: str; REQUIRED
- `fill_value`: any
- `source_column`: str
- `old_value`: any
- `limit`: int
- `inplace`: bool; default false
- `conditions`: list_of_mappings
  - `condition_column`: str; REQUIRED
  - `condition_type`: str; REQUIRED
  - `condition_value`: any
  - `fill_value`: any

### `filter_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `filters`: list_of_mappings
  - `column`: str; REQUIRED
  - `condition`: str; REQUIRED; one of contains, contains_all_in_list, contains_any_in_list, ends_with, ends_with_any_in_list, equals, equals_any_in_list, greater_equal, greater_equal_max_in_list, greater_equal_min_in_list, greater_than, greater_than_max_in_list, greater_than_min_in_list, in_list, in_stage, is_empty, less_equal, less_equal_max_in_list, less_equal_min_in_list, less_than, less_than_max_in_list, less_than_min_in_list, not_contains, not_contains_any_in_list, not_empty, not_ends_with, not_equals, not_equals_any_in_list, not_in_list, not_in_stage, not_starts_with, stage_comparison, starts_with, starts_with_any_in_list
  - `value`: any
  - `case_sensitive`: bool; default false
  - `comparison_operator`: str
  - `key_column`: str
  - `stage_name`: stage_in - For in_stage / not_in_stage
  - `stage_column`: str
  - `stage_key_column`: str
  - `stage_value_column`: str
- `pandas_expression`: str - pandas query text; alternative to filters
- at least one of: `filters`, `pandas_expression`

### `filter_terms_detector`  - family `transform`

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

### `flush_workbooks`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy

### `format_excel`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `formatting`: list_of_mappings; REQUIRED
  - `sheet_names`: list of str; REQUIRED - Tab names or ?sheet_NNN? tokens
  - `apply_templates`: list of str
  - `auto_fit_columns`: bool
  - `autofit_scan_rows`: int
  - `min_column_width`: number
  - `max_column_width`: number
  - `header_row`: int; default 1
  - `header_bold`: bool
  - `header_background`: bool
  - `header_background_color`: str
  - `header_text_color`: str
  - `header_font_size`: number
  - `header_alignment_horizontal`: str; one of left, center, right, justify, distributed
  - `header_alignment_vertical`: str; one of top, center, bottom, justify, distributed
  - `general_text_color`: str
  - `general_font_size`: number
  - `general_font_name`: str
  - `general_alignment_horizontal`: str; one of left, center, right, justify, distributed
  - `general_alignment_vertical`: str; one of top, center, bottom, justify, distributed
  - `freeze_top_row`: bool
  - `freeze_panes`: str
  - `auto_filter`: bool
  - `column_formats`: list_of_mappings
    - `column_names`: list of str - Header NAME strings this rule styles
    - `column_refs`: list of str - Positional Excel refs like A or BQ - never header names
    - `number_format`: str
    - `alignment_horizontal`: str; one of left, center, right, justify, distributed
    - `alignment_vertical`: str; one of top, center, bottom, justify, distributed
    - `wrap_text`: bool
    - `font_color`: str
    - `font_bold`: bool
    - `font_italic`: bool
    - `font_size`: number
    - `font_name`: str
    - `font_underline`: any
    - `font_strikethrough`: bool
    - `background_color`: str
    - `border_style`: str
    - `border_color`: str
    - `make_hyperlinks`: str - e.g. file_paths
    - `hyperlink_color`: str
    - `header_font_color`: str
    - `header_background_color`: str
    - `header_bold`: bool
    - `width`: number - Explicit column width
    - `whole_column`: bool; default false - Column-dimension style, for cells Excel creates at calculation time
    - at least one of: `column_names`, `column_refs`
  - `cell_formats`: list_of_mappings
    - `cells`: list of str; REQUIRED - A1-style cells or ranges, e.g. ["B2"] or ["A4:D4"]
    - `number_format`: str
    - `alignment_horizontal`: str; one of left, center, right, justify, distributed
    - `alignment_vertical`: str; one of top, center, bottom, justify, distributed
    - `wrap_text`: bool
    - `font_color`: str
    - `font_bold`: bool
    - `font_italic`: bool
    - `font_size`: number
    - `font_name`: str
    - `font_underline`: any
    - `font_strikethrough`: bool
    - `background_color`: str
    - `border_style`: str
    - `border_color`: str
  - `cell_ranges`: open_mapping - range string -> style mapping (text_color, background_color, font_size, font_name, bold, italic, alignment_horizontal, alignment_vertical, border)
  - `hidden_columns`: list of str - Column names
  - `on_missing_column`: str; default "warn"; one of error, warn, skip
  - `copy_widths_from_sheet`: str
  - `column_widths_from_stage`: stage_in
  - `column_widths_source`: str
  - `column_styles_from_stage`: stage_in
  - `column_styles_source`: str
  - `row_heights`: open_mapping - row number -> height
  - `header_row_height`: number
  - `data_row_height`: number
  - `show_gridlines`: bool
  - `banded_row_color`: str
  - `banded_row_border_style`: str
  - `banded_row_border_color`: str
  - `outline_border_style`: str
  - `outline_border_color`: str
  - `outline_border_range`: str
  - `tab_color`: str
  - `zoom_percent`: int
  - `sheet_state`: str; one of visible, hidden, very_hidden
- `templates`: list_of_mappings
  - `template_name`: str; REQUIRED
  - `apply_templates`: list of str
  - `auto_fit_columns`: bool
  - `autofit_scan_rows`: int
  - `min_column_width`: number
  - `max_column_width`: number
  - `header_row`: int; default 1
  - `header_bold`: bool
  - `header_background`: bool
  - `header_background_color`: str
  - `header_text_color`: str
  - `header_font_size`: number
  - `header_alignment_horizontal`: str; one of left, center, right, justify, distributed
  - `header_alignment_vertical`: str; one of top, center, bottom, justify, distributed
  - `general_text_color`: str
  - `general_font_size`: number
  - `general_font_name`: str
  - `general_alignment_horizontal`: str; one of left, center, right, justify, distributed
  - `general_alignment_vertical`: str; one of top, center, bottom, justify, distributed
  - `freeze_top_row`: bool
  - `freeze_panes`: str
  - `auto_filter`: bool
  - `column_formats`: list_of_mappings
    - `column_names`: list of str - Header NAME strings this rule styles
    - `column_refs`: list of str - Positional Excel refs like A or BQ - never header names
    - `number_format`: str
    - `alignment_horizontal`: str; one of left, center, right, justify, distributed
    - `alignment_vertical`: str; one of top, center, bottom, justify, distributed
    - `wrap_text`: bool
    - `font_color`: str
    - `font_bold`: bool
    - `font_italic`: bool
    - `font_size`: number
    - `font_name`: str
    - `font_underline`: any
    - `font_strikethrough`: bool
    - `background_color`: str
    - `border_style`: str
    - `border_color`: str
    - `make_hyperlinks`: str - e.g. file_paths
    - `hyperlink_color`: str
    - `header_font_color`: str
    - `header_background_color`: str
    - `header_bold`: bool
    - `width`: number - Explicit column width
    - `whole_column`: bool; default false - Column-dimension style, for cells Excel creates at calculation time
    - at least one of: `column_names`, `column_refs`
  - `cell_formats`: list_of_mappings
    - `cells`: list of str; REQUIRED - A1-style cells or ranges, e.g. ["B2"] or ["A4:D4"]
    - `number_format`: str
    - `alignment_horizontal`: str; one of left, center, right, justify, distributed
    - `alignment_vertical`: str; one of top, center, bottom, justify, distributed
    - `wrap_text`: bool
    - `font_color`: str
    - `font_bold`: bool
    - `font_italic`: bool
    - `font_size`: number
    - `font_name`: str
    - `font_underline`: any
    - `font_strikethrough`: bool
    - `background_color`: str
    - `border_style`: str
    - `border_color`: str
  - `cell_ranges`: open_mapping - range string -> style mapping (text_color, background_color, font_size, font_name, bold, italic, alignment_horizontal, alignment_vertical, border)
  - `hidden_columns`: list of str - Column names
  - `on_missing_column`: str; default "warn"; one of error, warn, skip
  - `copy_widths_from_sheet`: str
  - `column_widths_from_stage`: stage_in
  - `column_widths_source`: str
  - `column_styles_from_stage`: stage_in
  - `column_styles_source`: str
  - `row_heights`: open_mapping - row number -> height
  - `header_row_height`: number
  - `data_row_height`: number
  - `show_gridlines`: bool
  - `banded_row_color`: str
  - `banded_row_border_style`: str
  - `banded_row_border_color`: str
  - `outline_border_style`: str
  - `outline_border_color`: str
  - `outline_border_range`: str
  - `tab_color`: str
  - `zoom_percent`: int
  - `sheet_state`: str; one of visible, hidden, very_hidden
- `active_sheet_name`: str
- `pivot_style`: mapping
  - `name`: str; REQUIRED
  - `header_background_color`: str
  - `header_font_color`: str
  - `header_bold`: bool
  - `bold_subtotals`: bool
  - `bold_grand_totals`: bool
- `default_pivot_style`: str
- `workbook_theme`: mapping
  - `preset`: str
  - `accent_colors`: any
  - `apply`: bool
  - `from_file`: str

### `free_stages`  - family `base`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `stages`: stage_release; REQUIRED - Stages to release from memory
- `on_missing`: str; default "error"; one of error, warn, skip

### `generate_column_config`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_file`: str; REQUIRED
- `source_sheet`: any
- `template_file`: str; REQUIRED
- `template_sheet`: any
- `output_file`: str; REQUIRED
- `header_row`: int; default 1
- `max_rows`: int; default 1000
- `sample_rows`: int; default 5
- `similarity_threshold`: number; default 0.8
- `include_recipe_section`: bool; default false

### `group_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `source_column`: str; REQUIRED
- `target_column`: str - Default: <source_column>_Group
- `groups`: open_mapping - group name -> list of values
- `groups_source`: mapping
  - `type`: str; REQUIRED; one of stage, lookup, file
  - `stage_name`: stage_in
  - `lookup_stage`: stage_in
  - `lookup_key`: str
  - `filename`: str
  - `sheet`: any
  - `encoding`: str
  - `separator`: str
  - `format_type`: str; one of xlsx, csv, tsv
  - `format`: str; default "wide"; one of wide, long - Shape of the definitions table
  - `group_column`: str
  - `group_name_column`: str
  - `values_column`: str
  - `filter_condition`: any
- `groups_file`: str
- `unmatched_action`: str; default "keep_original"; one of keep_original, set_default, error
- `unmatched_value`: any; default "Other"
- `case_sensitive`: bool; default false
- `replace_source`: bool; default false
- at least one of: `groups`, `groups_source`, `groups_file`

### `import_file`  - family `import`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `input_file`: str; REQUIRED
- `sheet_name`: any; default "?sheet_001?" - Tab name, 1-based number, or ?sheet_NNN? token
- `header_row`: int; default 1
- `encoding`: str; default "utf-8"
- `separator`: str; default ","
- `format`: str; one of xlsx, xls, csv, tsv
- `verbatim_text_columns`: list of str - Column names
- `on_missing_file`: str; default "error"; one of error, create_empty
- when `on_missing_file` = `error`:
- when `on_missing_file` = `create_empty`:
  - `create_empty_columns`: list of str; REQUIRED - Column names

### `inject_formulas`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `mode`: str; default "live"; one of live, awaken
- `target_file`: str; REQUIRED
- `sheets_to_receive_formulas`: list_of_mappings
  - `sheet_names`: list of str; REQUIRED - Tab names or ?sheet_NNN? tokens
  - `formulas`: list_of_mappings; REQUIRED
    - `excel_formula`: str; REQUIRED - Excel formula text; {col:Header} resolves to that column letter on the sheet
    - `cell`: str - Target cell like B2
    - `range`: str - Target range like B2:B100
    - `fill_down`: bool; default false - Cell target: fill down the data extent
    - `array_formula`: bool; default false
    - at least one of: `cell`, `range`
- `sheet_names`: any - awaken mode: a list of tabs, "all", or omit for the active sheet
- `formulas`: list_of_mappings - awaken / single-sheet form
  - `excel_formula`: str; REQUIRED - Excel formula text; {col:Header} resolves to that column letter on the sheet
  - `cell`: str - Target cell like B2
  - `range`: str - Target range like B2:B100
  - `fill_down`: bool; default false - Cell target: fill down the data extent
  - `array_formula`: bool; default false
  - at least one of: `cell`, `range`
- `auto_scan`: bool; default false - awaken: scan every sheet for formula text

### `lookup_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `lookup_stage`: stage_in; REQUIRED
- `match_col_in_main_data`: str; REQUIRED
- `match_col_in_lookup_data`: str; REQUIRED
- `lookup_columns`: list of str; REQUIRED - Column names
- `join_type`: str; default "left"; one of left, inner
- `handle_duplicates`: str; default "first"; one of first, last, error
- `default_values`: open_mapping - lookup column -> value when unmatched
- `normalize_keys`: bool; default true
- `low_match_warning`: bool; default true
- `match_mode`: str; default "exact_key_equality"; one of exact_key_equality, lookup_value_within_main_text
- `prefix`: str; default ""
- `suffix`: str; default ""

### `manage_named_objects`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `operation`: str; REQUIRED; one of export_all, export_filtered, import_all, import_filtered, list_objects, validate_yaml, copy_direct, create_from_columns
- `name_validation`: str; default "excel"; one of none, excel, house
- when `operation` = `export_all`:
  - `source_file`: str; REQUIRED
  - `yaml_file`: str
  - `vba_file`: str
  - `export_formats`: mapping - Alternative grouped form of the two output paths
    - `yaml_file`: str
    - `vba_file`: str
- when `operation` = `export_filtered`:
  - `source_file`: str; REQUIRED
  - `yaml_file`: str
  - `vba_file`: str
  - `include_patterns`: list of str
  - `exclude_patterns`: list of str
- when `operation` = `import_all`:
  - `target_file`: str; REQUIRED
  - `yaml_file`: str; REQUIRED
  - `on_existing`: str; default "error"; one of error, replace, skip
  - `prune_orphans_with_prefix`: str
- when `operation` = `import_filtered`:
  - `target_file`: str; REQUIRED
  - `yaml_file`: str; REQUIRED
  - `on_existing`: str; default "error"; one of error, replace, skip
  - `include_patterns`: list of str
  - `exclude_patterns`: list of str
  - `prune_orphans_with_prefix`: str
- when `operation` = `list_objects`:
  - `source_file`: str; REQUIRED
- when `operation` = `validate_yaml`:
  - `yaml_file`: str; REQUIRED
- when `operation` = `create_from_columns`:
  - `target_file`: str; REQUIRED
  - `ranges`: list_of_mappings; REQUIRED
    - `name`: str; REQUIRED - Defined name to create
    - `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
    - `column_names`: list of str; REQUIRED - Header names spanning the range
    - `anchor_columns`: list of str - Columns to measure the row extent from; default the range columns
    - `row_mode`: str; default "data_with_header"; one of data, data_with_header, full_col, full_col_no_header
    - `header_row`: int; default 1
    - `expand_span`: bool; default true
    - `absolute`: bool; default true
    - `on_missing`: str; default "error"; one of error, warn, skip
    - `scope`: str; default "global"; one of global, local
    - `name_mgr_comment`: str - Name Manager comment text
  - `on_existing`: str; default "error"; one of error, replace, skip
  - `prune_orphans_with_prefix`: str
- when `operation` = `copy_direct`:
  - `source_file`: str; REQUIRED
  - `target_file`: str; REQUIRED
  - `include_local`: bool; default true
  - `include_patterns`: list of str
  - `exclude_patterns`: list of str
  - `on_existing`: str; default "error"; one of error, replace, skip

### `merge_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `merge_source`: mapping; REQUIRED
  - `type`: str; REQUIRED; one of excel, csv, tsv, dictionary, stage
  - `stage_name`: stage_in
  - `path`: str
  - `sheet`: any
  - `encoding`: str
  - `separator`: str
  - `format`: str
  - `data`: any
  - `columns_to_prefix`: list of str
- `left_key`: any; REQUIRED
- `right_key`: any; REQUIRED
- `join_type`: str; default "left"; one of left, right, inner, outer
- `column_prefix`: str; default ""
- `suffixes`: any - Two suffixes for overlapping names, list or tuple
- `drop_duplicate_keys`: bool; default true

### `pivot_table`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `index`: list of str - Column names
- `columns`: list of str - Column names
- `values`: list of str - Column names
- `aggfunc`: any; default "sum"
- `fill_value`: any; default 0
- `fill_blanks`: bool; default false
- `margins`: bool; default false
- `dropna`: bool; default false
- `sort_by_index`: bool; default false

### `profile_files`  - family `import`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `files`: list of str; REQUIRED
- `include_full_paths`: bool; default false
- `on_missing`: str; default "error"; one of error, note, skip

### `profile_named_objects`  - family `import`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `workbooks`: list of str; REQUIRED

### `profile_sheets`  - family `import`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `sheets`: list_of_mappings; REQUIRED
  - `source_stage`: stage_in
  - `input_file`: str
  - `sheet_name`: any
  - `label`: str
  - at least one of: `source_stage`, `input_file`
- `scan_rows`: int
- `min_width`: number
- `max_width`: number
- `padding`: number

### `profile_workbooks`  - family `import`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `workbooks`: list of str; REQUIRED

### `rename_columns`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `rename_type`: str; default "mapping"; one of mapping, pattern, transform
- `mapping`: open_mapping - old name -> new name
- `pattern`: str
- `replacement`: str; default ""
- `add_prefix`: str
- `add_suffix`: str
- `case_conversion`: str; one of upper, lower, title, snake_case, camel_case
- `replace_spaces`: str
- `strip_characters`: str

### `rows_to_columns`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `id_columns`: list of str - Omit to imply every column not named by labels_from / values_from
- `labels_from`: str; REQUIRED
- `values_from`: str; REQUIRED
- `fill_missing_with`: any

### `seed_donor_formulas`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_file`: str; REQUIRED
- `source_sheet`: any; REQUIRED
- `target_file`: str; REQUIRED
- `target_sheet`: any; REQUIRED
- `column_names`: list of str - Header names resolved separately in donor and target
- `column_refs`: list of str - Positional Excel refs like A or BQ - never header names
- `start_row`: int; default 2
- `row_count`: int; default 3
- `fill_down`: bool; default false
- `fill_anchor_columns`: list of str - Columns whose extent the fill-down follows
- `on_existing_cell`: str; default "error"; one of error, skip, overwrite
- `array_formula_mode`: str; default "preserve"; one of preserve, convert
- at least one of: `column_names`, `column_refs`

### `select_columns`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `columns_to_keep`: list of str - Names, in output order
- `columns_to_drop`: list of str - Column names
- `columns_to_create`: list of str - Created blank when absent from columns_to_keep
- `default_value`: any - Fill for created columns
- `strict_mode`: bool; default true
- `allow_duplicates`: bool; default true
- at least one of: `columns_to_keep`, `columns_to_drop`

### `slice_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `slice_type`: str; REQUIRED; one of row_range, column_range, transpose
- `slice_result_contains_headers`: bool; default false
- `header_column`: str
- `old_headers_column_name`: str; default "Field"
- when `slice_type` = `row_range`:
  - `start_row`: int; default 1
  - `end_row`: int
- when `slice_type` = `column_range`:
  - `start_col`: any
  - `end_col`: any
- when `slice_type` = `transpose`:

### `sort_data`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `columns`: list of str; REQUIRED - Column names
- `sort_type`: str; REQUIRED; one of ascending, descending, custom
- `custom_orders`: open_mapping - column -> ordered value list, for sort_type custom
- `case_sensitive`: bool; default false - Excel default: case-insensitive
- `na_position`: str; default "last"; one of first, last

### `split_column`  - family `transform`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `source_column`: str; REQUIRED
- `split_type`: str; REQUIRED; one of delimiter, fixed_width, regex, position
- `delimiter`: str
- `pattern`: str
- `widths`: list of int
- `positions`: list of int
- `max_splits`: int
- `new_column_names`: list of str - Column names
- `expand_to_columns`: bool; default true
- `fill_missing`: any; default ""
- `remove_original`: bool; default false
- `strip_whitespace`: bool; default true

### `strip_formula_caches`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `files`: list of str; REQUIRED
- `create_backup`: bool; default true
- `scope`: list_of_mappings - Sheets (and at most one of cells/columns/rows) to strip; absent = whole workbook
  - `sheet_names`: list of str; REQUIRED
  - `cells`: any
  - `columns`: any
  - `rows`: any

### `verify_columns`  - family `transform` (check: reads, writes nothing)

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `expected_columns`: list of str - Column names
- `expected_from_stage`: stage_in - Take the expected list from another stage
- `on_unexpected`: str; default "warn"; one of error, warn, skip
- `on_missing_expected`: str; default "error"; one of error, warn, skip
- at least one of: `expected_columns`, `expected_from_stage`

### `verify_excel_storage`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `files`: list of str; REQUIRED
- `on_violation`: str; default "halt"; one of halt, warn

### `verify_sheet_data`  - family `file_ops`

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `target_file`: str; REQUIRED
- `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
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

### `verify_stage_data`  - family `transform` (check: reads, writes nothing)

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

