# `export_file`

**Family:** `export`

Export stages to Excel or CSV multi-sheet workbooks, backing up replaced files

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `output_file`: str; REQUIRED - Output path; a template with variable substitution. Put a run stamp in the name - {hour}{minute}{second} at least, {timestamp} when the name carries no date - so each run writes a NEW file: Excel holds an open workbook, and a same-named rewrite is not what the open window shows
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

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple single-file export from a stage

```yaml
# Complete recipe with export from stage

settings:
  description: "Simple export from processed data to monthly Excel report"
  stages:
    - stage_name: "stg_processed_data"
      description: "Data ready for export"
      protected: false

recipe:
  # Previous steps would populate 'processed_data' stage
  - # OPT - Human-readable step description
    # Default value: "Unnamed export_file step"
    step_description: "Export processed data to monthly report"
    # REQ - Must be "export_file" for this processor type
    processor_type: "export_file"
    # REQ - Stage to export data from (must be declared in settings.stages)
    source_stage: "stg_processed_data"
    # REQ - Output file path with variable substitution support
    # Variable examples: {date}, {timestamp}, {company}
    #
    # STAMP THE NAME (2026-09-04). A same-named rewrite while Excel has
    # the previous file open is invisible: the window keeps showing the
    # old one, and the "why doesn't this look any different" hunt
    # begins. A run stamp makes every run a new file; {hour}{minute}
    # {second} suffices when the name already carries a date (as a
    # download stem does), {timestamp} when it does not. The house
    # pattern is one settings variable, e.g.
    #   output_basename: "{source_stem}_proc_{hour}{minute}{second}"
    # and every output plus the log named from it.
    output_file: "reports/monthly_report_{timestamp}.xlsx"
    # OPT - Sheet name for single-sheet export
    # Default value: "Data"
    sheet_name: "Monthly Data"
    # OPT - Backup existing file before overwrite
    # Default value: false
    create_backup: true
```

### multi sheet

Multi-sheet export with data from different stages

```yaml
# Export data from multiple stages to comprehensive Excel report

settings:
  description: "Export data from multiple stages to comprehensive multi-sheet quarterly report"
  variables:
    company: "AcmeCorp"
    quarter: "Q4"
  stages:
    - stage_name: "stg_executive_summary"
      description: "Executive summary data"
      protected: false
    - stage_name: "stg_detailed_data"
      description: "Detailed transaction data"
      protected: false
    - stage_name: "stg_monthly_trends"
      description: "Monthly trend analysis"
      protected: false

recipe:
  # Previous steps would populate the stages
  - step_description: "Export comprehensive quarterly report"
    processor_type: "export_file"
    # REQ - Still required in multi-sheet mode. The sheets list says what
    # goes on each tab; source_stage is what the step consumes.
    source_stage: "stg_executive_summary"
    # REQ - Output file path with variable substitution
    # Built-in variables: {date}, {timestamp}, {YYYY}, {MM}, {DD}
    # Custom variables: defined in recipe settings or CLI --var
    output_file: "reports/{company}_Q{quarter}_{date}.xlsx"
    create_backup: true
    # OPT - List of sheet configurations for multi-sheet Excel export
    sheets_to_create:
      # First sheet from executive summary stage
      - sheet_name: "Executive Summary"
        # REQ - Stage name must match exactly what was saved
        data_source: "stg_executive_summary"
      
      # Second sheet from detailed data stage
      - sheet_name: "Detailed Data"
        data_source: "stg_detailed_data"
      
      # Third sheet from trends analysis
      - sheet_name: "Monthly Trends"
        data_source: "stg_monthly_trends"
```

### csv export

Export to CSV with custom formatting options

```yaml
# Export stage data to CSV with custom options

settings:
  description: "Export data to CSV file with custom formatting options"
  stages:
    - stage_name: "stg_export_ready_data"
      description: "Data formatted for CSV export"
      protected: false

recipe:
  # Previous steps populate export_ready_data
  - step_description: "Export data to CSV with custom separator"
    processor_type: "export_file"
    source_stage: "stg_export_ready_data"
    output_file: "exports/system_data.csv"
    # OPT - Text encoding for CSV/TSV files (ignored for Excel files)
    # Valid examples: "utf-8", "latin1", "cp1252", "ascii"
    # Default value: "utf-8"
    encoding: "utf-8"
    # OPT - Column separator for CSV files (ignored for Excel and TSV)
    # Valid examples: ",", ";", "|", "\t"
    # Default value: ","
    separator: ";"
    # OPT - Don't backup CSV files
    # Default value: false
    create_backup: false
```

### variable substitution

Using built-in and custom variables in file paths

```yaml
# Export with dynamic file naming using various variable types

settings:
  description: "Demonstrate dynamic file naming using built-in and custom variables"
  variables:
    department: "sales"
    batch_id: "B001"
  stages:
    - stage_name: "stg_final_results"
      description: "Final processed results ready for export"
      protected: false

recipe:
  # Previous steps populate final_results
  - step_description: "Export with dynamic file naming"
    processor_type: "export_file"
    source_stage: "stg_final_results"
    # REQ - Output file with multiple variable types
    # Built-in date variables: {date}, {timestamp}, {YYYY}, {MM}, {DD}, {YY}, {MMDD}
    # Custom variables: {department}, {batch_id} - defined in recipe settings or CLI
    output_file: "reports/{department}_{YYYY}{MM}{DD}_{batch_id}.xlsx"
    # OPT - Create backup of existing files
    # Default value: false
    create_backup: true
```

### complete workflow

Complete workflow: import → process → export

```yaml
# Complete recipe showing import, processing, and export

settings:
  description: "Complete data processing workflow from import through filtering to export"
  variables:
    region: "west"
    output_prefix: "processed"
  stages:
    - stage_name: "stg_raw_data"
      description: "Raw imported data"
      protected: false
    - stage_name: "stg_filtered_data"
      description: "Filtered data ready for export"
      protected: false

recipe:
  # Step 1: Import data
  - step_description: "Import raw sales data"
    processor_type: "import_file"
    input_file: "sales_data_{region}.xlsx"
    save_to_stage: "stg_raw_data"
  
  # Step 2: Process data
  - step_description: "Filter for active customers"
    processor_type: "filter_data"
    source_stage: "stg_raw_data"
    save_to_stage: "stg_filtered_data"
    filters:
      - column: "Status"
        condition: "equals"
        value: "Active"
  
  # Step 3: Export results
  - step_description: "Export filtered results"
    processor_type: "export_file"
    source_stage: "stg_filtered_data"
    output_file: "output/{output_prefix}_{region}_{date}.xlsx"
    create_backup: true
```

### template

Copy an existing workbook and replace one sheet inside it

```yaml
# Normally export_file builds a new workbook from nothing. Template mode
# byte-copies an existing one and rewrites only the named sheet, so
# everything else rides along untouched: other sheets, named ranges,
# formatting, charts, images.
#
# This is what makes a "copy the last good file and swap the data" workflow
# possible without rebuilding every lookup table each run.

settings:
  description: "Refresh the data sheet in a copy of a prepared workbook"
  stages:
    - stage_name: "stg_report_data"
      description: "Processed rows ready to write"
      protected: false

recipe:
  - step_description: "Copy the template and replace the data sheet"
    processor_type: "export_file"
    source_stage: "stg_report_data"
    # REQ in template mode - workbook to copy
    template_file: "templates/report_template.xlsx"
    # REQ - where the copy goes
    output_file: "output/report_2026_08.xlsx"
    # REQ in template mode - which sheet inside the copy to replace
    sheet_name: "Data"
```

### backup control

Timestamped backups with a hard cap

```yaml
# An existing output file is backed up before being replaced. Backups
# are named with the marker BEFORE the extension:
#
#     report_erpbkup_260812_144320.xlsx
#
# so the file still opens in its default application - the old scheme
# appended ".backup" after the extension and broke that.
#
# Names are written once and never renamed: because the timestamp is
# zero-padded, sorting the names sorts them chronologically, so trimming
# is a pure deletion of the oldest. delete_backups_beyond says how many
# of the NEWEST to keep, counting the one being made - every older
# backup is deleted. Without it, an auto-generated file nobody looks at
# accumulates copies forever.
#
# Only names carrying the _erpbkup_ marker for THIS file's stem and
# extension are ever deleted. Backups from the older ".backup" scheme
# are reported once and left alone.

recipe:
  - step_description: "Export with two backups kept"
    processor_type: "export_file"
    source_stage: "stg_final"
    output_file: "report.xlsx"
    # OPT - Back up an existing file first (default: true)
    create_backup: true
    # OPT - Keep this many newest backups, delete older ones
    # (default: 2; 0 means make no backup at all)
    delete_backups_beyond: 2
```

## Parameter notes

- `source_stage` (required): Stage name to export data from (must be declared in settings.stages)
- `output_file` (required): Output file path with variable substitution support. Stamp the name with the run time so each run writes a new file; Excel keeps showing a file it already has open, so a same-named rewrite looks like nothing happened.
- `sheet_name` (default `Data`): Sheet name for single-sheet Excel export (ignored if sheets parameter is used)
- `sheets_to_create`: List of sheet configurations for multi-sheet Excel export
- `create_backup` (default `False`): Create backup copy of existing file before overwriting (.backup extension added)
- `encoding` (default `utf-8`): Text encoding for CSV/TSV files (ignored for Excel files)
- `separator` (default `,`): Column separator for CSV files (ignored for Excel and TSV)
- `format`: Explicit format override (usually auto-detected from file extension)

