# `inject_formulas`

**Family:** `file_ops`

Inject live or dead formulas with name-addressed cells and fill-down

## Notes

- **column placeholders**: {col:Header Name} in cell refs and formulas resolves to the column letter from the header row at injection time, so upstream column insertions cannot silently repoint a formula
- **fill down**: fill_down: true fills a formula from its origin cell to the last data row, translating relative references per row the way Excel's own fill handle does
- **function name translation**: modern function names (XLOOKUP, IFS, FILTER, ...) are stored with the _xlfn. prefixes Excel requires internally, and display without them
- **dynamic array declaration**: live-mode cells register with the workbook session as recipe-authored; with settings declare_dynamic_formulas: true they are declared dynamic-array-aware at save and open without the implicit-intersection @ (any function, by provenance)
- **array formula caveat**: array_formula: true stores a legacy CSE array formula ({braces}); use it only for formulas that genuinely are array formulas - it is NOT the fix for the @ display

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `mode`: str; default "live"; one of live, text, awaken - live: formulas calculate; text: same cells, inert formula text; awaken: make existing formula text live
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

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Inject live formulas into specific cells after exporting data

```yaml
settings:
  description: "Export sales data and add live calculation formulas"
  stages:
    - stage_name: "stg_sales_data"
      description: "Processed sales data"
      protected: false

recipe:
  - # Step 1: Import sales data
    step_description: "Import monthly sales report"
    processor_type: "import_file"
    input_file: "data/sales_march_2024.xlsx"
    save_to_stage: "stg_sales_data"

  - # Step 2: Export data to Excel file
    step_description: "Export processed data to Excel"
    processor_type: "export_file"
    source_stage: "stg_sales_data"
    output_file: "reports/sales_report.xlsx"

  - # Step 3: Inject live formulas into the Excel file
    # OPT - Human-readable step description
    # Default value: "Unnamed inject_formulas step"
    step_description: "Add dynamic calculation formulas to Excel report"
    # REQ - Must be "inject_formulas" for this processor type
    processor_type: "inject_formulas"
    # REQ - Excel file to inject formulas into (must already exist)
    # File path supports variable substitution
    target_file: "reports/sales_report.xlsx"
    # REQ - Formula injection mode
    # Values: "live" (formulas calculate), "text" (same cells, inert formula text a later
    # awaken step or a person can make live), "awaken" (turn formula text already in cells live)
    # Default value: "live"
    mode: "live"
    # REQ (live) - Entries pairing a sheet_names LIST with formulas
    sheets_to_receive_formulas:
      - # The export above writes the single sheet as "Data"
        sheet_names: ["Data"]
        formulas:
          # Individual cell formula
          - cell: "D2"
            excel_formula: "=B2*C2"
          # Another cell with percentage calculation
          - cell: "E2"
            excel_formula: "=D2*0.1"
          # Sum formula in total row
          - cell: "D6"
            excel_formula: "=SUM(D2:D5)"
```

### range formulas

Apply formulas to ranges and multiple sheets

```yaml
settings:
  description: "Create comprehensive financial model with range formulas"
  variables:
    tax_rate: "0.08"
    discount_rate: "0.05"
  stages:
    - stage_name: "stg_financial_data"
      description: "Financial model data"
      protected: false

recipe:
  - # Step 1: Import financial data
    step_description: "Import quarterly financial data"
    processor_type: "import_file"
    input_file: "data/q1_financials.xlsx"
    save_to_stage: "stg_financial_data"

  - # Step 2: Export to multi-sheet Excel workbook
    step_description: "Export data to formatted workbook"
    processor_type: "export_file"
    source_stage: "stg_financial_data"
    output_file: "reports/financial_model.xlsx"
    sheets_to_create:
      - sheet_name: "Revenue"
        data_source: "stg_financial_data"
      - sheet_name: "Expenses"
        data_source: "stg_financial_data"

  - # Step 3: Inject range formulas across multiple sheets
    step_description: "Add calculation formulas to financial model"
    processor_type: "inject_formulas"
    target_file: "reports/financial_model.xlsx"
    mode: "live"
    # REQ (live) - one entry, both sheets: the same formulas land
    # on each (the shape that subsumed the old broadcast pair)
    sheets_to_receive_formulas:
      - sheet_names: ["Revenue", "Expenses"]
        formulas:
          # Apply formula to entire column range
          - range: "D2:D50"
            excel_formula: "=B2*C2"
          # Tax calculation column
          - range: "E2:E50"
            excel_formula: "=D2*{tax_rate}"
          # Total formulas
          - cell: "D51"
            excel_formula: "=SUM(D2:D50)"
          - cell: "E51"
            excel_formula: "=SUM(E2:E50)"
```

### text formulas

Inject the formulas as inert text - visible, copyable, not calculated - for a template or a review copy

```yaml
# mode: text writes the SAME formulas into the SAME cells as live, but as
# string cells: Excel shows the formula text and calculates nothing. A
# later inject_formulas step with mode: awaken (or a person editing the
# cell) makes them live. The switch, not the point, of the processor.

settings:
  description: "Budget template with formulas shown, not run"
  stages:
    - stage_name: "stg_budget"
      description: "Budget lines"
      protected: false

recipe:
  - step_description: "Import budget lines"
    processor_type: "import_file"
    input_file: "data/budget.xlsx"
    save_to_stage: "stg_budget"

  - step_description: "Write the template"
    processor_type: "export_file"
    source_stage: "stg_budget"
    output_file: "reports/budget_template.xlsx"

  - step_description: "Show the formulas without running them"
    processor_type: "inject_formulas"
    target_file: "reports/budget_template.xlsx"
    mode: "text"
    sheets_to_receive_formulas:
      - sheet_names: ["?sheet_001?"]
        formulas:
          - cell: "E2"
            excel_formula: "={col:Units}2*{col:Unit Price}2"
            fill_down: true
```

### awaken dead formulas

Convert dead formula text to live calculations

```yaml
settings:
  description: "Convert budget template with dead formulas to live calculations"
  stages:
    - stage_name: "stg_template_data"
      description: "Budget template data with dead formulas"
      protected: false

recipe:
  - # Step 1: Import template with dead formulas
    step_description: "Import budget template with formula documentation"
    processor_type: "import_file"
    input_file: "templates/budget_template.xlsx"
    save_to_stage: "stg_template_data"

  - # Step 2: Export template to working file
    step_description: "Create working copy of template"
    processor_type: "export_file"
    source_stage: "stg_template_data"
    output_file: "working/budget_working.xlsx"

  - # Step 3: Awaken all dead formulas in the workbook
    step_description: "Convert template formulas to live calculations"
    processor_type: "inject_formulas"
    target_file: "working/budget_working.xlsx"
    # Mode: "awaken" scans for dead formulas and makes them live
    mode: "awaken"
    # Process all sheets in the workbook
    sheet_names: "all"
    # OPT - Enable automatic scanning for formula-like text
    # Default value: false (only processes explicit formulas list)
    auto_scan: true
```

### named columns fill down

Name-addressed cells with fill-down - survives column insertions upstream

```yaml
settings:
  description: "Inject per-row formulas addressed by header name, filled to the last row"
  stages:
    - stage_name: "stg_report_data"
      description: "Processed rows exported to the report"
      protected: false

recipe:
  - # The exported sheet carries headers; {col:Name} resolves each name
    # to its column letter from the header row AT INJECTION TIME, so a
    # column inserted upstream cannot silently repoint any reference.
    step_description: "Inject the margin formulas by column name"
    processor_type: "inject_formulas"
    # REQ - Existing Excel file to modify
    target_file: "reports/margin_report.xlsx"
    # OPT - live (default) stores recalculating formulas
    mode: "live"
    sheets_to_receive_formulas:
      - sheet_names: ["Data"]
        formulas:
          - # {col:...} works in the cell address AND inside the formula.
            cell: "{col:Margin}2"
            # OPT - Fill from the origin row to the last data row,
            # translating relative references per row like Excel's fill
            # handle. Default value: false
            fill_down: true
            excel_formula: '={col:Price}2-{col:Cost}2'
          - # Modern function names are stored with the _xlfn. prefix Excel
            # requires internally; they display without it. With settings
            # declare_dynamic_formulas: true, injected cells open WITHOUT
            # the implicit-intersection @ (declared by provenance).
            cell: "{col:Supplier Region}2"
            fill_down: true
            excel_formula: '=XLOOKUP({col:Supplier}2,rng_suppliers,rng_regions)'
```

### grouped sheets

sheets_to_receive_formulas is THE live injection shape (2026-08-17): a list of entries, each pairing a sheet_names LIST with its own formulas. Many entries give per-sheet formulas; ONE entry naming several sheets sends the same formulas to each (this subsumed and retired the old top-level sheet_names + formulas broadcast pair). The plural list is required even for one sheet; there is no implicit active-sheet default.

```yaml
recipe:
  - step_description: "Inject the live view spills"
    processor_type: "inject_formulas"
    target_file: "output/workbook.xlsx"
    mode: "live"
    sheets_to_receive_formulas:
      - sheet_names: ["Export_View"]
        formulas:
          - cell: "A2"
            excel_formula: '=fn_vms_view(rng_saletype="Export")'
      - sheet_names: ["Domestic_View"]
        formulas:
          - cell: "A2"
            excel_formula: '=fn_vms_view(rng_saletype="Domestic")'
      - # One entry, two sheets: the same stamp lands on both
        sheet_names: ["Notes_A", "Notes_B"]
        formulas:
          - cell: "A1"
            excel_formula: '=TEXT(TODAY(),"yyyy-mm-dd")&" refresh"'
```

## Parameter notes

- `target_file` (required): Path to existing Excel file to inject formulas into
- `mode` (default `live`): Formula injection mode determining formula behavior
- `formulas` (default `[]`): List of formulas to inject with target locations
- `sheet_names` (default `None`): Which sheets to process in the workbook
- `auto_scan` (default `False`): Automatically scan for and process formula-like text (used with 'awaken' mode)

