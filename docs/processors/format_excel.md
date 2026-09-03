# `format_excel`

**Family:** `file_ops`

Format existing Excel files with professional presentation features

## Notes

- **tab color**: per-sheet tab_color option colors the sheet tab; accepts the same vocabulary as every other color option (hex with or without #, CSS names, rgb())

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

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

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic template

Use templates to reduce formatting configuration redundancy

```yaml
settings:
  description: "Multi-sheet report with consistent formatting using templates"
  stages:
    - stage_name: "stg_financial_data"
      description: "Quarterly financial data"
      protected: false
    - stage_name: "stg_sales_data"
      description: "Sales performance data"
      protected: false

recipe:
  - step_description: "Import financial data"
    processor_type: "import_file"
    input_file: "data/q3_financial.xlsx"
    save_to_stage: "stg_financial_data"

  - step_description: "Import sales data"
    processor_type: "import_file"
    input_file: "data/q3_sales.xlsx"
    save_to_stage: "stg_sales_data"

  - step_description: "Export combined workbook"
    processor_type: "export_file"
    source_stage: "stg_financial_data"
    output_file: "reports/q3_executive_report.xlsx"
    sheet_name: "Financial Summary"

  - step_description: "Add sales data to second sheet"
    processor_type: "export_file"
    source_stage: "stg_sales_data"
    output_file: "reports/q3_executive_report.xlsx"
    sheet_name: "Sales Performance"

  - # NEW: Template-based formatting step
    step_description: "Apply consistent formatting using templates"
    processor_type: "format_excel"
    target_file: "reports/q3_executive_report.xlsx"
    
    # Define reusable templates
    templates:
      - template_name: "corporate_header_style"
        header_background: true
        header_background_color: "2F5597"   # Executive blue
        header_text_color: "white"
        header_font_size: 14
        header_bold: true
        header_alignment_vertical: "center"
        freeze_top_row: true
        auto_filter: true

      - template_name: "standard_sizing"
        auto_fit_columns: true
        max_column_width: 100
        min_column_width: 12
        row_heights: {1: 25}

      - template_name: "financial_data_style"
        general_font_name: "Calibri"
        general_font_size: 10
        general_alignment_horizontal: "right"

    # Apply templates to sheets
    formatting:
      - sheet_names: ["Financial Summary"]
        apply_templates: ["corporate_header_style", "standard_sizing", "financial_data_style"]

      - sheet_names: ["Sales Performance"]
        apply_templates: ["corporate_header_style", "standard_sizing"]
        # Override template for sales data alignment
        general_alignment_horizontal: "left"
```

### advanced template

Complex template composition with overrides and multiple template application

```yaml
settings:
  description: "Comprehensive dashboard with multiple formatting templates"

recipe:
  - step_description: "Create multi-sheet dashboard with advanced template composition"
    processor_type: "format_excel"
    target_file: "reports/executive_dashboard.xlsx"
    
    # Multiple templates for different purposes
    templates:
      - template_name: "base_professional"
        header_bold: true
        header_background: true
        header_alignment_vertical: "center"
        auto_fit_columns: true
        freeze_top_row: true
        general_font_name: "Calibri"
        general_font_size: 10

      - template_name: "executive_branding"
        header_background_color: "darkslateblue"
        header_text_color: "white"
        header_font_size: 16
        max_column_width: 50
        min_column_width: 15

      - template_name: "financial_precision"
        general_alignment_horizontal: "right"
        general_font_name: "Consolas"
        row_heights: {1: 30}

      - template_name: "summary_highlights"
        auto_filter: true
        cell_ranges: {
          "A1:Z1": {"background_color": "midnightblue", "text_color": "white", "font_size": 14},
          "A2:A100": {"background_color": "lightgray", "text_color": "darkslategray", "bold": true}
        }

      - template_name: "data_visualization_ready"
        general_alignment_vertical: "center"
        cell_ranges: {
          "A:A": {"alignment_horizontal": "left", "bold": true},
          "B:Z": {"alignment_horizontal": "center"}
        }

    formatting:
      # Executive summary with full branding
      - sheet_names: ["?sheet_001?"]
        apply_templates: ["base_professional", "executive_branding", "summary_highlights"]

      # Financial details with precision formatting
      - sheet_names: ["?sheet_002?"]
        apply_templates: ["base_professional", "executive_branding", "financial_precision"]

      # Data visualization sheet
      - sheet_names: ["?sheet_003?"]
        apply_templates: ["base_professional", "data_visualization_ready"]
        # Override header color for this sheet
        header_background_color: "forestgreen"

      # Raw data sheet with minimal formatting
      - sheet_names: ["?sheet_004?"]
        apply_templates: ["base_professional"]
        # Completely override header styling for raw data
        header_background_color: "lightgray"
        header_text_color: "black"
        header_font_size: 11
```

### template reuse

Reusing templates across multiple format_excel steps in the same recipe

```yaml
settings:
  description: "Multi-step report generation with consistent template reuse"

recipe:
  # First formatting step - main reports
  - step_description: "Format main reports with standard templates"
    processor_type: "format_excel"
    target_file: "reports/main_report.xlsx"
    
    templates:
      - template_name: "company_standard"
        header_background: true
        header_background_color: "2F5597"
        header_text_color: "white"
        header_font_size: 14
        header_bold: true
        auto_fit_columns: true
        freeze_top_row: true
        max_column_width: 80
        min_column_width: 10

      - template_name: "data_presentation"
        general_font_size: 10
        general_alignment_vertical: "center"
        auto_filter: true

    formatting:
      - sheet_names: ["?sheet_001?"]
        apply_templates: ["company_standard", "data_presentation"]
      - sheet_names: ["?sheet_002?"]
        apply_templates: ["company_standard", "data_presentation"]

  # Second formatting step - appendix with same templates
  - step_description: "Format appendix using same template standards"
    processor_type: "format_excel"
    target_file: "reports/appendix.xlsx"
    
    # Reuse the same template definitions (copy-paste friendly)
    templates:
      - template_name: "company_standard"
        header_background: true
        header_background_color: "2F5597"
        header_text_color: "white"
        header_font_size: 14
        header_bold: true
        auto_fit_columns: true
        freeze_top_row: true
        max_column_width: 80
        min_column_width: 10

      - template_name: "appendix_style"
        general_font_size: 9
        general_text_color: "darkgray"
        header_font_size: 12

    formatting:
      - sheet_names: ["?sheet_001?"]
        apply_templates: ["company_standard", "appendix_style"]
```

### migration

Before and after comparison showing template migration benefits

```yaml
settings:
  description: "Clean template-based formatting configuration"

recipe:
  - step_description: "Apply formatting with templates (much cleaner!)"
    processor_type: "format_excel"
    target_file: "reports/clean_formatted_report.xlsx"
    
    templates:
      - template_name: "standard_professional"
        freeze_top_row: true
        header_background_color: "2F5597"
        header_background: true
        header_text_color: "white"
        header_font_size: 14
        header_bold: true
        header_alignment_vertical: "center"
        max_column_width: 100
        min_column_width: 12
        auto_filter: true
        auto_fit_columns: true

    formatting:
      - sheet_names: ["?sheet_001?"]
        apply_templates: ["standard_professional"]
      - sheet_names: ["?sheet_002?"]
        apply_templates: ["standard_professional"]
      - sheet_names: ["?sheet_003?"]
        apply_templates: ["standard_professional"]
```

### complex override

Complex template overrides and multiple template conflicts

```yaml
settings:
  description: "Demonstrate template precedence and override behavior"

recipe:
  - step_description: "Complex template composition with precedence rules"
    processor_type: "format_excel"
    target_file: "reports/precedence_demo.xlsx"
    
    templates:
      - template_name: "base_style"
        header_background_color: "blue"
        header_text_color: "white"
        header_font_size: 12
        auto_fit_columns: true

      - template_name: "enhanced_style"
        header_background_color: "darkgreen"  # This will override "blue"
        header_font_size: 14                   # This will override 12
        freeze_top_row: true                   # This adds new functionality

      - template_name: "premium_style"
        header_background_color: "gold"       # This will override "darkgreen"
        header_text_color: "black"            # This will override "white"
        max_column_width: 50                   # This adds column width limits

    formatting:
      - sheet_names: ["?sheet_001?"]
        # Apply templates in order: base_style, then enhanced_style, then premium_style
        # Final result: gold background, black text, font size 14, frozen row, auto-fit, max width 50
        apply_templates: ["base_style", "enhanced_style", "premium_style"]

      - sheet_names: ["?sheet_002?"]
        # Template + direct overrides
        apply_templates: ["base_style", "enhanced_style"]
        # Direct rules override template rules
        header_background_color: "red"        # This overrides template's "darkgreen"
        freeze_top_row: false                 # This overrides template's true
```

### validation

Template validation and error handling scenarios

```yaml
settings:
  description: "Demonstrate template validation and missing template handling"

recipe:
  - step_description: "Template validation scenarios"
    processor_type: "format_excel"
    target_file: "reports/validation_demo.xlsx"
    
    templates:
      - template_name: "valid_template"
        header_background: true
        header_background_color: "navy"
        auto_fit_columns: true

      # This template has a validation error
      - template_name: "invalid_template"
        header_font_size: -5              # Invalid: negative font size
        header_background_color: "notacolor"  # Invalid: unrecognized color

    formatting:
      - sheet_names: ["?sheet_001?"]
        apply_templates: ["valid_template"]

      - sheet_names: ["?sheet_002?"]
        # Reference to non-existent template (will be warned and skipped)
        apply_templates: ["valid_template", "missing_template"]
        header_bold: true  # This will still be applied

      - sheet_names: ["?sheet_003?"]
        # Mix of valid and invalid templates
        apply_templates: ["valid_template", "invalid_template"]
```

### column formats

Number formats, fonts and alignment applied to columns by NAME

```yaml
# Rules address columns by header name, not by letter, so they survive a
# column being inserted upstream. They apply to data rows only - the header
# keeps whatever the header_* options gave it.

settings:
  description: "Format a report by column meaning rather than position"

recipe:
  - step_description: "Apply column formats"
    processor_type: "format_excel"
    target_file: "output/report.xlsx"
    formatting:
      - sheet_names: ["Data"]
        header_bold: true
        header_background: true
        header_background_color: "1F4E79"
        header_text_color: "white"
        auto_fit_columns: true
        # OPT - "error", "warn" (default), or "skip" for a column name that
        # cannot be resolved
        on_missing_column: "error"
        column_formats:
          - # Thousands separator, no decimals
            column_names: ["Cases", "Gross Wt", "Packages", "Units"]
            number_format: "thousands"

          - # Currency pinned left, negatives in parentheses, zero as a dash
            column_names: ["Price", "Total Price"]
            number_format: "accounting"

          - column_names: ["Product ID"]
            alignment_horizontal: "center"

          - # Per-column header styling, unlike the sheet-wide header_*
            # options, so a subset of columns can be marked without
            # touching the rest
            column_names: ["Region", "Country"]
            font_color: "red"
            header_font_color: "white"
            header_background_color: "red"
            header_bold: true

          - # An explicit width overrides auto-fit. Worth setting on a
            # formula column: auto-fit measures a formula cell's SOURCE
            # TEXT, not its result, so a column displaying "OK" can end up
            # sized for a forty character formula.
            column_names: ["Sale Type"]
            width: 13

        # OPT - Columns to hide. The data stays in the file and stays
        # available to formulas; it simply is not displayed.
        hidden_columns: ["Notes", "Internal Reference"]
```

### tab color

Color sheet tabs so related sheets read as a family at a glance

```yaml
settings:
  description: "Color the summary tabs as a stepped family of blues"

recipe:
  - step_description: "Color the summary tabs"
    processor_type: "format_excel"
    # REQ - Existing Excel file to format
    target_file: "reports/monthly_report.xlsx"
    formatting:
      - sheet_names: ["Summary"]
        # OPT - Tab color. Same vocabulary as every other color option:
        # hex with or without #, CSS names ("lightsteelblue"), rgb().
        # Sheets without the option keep the default (uncolored) tab.
        # Excel washes out INACTIVE tab colors, so shades meant to be
        # told apart should be spaced widely.
        tab_color: "CFE7F7"
      - sheet_names: ["Regional_Summary"]
        tab_color: "A6D2EF"
      - sheet_names: ["Exception_Summary"]
        tab_color: "79B8E4"
```

### autofit scan limit

Cap the auto-fit scan on very large sheets

```yaml
# Auto-fit measures EVERY cell by default - correctness first, because a
# long value near the bottom would otherwise get a too-narrow column.
# On big sheets that full scan dominates formatting time. A recipe that
# knows its data (uniform-width codes, dates, numbers) can cap the scan
# to the first N data rows; the header row is always measured.

recipe:
  - step_description: "Format with a capped auto-fit scan"
    processor_type: "format_excel"
    target_file: "big_output.xlsx"
    formatting:
      - sheet_names: ["Data"]
        auto_fit_columns: true
        # OPT - Measure only the first 2000 data rows (default: all)
        autofit_scan_rows: 2000
```

### pivot style

Style the PivotTables a user builds in the generated file

```yaml
# THREE INDEPENDENT LEVERS, from cheapest to heaviest:
#
# 1. DEFAULT (nothing configured) - format_excel points the workbook's
#    defaultPivotStyle at PivotStyleLight19, the purple swatch beside
#    Excel's usual blue default (PivotStyleLight16). One attribute; no
#    theme change, no style definition. A pivot the user inserts comes
#    up purple, which makes a generated file recognisable at a glance.
#
# 2. default_pivot_style - name any other built-in gallery style.
#
# 3. pivot_style - define a NAMED style with exact colours and bold
#    total rows, and make it the workbook default. Built-in gallery
#    styles resolve their colours through the workbook's theme accents,
#    so only a custom style can pin an exact header colour, and only a
#    custom style can bold subtotals and grand totals.
#
# None of these touches explicit cell formatting - header fills, fonts
# and number formats are literal RGB and are unaffected.

recipe:
  - step_description: "Name a different built-in default"
    processor_type: "format_excel"
    target_file: "report.xlsx"
    # OPT - Any built-in gallery style name
    default_pivot_style: "PivotStyleMedium19"
    formatting:
      - sheet_names: ["Data"]
        header_bold: true

  - step_description: "Define a custom pivot style instead"
    processor_type: "format_excel"
    target_file: "report2.xlsx"
    pivot_style:
      name: "Company Pivot"
      header_background_color: "1F4E79"
      header_font_color: "white"
      header_bold: true
      bold_subtotals: true
      bold_grand_totals: true
      # set_as_default: false     # register without making it default
    formatting:
      - sheet_names: ["Data"]
        header_bold: true
```

### workbook theming

Accent palette and a custom PivotTable style for user-built pivots

```yaml
# TWO DIFFERENT MECHANISMS, often confused:
#
# THE BASE: every workbook ERP constructs already carries the modern
# Office theme (Aptos font scheme, 156082/E97132/196B24/0F9ED5/A02B93/
# 4EA72E accents), applied by the writer. That is the floor, not an
# option - openpyxl's bundled 2007 palette is never what ships.
#
# workbook_theme is OPT-IN and layers ON TOP of that base.
# It replaces the six ACCENT SLOTS every gallery style resolves through,
# which is why the same gallery style looks more saturated in a file
# Excel wrote than in a generated one: openpyxl bundles a theme frozen
# at the Office 2007 palette. Use it to make generated files match what
# Excel produces, or to carry a brand palette. It changes the colours of
# EVERY gallery style in the file, so it is never applied by default.
#
# It does NOT touch explicit cell formatting - header fills, fonts and
# number formats applied below are literal RGB and are unaffected.

recipe:
  - step_description: "Format with workbook theming"
    processor_type: "format_excel"
    target_file: "report.xlsx"

    # OPT - ONE source: from_file, accent_colors, or preset
    workbook_theme:
      # from_file: "brand_book.xlsx"   # any OOXML: xlsx/xlsm/xltx/pptx/docx/thmx
      # preset: "office_modern"        # purple (default), office_modern, office_legacy
      accent_colors: ["1F4E79", "2E75B6", "9DC3E6", "203864", "8FAADC", "548235"]
      # apply: false                   # leave the workbook's theme alone

    # OPT - A named PivotTable style, set as the workbook default
    pivot_style:
      name: "Company Pivot"
      header_background_color: "1F4E79"
      header_font_color: "white"
      header_bold: true
      bold_subtotals: true
      bold_grand_totals: true
      # set_as_default: false          # register without making it default

    formatting:
      - sheet_names: ["Data"]
        header_bold: true
```

