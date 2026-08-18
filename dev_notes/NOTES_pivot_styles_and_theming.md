# Modern theme baseline, pivot styles, optional theme overrides

Expand at the repo root. Base verified against dev_beta HEAD 6b79e4d.
excel_writer.py and format_excel_processor.py are WHOLE FILES - replace,
don't merge (excel_writer carries the export bridge; format_excel carries
autofit_scan_rows and the session work). Three helper modules land in
_helpers/, with regex and the theme blob in their own modules per
convention.

## Rung 1 (the floor): every constructed workbook gets the modern theme

The writer applies the modern Office theme - Aptos font scheme, accents
156082/E97132/196B24/0F9ED5/A02B93/4EA72E - to every workbook ERP builds,
single-sheet and multi-sheet alike. openpyxl's bundled 2007 palette
(4F81BD, Cambria/Calibri) never ships again. This is not an option and not
a recipe directive; it is what "a file ERP made" now means. Files ERP did
NOT construct are left alone.

Cell fonts are unaffected - openpyxl names Calibri explicitly in
styles.xml, so the theme font scheme only reaches things that reference it
(charts, shapes, formatting Excel applies later). Verified: VMS output
cells are still Calibri, headers still navy 1F4E79, lookups still 548235.

## Rung 2: the default pivot style MOVED because of rung 1

defaultPivotStyle points at the purple swatch beside Excel's usual blue
default. The gallery runs seven per row, positions 2-7 mapping to
accent1-accent6 - so WHICH name is purple depends on the palette:

    legacy palette  accent4 = 8064A2 purple -> PivotStyleLight19
    modern palette  accent5 = A02B93 purple -> PivotStyleLight20

With the modern base it is PivotStyleLight20, four swatches right of
PivotStyleLight16. A test asserts this against the actual base theme, so
if the base ever changes the test fails rather than the colour silently
drifting.

    default_pivot_style: "PivotStyleMedium19"   # name any built-in instead

## Rung 3: a custom pivot style

    pivot_style:
      name: "SBS Pivot"
      header_background_color: "1F4E79"
      header_font_color: "white"
      bold_subtotals: true          # all three nesting levels
      bold_grand_totals: true
      # set_as_default: false

Only a custom style can pin an exact colour (built-ins resolve through
theme accents) or bold total rows.

## Rung 4: workbook_theme, opt-in, layered on the base

    workbook_theme:
      preset: "office_legacy"      # purple / office_modern / office_legacy
      # accent_colors: [six hex]
      # from_file: "donor.xlsx"    # xlsx/xlsm/xltx/pptx/docx/thmx
      # apply: false

A recipe that injects a different palette may move where purple lives; it
should then name its own default_pivot_style.

## Guarding

Unit tests 12/12, including: both writer paths carry the modern theme and
their data survives; the purple default verified against the base theme's
actual accent5; explicit cell formatting unchanged. Full suite: 20
failures, baseline. Full VMS run verified end to end - modern accents,
Aptos scheme, SBS Pivot as default, 13 named ranges, formulas seeded,
static formatting untouched.

# End of file #
