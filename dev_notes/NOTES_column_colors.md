# Per-column font and header colours

Extends `column_formats` so a rule can style the data rows, that column's header
cell, or both. Supersedes the `format_excel` files in
`fix_format_and_stage_creation.tgz`.

## New rule keys

| Key | Applies to |
|---|---|
| `font_color` | data rows |
| `font_bold`, `font_italic` | data rows |
| `header_font_color` | that column's header cell |
| `header_background_color` | that column's header cell |
| `header_bold` | that column's header cell |

The header keys are **per column**, unlike the sheet-wide `header_*` options.
That is what makes it possible to mark a subset of columns without touching the
rest, and a per-column rule overrides the sheet-wide setting on its own columns.

Colours go through `format_excel`'s own `_normalize_color`, so CSS names work
exactly as they do elsewhere — `red`, `white`, `forestgreen`.

## The VMS case

```yaml
- columns: ["REGION", "PRODUCT  FORM", "PRODUCT  GROUP",
            "World Region", "Country",
            "Test Fresh", "Test Cans", "Test Dest",
            "Test Carrier", "SALE TYPE1"]
  font_color: "red"
  header_font_color: "white"
  header_background_color: "red"
  header_bold: true
```

Verified on the real output: all 10 inserted columns red, all 66 original
columns still navy with black data text, **zero mismatches** when every column
is checked against whether it appears in `var_columns_to_create`.

## Tests

`test_format_excel_column_formats.py` — 10/10. Three new cases: per-column
header styling that leaves untargeted headers alone, CSS name resolution
(`forestgreen` → `228B22`), and a number format combined with a font colour on
one column.

# End of file #
