# Consolidated formatting cycle (2026-08-23)

One cycle closing the format_excel piecemeal gaps: hyperlinks, font
underline and strikethrough, banded rows, outline borders, row height
conveniences, and the gridline toggle. Plus the profile_files Path
column that feeds the Sources-tab file link pattern.

Test module: `tests/test_format_excel_consolidated_cycle.py` (6 tests).
Regression sweep at delivery: 15/15 modules across the format, profile
and conditional_format families, all exit 0.

## New column_formats rule keys

| Key | Values | Notes |
|---|---|---|
| `font_underline` | `true`, `"single"`, `"double"` | `true` means single |
| `font_strikethrough` | `true` / `false` | |
| `make_hyperlinks` | `"file_paths"`, `"web_urls"`, `"email_addresses"` | see below |
| `hyperlink_color` | any color spec | requires `make_hyperlinks`; default `0563C1` |

`font_underline` and `font_strikethrough` also work in `cell_formats`
spot rules. Both work with `whole_column: true` (dimension-level font).

## make_hyperlinks semantics

The rule DECLARES what the bare cell text is. No sniffing, no auto
mode: a formatter guessing between a path and a URL is implicit
behavior, and implicit behavior makes links that are wrong in ways
nobody notices until click time.

Mechanism: real `cell.hyperlink` relationships (`TargetMode` external
in the sheet rels), NOT injected `HYPERLINK()` formulas. No recalc, no
formula caches, no dynamic-array declaration machinery for what is
static metadata. Survives save/load round trips (tested). Cell text
stays the readable value; only the stored target is transformed.

Per kind:

- `file_paths`: absolute POSIX paths. Percent-encodes spaces and
  specials (`/Users/mmf/Dropbox/2026 Data Downloads/x.xlsx` becomes
  target `file:///Users/mmf/Dropbox/2026%20Data%20Downloads/x.xlsx`).
  Values already starting `file://` pass through. Relative paths and
  backslash paths raise with the cell address named: a file link has
  no working directory, so a relative path would be quietly wrong.
- `web_urls`: values with `://` pass through. Scheme-less values get
  `https://` prefixed, the same assumption Excel itself makes.
  Whitespace-bearing values raise.
- `email_addresses`: `mailto:` prefixed; needs an `@`, no spaces.

Cross-cutting: blank cells skip silently (sparse link columns are a
designed shape, same doctrine as the low-match warning suppression).
Non-blank failures raise loudly with the cell address. Not combinable
with `whole_column` (links are per-cell relationships). Link font is
Excel standard blue underlined on top of the existing cell font.

Mac notes: spaces MUST be percent-encoded or Excel mangles the target
(handled automatically). First-ever click from sandboxed Mac Excel may
show a one-time grant-access dialog; after that, one click opens the
file. A target path that does not exist on the opening machine gets
Excel own polite error at click time, acceptable for diagnostic tabs.

## New sheet-level keys

| Key | Values | Notes |
|---|---|---|
| `header_row_height` | points | on the `header_row` dimension |
| `data_row_height` | points | sheet DEFAULT height + customHeight flag |
| `show_gridlines` | `true` / `false` | sheet view toggle |
| `banded_row_color` | color spec | fills every second data row |
| `banded_row_border_style` | border style name | rules the banded cells |
| `banded_row_border_color` | color spec | default gridline gray `D9D9D9` |
| `outline_border_style` | border style name | box around a range |
| `outline_border_color` | color spec | default black `000000` |
| `outline_border_range` | range string or list | default whole used range |

`data_row_height` uses the sheet default (`sheetFormatPr`) rather than
per-row entries, so rows Excel creates later - dynamic-array spill
rows - inherit it. Same reasoning as whole_column styles. The
`customHeight` flag is set alongside; without it Excel ignores the
stored default. Explicit `row_heights` entries still win per row.

Banding rhythm: first data row unbanded, second banded, matching Excel
own table styles. Border remedy doctrine carries over: a fill
suppresses gridlines, so `banded_row_border_style: "thin"` restores
the ruling on banded rows (gray by default).

Outline borders touch ONLY the outward-facing sides of perimeter
cells; existing sides survive, so a box lands on top of gridline-gray
ruling without erasing it. Corner cells get two sides. Default color
is black because a box is emphasis, unlike the gridline remedy.

## Precedence (the fill stack, bottom to top)

1. Column tints (`background_color` in column_formats)
2. Banded rows - banding WINS over tints BY DESIGN: the band tracks a
   ROW and must be continuous, while the tint marks column provenance
   and still reads through on the off-band rows
3. `cell_formats` spot rules - explicit beats everything
4. Outline borders land last among border-touching passes (outward
   sides only)

All three sheet-level passes (column rules, banding, outline default
range) measure the data extent with the same widest-column logic, so
they agree about where the data ends.

## Guided errors added

- `make_hyperlinks` wrong value: names the three legal kinds
- `make_hyperlinks` + `whole_column`: refuses, names the conflict
- `hyperlink_color` without `make_hyperlinks`: refuses
- bad path/URL/email cell values: refuse with cell address
- `font_underline` wrong value: names `true`, `single`, `double`
- `banded_row_border_*` without `banded_row_color`: nothing to rule
- `outline_border_color`/`outline_border_range` without
  `outline_border_style`: refuses, suggests styles
- malformed `outline_border_range` entries: refuses, shows the shape

## profile_files: include_full_paths

`include_full_paths: true` appends a `Path` column with the absolute
resolved path. Opt-in, so existing Sources tabs keep their shape.
MISSING rows (under `on_missing: note`) still carry the Path - a row
pointing at where the file SHOULD be is exactly the diagnostic a
provenance sheet exists to surface. Point a
`make_hyperlinks: "file_paths"` rule at Path and every source file is
one click away.

## Deferred

Merged header cells (group banners over column spans) deferred from
this cycle: the spec shape (label + span + freeze-pane interaction)
needs its own design pass.

# End of file #
