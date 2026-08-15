# NOTES: FILTER-spill view tabs (2026-08-14)

The house pattern for "same data, different tab, filtered" WITHOUT the
file-size multiplication of static copies. First production instance:
Export_View in vms_process.yaml, seated right after VMS_Data.

## The problem it solves

Static filtered copies of a large main tab cost nearly a full
file-multiple each (pandas writes inline strings; every copy repeats
every value). A FILTER-spill view costs a few KB regardless of row
count: the stored file carries ONE formula cell plus a real-text header
row. Measured on the trimmed demo: static 114x77 sheet ~36KB vs the
whole view tab ~7KB - and the static cost scales with rows, the view
cost does not.

## The pattern (three pieces, all existing vocabulary)

1. HEADER FRAME: a zero-row copy of the main stage via filter_data with
   an impossible match. All headers arrive as REAL TEXT, exactly
   matching the source, self-maintaining when columns change.
2. NAMED RANGES: rng_vms_all spanning first-to-last column
   (expand_span), plus the criteria column - both anchored on Customer
   so extents match (FILTER needs equal-length arrays).
3. ONE INJECTED FORMULA at A2:
   =FILTER(rng_vms_all, rng_vms_saletype="Export", "no matching rows")
   Stored (via the transforms) as _xlfn._xlws.FILTER with cm="1".

Each ADDITIONAL view = one more frame stage + one formula.

## Formatting facts (asked and answered 2026-08-14)

- Header-row formatting works EXACTLY like the main tab - the header
  row is real text. Deliberate exception: NO auto_filter; Excel's
  filter UI cannot reorder a spill ("can't change part of an array"),
  so the dropdowns would only mislead. Band + freeze, no filter.
- Column formatting does NOT ride along: FILTER projects VALUES ONLY.
  Number formats, widths, fills all stay on the source tab. The view
  mirrors them itself, with number formats as whole_column (spill-fed
  cells do not exist at format time). VERIFIED: one rule can mix
  whole_column data styling with per-cell header overrides (the red
  recipe-added-columns convention) - header cells carry explicit
  styles, which outrank column styles by OOXML precedence.
- Auto-fit only sees the header row on a view tab, so wide DATA columns
  need explicit widths (Customer 64, Product Name 40 in the instance).

## Trade-offs (stated plainly)

- Read-only projection: no per-tab manual edits.
- Live: source edits re-filter instantly - feature or hazard by tab
  purpose.
- Excel-saves cache spill values via sharedStrings (deduped across
  tabs) - post-save growth far below static duplication, and the
  recipe-delivered file keeps the full savings.

## Adoption notes

Export_View criteria: Sale Type = "Export". The trimmed sample is
114/114 Export, so the demo view shows everything; visible filtering
appears on real data with Domestic rows. Tab wears FFF59D, a paler
main-family yellow (a view OF the main tab). Recipe validates clean:
68 steps, 52 declared stages.
