# NOTES: view tabs inherit the seed's formatting (2026-08-15)

The Exp_View test bed now uses BOTH profiling abilities when
formatting the spilled view - widths from the data profile (shipped
earlier) and, new this round, FORMATTING from a survey of the seed
sheet itself.

## The format survey (profile_sheets, file inputs)

The previously PLANNED census, implemented: file-input entries gain
Number_Format (modal data-cell format, '' for General),
Alignment_Horizontal, Data_Font_Color (modal explicit RGB),
Header_Fill_Color / Header_Font_Color / Header_Bold. MODAL over the
first 50 data rows: a column's format is what MOST cells wear, so a
stray hand-edit cannot hijack the inheritance. Stage inputs survey
empty - a stage is data, not a styled sheet. Contract grew by
appending, per the family rule.

## The consumer (format_excel column_styles_from_stage)

Applies by header name: number format / alignment / data font color
at the COLUMN DIMENSION (spill-created cells inherit them), header
fill/font/bold at the header CELL. Ordering is the doctrine: header
BAND first (the default), inheritance second (surveyed specifics),
column_formats rules third - an explicit rule still wins, the escape
hatch stays, though the view currently needs none.

## The pipeline shape (the part that makes it honest)

Surveying the seed's APPLIED formats requires them to exist on disk,
so the recipe splits: main format step (VMS et al., view entry
REMOVED) -> flush_workbooks -> profile_sheets on the WRITTEN file's
VMS sheet -> a second format_excel step formatting Exp_View by pure
inheritance. Recipe now 73 steps; stg_vms_format_profile declared,
not freed. What the view entry LOST: all four hand-mirrored rule
blocks (thousands x5 columns, accounting x4, centering x2, the
11-column red convention) plus the Test-columns width rule - every
one a copy of VMS's formats that had to be maintained by hand. The
survey cannot drift from the seed because it IS the seed, read back.

## Verified

Demo reshaped to the same pipeline; regenerated output checks all
green: thousands formats on Units/Net Weight dimensions, centered
Process Year, the red convention complete (dimension data font +
header fill/font/bold) sitting on the navy band, A2 anchor centered,
widths still inherited, whole-file grammar audit CLEAN. Test module
5/5 including survey facts and consumer replay onto a bare sheet.

## Scope restraint (user-directed)

ONE view is the test bed. The Exp_Summ/Exp_Summ_CMA spilled-summaries
evaluation stays queued; drift alarms stay queued pending the
previous-output retention convention; no other tabs converted.
