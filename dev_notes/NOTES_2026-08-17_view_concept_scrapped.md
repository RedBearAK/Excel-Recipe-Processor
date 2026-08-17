# The formula-view concept, scrapped: Excel caches every spill on save

dev_notes/NOTES_2026-08-17_view_concept_scrapped.md

Ruling: the read-only FILTER/GROUPBY spill view tabs (Exp_Vw, Dom_Vw,
Exp_Summ_Vw, Exp_Summ_CMA_Vw) are removed from the VMS recipe. The
concept is recorded here so it is not re-invented without this
knowledge.

## What the concept was

A whole tab as ONE stored formula: a live projection of the main
sheet (filtered views) or its summary derivation (GROUPBY views).
The ERP-authored file carried no duplicated rows - a few KB per tab
regardless of row count, against nearly a full file-multiple for a
static copy of the main sheet. The summary views also served as a
live cross-check of the static pipeline, and on their first real
comparison caught the blank-key group-loss bug (13 vans, ~679k lbs
silently missing from the static summary - see
NOTES_2026-08-17_blank_key_group_loss.md). The machinery worked:
storage grammar, xlpm names, cm declarations, formatting inheritance,
collation analysis - all byte-verified.

## Why it dies: the first save cycle

Excel UNCONDITIONALLY writes the cached value of every calculated
cell on save. There is no per-formula, per-sheet, or per-workbook
opt-out; "save external link values" governs links only, and manual
calculation mode merely shows blanks until the first F9 - after which
the save caches everything anyway. Measured on the production file:
3.9 MB as authored -> 7.7 MB after OPEN + SAVE WITH NO CHANGES. Two
full-width view tabs of a ~9,700-row sheet is on the order of a
million cached cells of sheet XML plus calc chain and dynamic-array
value metadata.

So the file-size win is real only for the authored artifact and only
until any user saves. A formula view CANNOT keep a distributed file
small past its first save cycle - and since the views were read-only
(no per-row edits, no filter-UI reordering), they offered nothing
else over a static summary that usually contains FAR less data than
the source sheet. This is also, in general, why Excel files bloat:
every formula is stored twice, as code and as its last answer.

## What was removed vs kept

Removed from the recipe (75 -> 68 steps, 57 -> 51 stages): the four
view tabs, their header-frame and width-profile steps, both format
surveys and the inheritance format step, nine view-only named ranges
(rng_vms_all, rng_vms_saletype, and the seven summary GROUPBY keys),
and four injection entries. Removed from the library: fn_vms_view,
fn_summ_display, fml_exp_summ_base.

Kept: the INTERACTIVE Cust_Summ tab and its Cust_List pick-list
spill. These are not read-only views - the dropdown-driven GROUPBY is
a capability a static sheet cannot provide, and their cached results
are tiny (~50-cell pick list, one small per-customer table). Also
kept: fn_blank_safe in the library (generic, three lines, the next
spill formula will want it), all storage/grammar machinery in the
framework (inject_formulas, xlpm transformer, declarations, audit -
Cust_Summ exercises the same path), and every bug fix the view work
surfaced: the blank-key doctrine (six sites), the xlpm injection
wiring, the importer definition-error fail-loud, and the
lookalike-name doctrine. The views died; what they taught stays.

## If the concept ever returns

Preconditions worth knowing: it only pays where files are distributed
READ-ONLY and never re-saved (or where a save-side process strips
caches - zip surgery deleting cached <v> elements is possible but
means owning a re-save pipeline), or where the projection is small
enough that its cache is immaterial - which is exactly the Cust_Summ
exception, and exactly not the full-sheet case.

# End of file #
