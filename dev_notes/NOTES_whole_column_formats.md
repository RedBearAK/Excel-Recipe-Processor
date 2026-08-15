# NOTES: whole_column formatting + interactive-tab layout (2026-08-14)

Formatting round for the working Customer_Summary tab, and the
format_excel capability it forced. Follow-up to
NOTES_spill_storage_forms.md.

## The gap that forced the feature

apply_column_formats writes number formats PER CELL up to the current
data extent. The GROUPBY spill's result cells do not exist in the file -
Excel creates them at calculation time - so extent-based per-cell
formatting can never reach them. (Two earlier assumptions died pleasant
deaths on inspection: column LETTERS were already accepted by
resolve_column_letter, and widths were already column-dimension-level
and therefore spill-safe. Only number formats had the gap.)

## whole_column: true (column_formats rule key)

Applies number_format / font / alignment at the COLUMN-DIMENSION level -
a col-level style in the stored file (<col style="N">) that every cell
Excel later creates inherits. Header-cell styling stays per-cell either
way; an explicit cell style overrides the column style, so headers stay
clean. Equivalence-gated: with the key absent, behavior is byte-for-byte
the legacy path, including the empty-sheet skip - except the skip is now
per-RULE with the rule number named, and whole_column rules run on empty
sheets (the spill-fed sheet IS empty at format time; that is the point).

KNOWN EFFECT: openpyxl serializes a col-level style with a width
attribute, so a whole_column rule without an explicit width leaves the
column at openpyxl's default 13 (logged when it happens). Pair
whole_column with width - on a spill-fed sheet auto-fit has nothing to
measure anyway.

TEST TRAP, recorded: pytest shows these return-value-style tests green
even when they fail, because a returned False is not a pytest failure.
The direct `python3 tests/...` run is the authoritative score. (An
assert-based bridge is a possible future tweak for the house style.)

## Layout change (user request)

"Customer:" label -> A1 (the frame's single column header), dropdown ->
A2, so the selected customer shares column A's 40-char width with the
equally long product names. Labels moved to A4:D4, GROUPBY to A5,
selection refs to $A$2. Customer_Summary wears the house navy header
treatment as DIRECT keys (tpl_header's auto_filter would hang a filter
dropdown next to the real one; freezing one label row buys nothing).
Customer_List wears tpl_lookup_header (it reads as reference data) with
the same 40-char width.

Column formats: A width 40; B (Units) default; C (Net Weight) thousands
+ width 15, whole_column; D (Total Price) accounting_0dp + width 18,
whole_column.

## Verified stored bytes (regenerated interactive_test.xlsx)

    <col width="40" customWidth="1" min="1" max="1"/>
    <col width="15" customWidth="1" style="1" min="3" max="3"/>  -> #,##0
    <col width="18" customWidth="1" style="2" min="4" max="4"/>  -> accounting 0dp
    DV: sqref A2, formula1 _xlfn.ANCHORARRAY(Customer_List!$A$2)
    A1 "Customer:" carries the navy header style; A2 content-empty.

Tests: test_format_excel_whole_column.py 3/3 (style chain, empty-sheet
guard preservation incl. mixed rules, guided error). Recipe validates
clean (66 steps, 51 stages).

## EXCEL EYEBALL round 3

1. Navy label cell, dropdown directly beneath it, both in the wide
   column A; green-headed Customer_List.
2. Pick a customer: Net Weight renders 1,647,134-style, Total Price
   renders accounting with the pinned $ and dash-for-zero, INCLUDING on
   every spill row (the whole point of whole_column).
3. Grand-total row formats correctly too (same columns).
