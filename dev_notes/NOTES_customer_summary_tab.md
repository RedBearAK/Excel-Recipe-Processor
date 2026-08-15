# NOTES: interactive Customer_Summary tab + GROUPBY prefixes (2026-08-14)

The recipe-authored interactive mini-report, realized: a dropdown cell
(C1) on a new Customer_Summary tab drives a GROUPBY that re-pivots live.
First real exercise of excel_data_validation's spill-ref source.

## Framework patch in this TGZ

`inject_formulas_functions.py`: GROUPBY, PIVOTBY, PERCENTOF added to the
future-function map ('_xlfn.'). SHARP EDGE recorded in the map's comment:
their aggregation argument is an eta-reduced lambda (bare SUM) which
Excel STORES as `_xleta.SUM` - a prefix the name-anywhere map cannot add
without corrupting every ordinary SUM. Recipes therefore write the
aggregation as a full `LAMBDA(x,SUM(x))`; LAMBDA is mapped, and the
legacy function inside needs no prefix.

## Recipe changes (vms_process.yaml, six splices, all delivered spliced)

1. Stage declaration + create_stage: a ZERO-ROW frame whose column
   headers land the labels - A1 " ", B1 "Customer:". Row 1 is the only
   thing the stage delivers; everything else is injected onto the sheet.
2. sheets_to_create: Customer_Summary after No_Price_Product_Summary.
3. free_stages: the frame stage added.
4. manage_named_objects: rng_vms_customer / _product / _units / _netwt /
   _totprice over VMS_Data columns, ALL anchored on Customer so the five
   ranges share ONE extent - Total Price has blank cells (no-price rows)
   and per-column extents would hand GROUPBY unequal-length arrays.
5. inject_formulas on Customer_Summary: Z1 spill
   `=SORT(UNIQUE(FILTER(rng_vms_customer,...)))`, A2:D2 labels, A3
   `=IF($C$1="",...,GROUPBY(product, HSTACK(units,netwt,totprice),
   LAMBDA(x,SUM(x)), 0, 1, 1, customer=$C$1))` - then
   excel_data_validation: C1 list from spill ref `$Z$1#`, prompt +
   stop-style alert.
6. format_excel: tab_color 5A9FD4 (fourth summary blue), header_bold.
   Deliberately NO tpl_header: its auto_filter would hang a filter
   dropdown on the label row next to the real dropdown in C1.

## Verified on real data (standalone demo, interactive_test.xlsx)

Ran the full chain against the trimmed processed workbook (114 rows, 7
customers). Stored XML checked: `_xlfn.GROUPBY`, `_xlfn.HSTACK`,
`_xlfn.LAMBDA` with bare SUM inside; `_xlfn._xlws.SORT/FILTER` and
`_xlfn.UNIQUE` on the spill; cm="1" + t="array" on both formula cells;
xl/metadata.xml present; DV stored `list | $Z$1# | C1 | stop`. DV
survives the later session format pass. Patched vms_process.yaml passes
--validate-recipe with zero warnings (64 steps, 50 declared stages).

## EXCEL EYEBALL - open interactive_test.xlsx and check

1. C1 dropdown populates, sorted unique customers (the stored literal
   '#' question - if it fails, the fix is ANCHORARRAY translation in the
   dynamic-array zip-rewrite layer, not in the DV processor).
2. Pick "Rizhao Smart Foods Co., Ltd." - A3 should re-pivot to ONE row:
   Block Chum Salmon H&G #1 GMC O/R | 32,560 | 1,647,133.6 | 5,042,560.40
   plus a grand-total row with the same numbers (total_depth 1).
3. Pick the Shandong customer - multi-product grid, ascending by product.
4. Clear C1 - A3 shows "(pick a customer above)".
5. GROUPBY computes with the full-LAMBDA aggregation (the _xleta
   avoidance). If it works, a later experiment can try bare SUM to see
   whether Excel repairs or accepts it.

## Known cosmetic items, deliberately unhandled in v1

- Column Z (the spill) is visible; hiding it needs column-letter
  addressing that format_excel's header-name vocabulary cannot express
  on a sheet with only two headers. Candidate for the format_excel
  back-burner list.
- Column widths on the summary grid: same addressing limitation.
- A2:D2 labels are injected `="..."` formulas (declared dynamic like all
  provenance-marked cells) - harmless, but real text would need either a
  row in the frame stage or a write-values capability.

# End of file #
