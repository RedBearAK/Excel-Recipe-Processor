# NOTES: Excel storage forms for spills, eta lambdas, LAMBDA (2026-08-14)

Why the first interactive-tab output triggered Excel's repair dialog,
what the ground truth turned out to be, and what now translates between
recipe syntax and stored syntax. Follow-up to
NOTES_excel_data_validation.md and NOTES_customer_summary_tab.md.

## The two repair triggers (first attempt)

1. `<formula1>$Z$1#</formula1>` in the dataValidation record. A stored
   literal '#' is invalid; repair strips the whole validation (the dead
   dropdown).
2. `_xlfn.LAMBDA(x,SUM(x))` in the GROUPBY cell. LAMBDA parameters must
   be stored with an `_xlpm.` prefix - declaration AND every body
   occurrence. A bare parameter is not merely #NAME?; it is
   grammatically invalid, so repair strips the whole formula (the
   missing A3). The eta-avoidance chosen on 2026-08-14 dodged `_xleta.`
   and walked into `_xlpm.`.

The Z-column spill itself survived (SORT/UNIQUE/FILTER chain clean),
which isolated the damage to exactly these two records.

## Ground truth (harvested verbatim from data-validation-test.xlsx,
## a hand-built Excel 365 workbook - same method as the XLDAPR bytes)

    D1:  <f t="array" ref="D1:D3">_xlfn._xlws.SORT(_xlfn.UNIQUE(A1:A6))</f>
    G1:  _xlfn.GROUPBY(A1:A6,B1:B6,_xleta.SUM)
    H8:  _xlfn.GROUPBY(A1:A6,B1:B6,_xlfn.LAMBDA(_xlpm.x,SUM(_xlpm.x)))
    K1:  SUM(_xlfn.ANCHORARRAY(D1))
    DV:  <formula1>_xlfn.ANCHORARRAY($D$1)</formula1>

So: '#' NEVER survives storage (cell formulas or DV - always
ANCHORARRAY); eta references carry `_xleta.`; LAMBDA params carry
`_xlpm.`.

## What changed

- `inject_formulas_rgx.py`: string_literal_rgx, spill_reference_rgx,
  eta_reference_rgx, lambda_call_rgx.
- `inject_formulas_functions.py`: `transform_storage_forms()` +
  `apply_outside_strings()`. Recipes keep writing display syntax
  (`SUM(D1#)`, `GROUPBY(a,b,SUM)`); live storage gets the harvested
  forms. String literals are never rewritten. Idempotent. A live
  formula containing `LAMBDA(` is REFUSED with guidance naming the
  `_xlpm.` requirement - failing loud beats Excel's silent strip.
  (`_xlpm.` support itself is a queue candidate: a paren-scanner that
  prefixes each parameter in declaration and body; nesting recursion.)
- `inject_formulas_processor.py`: calls the transform for live mode
  only; dead formulas are documentation text and keep display forms.
- `excel_data_validation_processor.py`: `list_from_spill_ref` stores
  `_xlfn.ANCHORARRAY(<ref>)`; recipes keep the natural `"$A$2#"`.
  Also: showDropDown normalized to attribute-ABSENT for the default
  (openpyxl's constructor default False serialized an explicit
  showDropDown="0" Excel itself never writes). Note: openpyxl's LOADER
  refills that False default on read, so byte-parity is only checkable
  in the stored XML, not on a reloaded object - the test does exactly
  that.
- Recipe redesign per the user: the customer spill moved to its own
  Customer_List tab (header "Customers", spill at A2), keeping
  Customer_Summary clean; DV source is `Customer_List!$A$2#`; GROUPBY
  aggregation is eta `SUM` (stored `_xleta.SUM`).

## Verified stored bytes (regenerated interactive_test.xlsx)

    DV:  <formula1>_xlfn.ANCHORARRAY(Customer_List!$A$2)</formula1>
    A3:  ..._xlfn.GROUPBY(..., _xleta.SUM, 0,1,1, rng_vms_customer=$C$1)
    A2 (Customer_List): _xlfn._xlws.SORT(_xlfn.UNIQUE(_xlfn._xlws.FILTER(...)))
    No literal '#', no showDropDown attribute. cm="1" + metadata intact.

Tests: test_inject_formula_storage_forms.py 3/3 (harvest-form cases,
untouched cases incl. string safety + idempotence, LAMBDA guard);
test_excel_data_validation_processor.py 4/4 (ANCHORARRAY expectation,
XML-level showDropDown absence).

## EXCEL EYEBALL round 2 (interactive_test.xlsx)

1. NO repair dialog on open - the whole point.
2. Customer_List!A2 spills the sorted unique customers.
3. C1 dropdown populates from the cross-sheet ANCHORARRAY source.
4. Pick Rizhao: one row - Block Chum Salmon H&G #1 GMC O/R | 32,560 |
   1,647,133.6 | 5,042,560.40 - plus matching grand total. Shandong:
   multi-product grid. Clear C1: "(pick a customer above)".

## Possible later refinements

- GROUPBY self-headers: field_headers=3 works when the source arrays
  include header rows; would need header+data row_mode named ranges and
  retires the injected A2:D2 labels.
- `_xlpm.` LAMBDA-parameter support in the injector.
- Hide/narrow the Customer_List tab (needs sheet-visibility or
  column-letter vocabulary in format_excel - back-burner list).

# End of file #
