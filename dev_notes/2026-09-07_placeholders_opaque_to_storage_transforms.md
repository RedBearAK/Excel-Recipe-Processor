# Column placeholders are opaque to the storage transforms

`dev_notes/2026-09-07_placeholders_opaque_to_storage_transforms.md`

## The failure

Injecting a live formula into the VMS recipe (2026-09-07):

    =IF(OR(UPPER(TRIM({col:AUTO PRODUCT  FORM}2))<> ... ,"Check","")

failed at placeholder resolution with

    Formula references column 'AUTO _xleta.PRODUCT  FORM', which is not
    in the header row of sheet 'VMS'.

The storage transform that gives eta-reduced lambda references their
`_xleta.` prefix (`GROUPBY(a, b, SUM)` stores as `_xleta.SUM`) runs over
the formula text before `{col:...}` placeholders are resolved, and saw a
bare `PRODUCT` in value position - preceded by a space, not followed by
`(` - inside the placeholder. It rewrote the column name. The hand
pair's decode formulas had survived only because their placeholders are
the target cells, not formula text, and `{col:Product ID}` is mixed
case.

## The fix

`apply_outside_strings` now skips `{col:...}` spans as well as string
literals (`opaque_span_rgx` in `inject_formulas_rgx.py`). A placeholder
names a column; a column name may contain any word, including one that
looks like a function in value position or a literal `#`. Placeholders
resolve to cell references later; until then they are opaque, exactly
as string literals are.

`test_inject_formula_storage_forms.py` gains a case: placeholders
holding `PRODUCT` and `#` pass through untouched, and a real eta
reference beside placeholders still gains its prefix.

## Recipe author's note

No recipe change is needed. Any `{col:...}` name is safe in a formula
now, whatever words it contains.


# End of file #
