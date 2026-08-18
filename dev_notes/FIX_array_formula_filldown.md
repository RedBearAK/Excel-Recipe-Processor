# fill_down skipped array formulas

## Symptom

`SALE TYPE1` in the VMS workbook is an **array formula**. `fill_down` left it
empty below the seeded rows while reporting success:

```
⬇️  Filled 4 column(s) from row 3 to 8682      <- four, not five
✅ Transplanted 5 formulas successfully
```

Four of five columns filled. The fifth — the one that decides Fresh / Canned /
Export / Domestic for every row — was blank from row 3 down.

## Cause

An array formula arrives from openpyxl as an `ArrayFormula` object, not a
string. The fill test was:

```python
if not (isinstance(source_value, str) and source_value.startswith('=')):
    continue
```

which quietly skips it. The transplant of the seeded rows was never affected;
only the fill.

## Change

`fill_down` now recognises `ArrayFormula`, translates its `.text`, and rebuilds
a fresh single-cell `ArrayFormula` for each target row:

```python
translated = ArrayFormula(ref=target_reference, text=translated)
```

Each filled cell needs its own `ref`. Reusing the origin's ref would make Excel
treat the whole column as one spilled block anchored at row 2.

Verified:

```
row   2 | ref=BA2  =_xlfn.IFS(AW2=1,"Fresh",AX2=1,"Canned",...)
row   5 | ref=BA5  =_xlfn.IFS(AW5=1,"Fresh",AX5=1,"Canned",...)
row   9 | ref=BA9  =_xlfn.IFS(AW9=1,"Fresh",AX9=1,"Canned",...)

⬇️  Filled 5 column(s)      43,400 cells
```

Existing suites unchanged: 3/3, 5/5, full suite 20 failures as at baseline.

# End of file #
