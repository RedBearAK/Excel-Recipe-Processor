# Auto-fit was measuring formula source text

Supersedes the format_excel files in `format_excel_column_colors.tgz`.

## Cause

Auto-fit sized every column from `len(str(cell.value))`. For a formula cell,
`cell.value` is the formula's **source text**, not its result:

```
=IF(COUNTIF(rng_carrier,AK2)>0,"OK","CHECK")     43 characters
                                          OK      2 characters displayed
```

So the five formula columns were sized to their formulas and hit the
`max_column_width: 40` cap. The columns did not become wide when the formulas
were seeded a few rows deep — they became wide once `fill_down` put a formula in
every row, because auto-fit then found one in the column no matter where it
looked.

Ordering was not the problem. Auto-fit already ran after seeding.

## Change 1 — ignore formula cells when measuring

openpyxl cannot know a formula's computed value, so guessing a width from the
source text is worse than not guessing. Formula cells are now skipped, and the
width comes from the header and any literal cells.

Result on the real sheet: `Test Carrier` went from the 40 cap to 16, driven by
its header.

## Change 2 — explicit `width` in column_formats

```yaml
column_formats:
  - columns: ["Test Fresh", "Test Cans", "Test Dest", "Test Carrier", "SALE TYPE1"]
    width: 13
```

Widths are applied in a **separate pass after auto-fit**, since everything else
in a rule must run before it. Number formats and fonts affect how wide text
measures, so they have to be in place first; a stated width has to come last or
auto-fit overwrites it.

## Verification

```
auto-fit only, formula column:   40  ->  16
explicit width applied:          16  ->  13
neighbouring literal columns:    unchanged (Customer 17, Destination 26)
```

`test_format_excel_column_formats.py` — 10/10. Full suite 20 failures before and
after, identical to baseline.

# End of file #
