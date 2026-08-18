# fill_down for seed_donor_formulas

## The old concern no longer applies

The worry was that filling formulas down cell by cell would be slow, because
each one needs its row references adjusted the way Excel does it.

openpyxl ships `openpyxl.formula.translate.Translator`, which does exactly that,
and it is fast. Measured at real VMS scale — 8,681 rows across the five formula
columns:

| | formulas | seconds | file size |
|---|---|---|---|
| seed 5 rows (previous behaviour) | 20 | 15.9 | 3.79 MB |
| fill every row | 43,395 | 15.0 | 4.22 MB |

**The marginal cost is nil.** Loading and saving the workbook dominate
completely, and `seed_donor_formulas` already pays both. The translation itself
ran at roughly **104,000 cells per second** — 0.42 s for all 43,395.

Translation is correct, too: relative references shift, named ranges do not.

```
origin AZ2:  =IF(COUNTIF(rng_carrier,AK2)>0,"OK","CHECK")
   -> AZ8681: =IF(COUNTIF(rng_carrier,AK8681)>0,"OK","CHECK")
```

## Usage

```yaml
processor_type: "seed_donor_formulas"
columns: ["Test Fresh", "Test Cans", "Test Dest", "Test Carrier", "SALE TYPE1"]
start_row: 2
row_count: 5
fill_down: true              # continue to the last populated row
fill_anchor_columns: []      # OPT - columns measuring the data extent
```

`fill_anchor_columns` defaults to every column in the sheet, so a sparse column
cannot cut the fill short. Name specific columns to override.

A column whose seed cell holds no formula is skipped rather than filled with a
constant.

## One thing to watch in Excel, not here

Generation is cheap; **recalculation might not be**. `rng_carrier` is defined as
`Carriers!$A$2:$A$1048576` — a full column, chosen so a hand-added carrier can
never fall outside it. A `COUNTIF` against that range, evaluated 8,681 times,
is more work for Excel than the same test against `$A$2:$A$9`.

Excel usually optimises `COUNTIF` to the used range, so this may be a non-issue.
But if the workbook feels sluggish on open, that is the first thing to change:
switch `rng_carrier` to `row_mode: "data"` in the `manage_named_objects` step.
The trade-off is that a hand-added carrier would then sit outside the range
again — which is the exact bug that made `full_col_no_header` attractive.

That tension only exists because formulas now fill down. With a handful of
seeded rows it did not matter.

## If you prefer the manual fill

Set `fill_down: false`. The `XXXX` sentinel row exists for that: click a seeded
cell, Ctrl-Shift-Down, Ctrl-D.

# End of file #
