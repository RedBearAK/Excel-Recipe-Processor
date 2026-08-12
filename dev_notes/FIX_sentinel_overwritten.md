# The fill was writing over the sentinel row

Supersedes the seed_donor_formulas in `fix_seed_gap_and_array_mode.tgz`.
Contains that fix plus these.

## Cause

`find_last_data_row` measures the data extent across **every** column by
default. The sentinel row populates ten of them:

```
PRODUCT  FORM, PRODUCT  GROUP, REGION, World Region, Country,
Test Fresh, Test Cans, Test Dest, Test Carrier, SALE TYPE1
```

So the sentinel counted as data, the fill extent included it, and formulas were
written straight over the XXXX markers in the five formula columns. The other
five kept their XXXX because nothing writes to them — which is exactly the
half-and-half row you saw:

```
XXXX  XXXX  0  0  0  0  Domestic
```

Pre-existing, as you guessed. It has been happening since `fill_down` was added.

## Two independent fixes

**1. The fill never overwrites an existing value.** The transplant already
refuses to; the fill now behaves the same way. A cell holding something was put
there deliberately.

```
⬇️  Left 5 cell(s) alone that already held a value
```

Verified on its own, with no anchor configured: sentinel at row 20 kept its
XXXX while rows 18 and 19 filled normally.

**2. `fill_anchor_columns` accepts header names**, and both recipes now anchor
on `Van Number`:

```yaml
fill_anchor_columns: ["Van Number"]
```

`Van Number` is populated on every real row and blank on the sentinel, so the
extent stops one row short of it. Previously this option only took column
letters, which leaked an implementation detail into the recipe — passing
`"Van Number"` failed with *"is not a valid column name. Column names are from
A to ZZZ"*.

The two are deliberately redundant. The anchor keeps the extent honest; the
overwrite guard keeps a mistake harmless.

## Result

```
row 83 (last data)   Test Carrier  =COUNTIF(rng_carrier,AK83)
                     SALE TYPE1    =_xlfn.IFS(AW83=1,"Fresh",...)

row 84 (sentinel)    Test Carrier  XXXX
                     SALE TYPE1    XXXX
```

## On the ordering point

Appending the sentinel after the fill would also solve it, and is arguably
cleaner. It is not currently possible: the sentinel is added in the stage
pipeline via `combine_data` before the workbook is written, and no processor
appends a row to an existing file. Making the fill respect the sentinel achieves
the same outcome without a new capability, and the overwrite guard is worth
having regardless.

Suites: 3/3, 5/5, full suite 20 failures as at baseline.

# End of file #
