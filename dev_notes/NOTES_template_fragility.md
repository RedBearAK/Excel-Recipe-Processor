# How fragile is the template path?

Every failure mode below was tested by deliberately breaking a template, not
reasoned about. What matters is not whether something breaks — it is whether it
breaks **loudly**.

## Results

| Change to the template | Before | Now |
|---|---|---|
| Rename a lookup **sheet** | halts, lists available sheets | unchanged |
| Rename a lookup **column** | halts, lists available columns | unchanged |
| Delete the donor formula rows | **warned and carried on** | **halts** |
| Rename a **named range** | **silently duplicated** | reported, optionally pruned |
| Reorder / add / remove columns on `VMS Data` | no effect | no effect |

The first two were already fine:

```
Sheet 'Product_IDs' not found. Available sheets: ['VMS Data', 'Products', ...]
Lookup columns not found: ['Product Form']. Available columns: [... 'Prod Form' ...]
```

Both name the problem and show what is actually there. Nothing to improve.

`VMS Data` is immune to structural edits because the recipe rewrites that sheet
wholesale — column order comes from `var_columns_final_order`, not from the
template.

## The two that needed fixing

### Deleted donor rows produced a formula-free file

```
✅ Transplanted 0 formulas successfully
⚠️  Found 25 empty source cells - check column specifications
⬇️  Filled 5 column(s) from row 7 to 84          <- filled nothing
```

Run "succeeded". Output had no formulas anywhere. The fill-down log line was
actively misleading, claiming five columns it had skipped.

Transplanting zero formulas is now an error, and the fill-down line only counts
columns that actually had a formula to continue:

```
No formulas found in donor '...' sheet 'VMS Data' at rows 2-6 for columns
[...]. The donor rows may have been deleted, or start_row may not point at
them. Writing a formula-free file silently is worse than stopping here.
```

### A renamed range silently duplicated

Renaming `rng_PID` to `rng_ProductID` in the template produced **14 names**: the
inherited `rng_ProductID` plus a regenerated `rng_PID`. Formulas kept working,
so nothing looked wrong — and the orphan would ride into the next generation.

`create_from_columns` now reports any `rng_`-prefixed name it does not define,
and `prune_orphans_with_prefix: "rng_"` removes them. Enabled in the template
recipe.

## Compared to assembling from scratch

**Fewer inputs.** Six lookup files become one template. Fewer things to move,
rename, or forget to update.

**Same class of dependency.** Both paths bind to sheet names and column headers.
The template path just has them in one file instead of six, and the failures are
equally loud.

**The real difference is accumulation.** From-scratch writes a fresh workbook
every time, so nothing can carry forward. Anything wrong is wrong once and then
gone. The template path inherits, so a mistake made in the template persists
until someone notices — which is the same property that makes it useful.

**The compounding risk is openpyxl round trips.** Each run puts the template
through one load-and-save. Charts, images, formulas, conditional formatting and
data validation all survive it, but **WMF images do not** — already observed on
the processed sample. Pivot tables are untested.

That is survivable exactly once. It compounds if each output becomes the next
template. **Pin one pristine template and copy it every run.**

## What to watch over time

- Do not chain outputs into templates
- Keep the donor formula rows; deleting them now halts rather than quietly
  producing a formula-free file
- If the template gains charts, images or pivots, verify they survive a run
  before relying on them
- Run the full `vms_process.yaml` occasionally to rebuild a template from the
  separate lookup files, and diff against the one in use

## Bottom line

Slightly more fragile per generation, because it inherits rather than rebuilds.
Considerably less fragile per edit, because there is one place to change things
instead of six. Every failure mode found is now loud.

The from-scratch recipe stays as the way back: if a template is ever wrong,
corrupted, or degraded, it rebuilds one from the original lookup files.

# End of file #
