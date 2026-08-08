# Copy-the-file workflow

Your reframing was right and simpler than mine. The answer to "that would be
about all, right?" is yes, with one caveat about openpyxl.

`vms_process.yaml` is untouched and still works. The new file is
`vms_process_from_template.yaml`.

## What survives a copy, and what has to be redone

An `openpyxl` load-and-save round trip was tested against a workbook carrying
one of everything:

| Preserved | Lost |
|---|---|
| sheets, named ranges, formulas | **WMF images** |
| fonts, fills, freeze panes, auto-filter | pivot tables *(untested)* |
| hidden columns, column widths, number formats | |
| conditional formatting, data validation | |
| charts, PNG images | |

Better than the old wisdom suggests — charts and PNGs both came through intact.

**The WMF loss is real and already observed**: an earlier run against the
processed sample logged `wmf image format is not supported so the image is being
dropped`. If the template carries WMF artwork, it goes on the first run and
cannot come back.

Pivot tables could not be constructed here to test. openpyxl's pivot support is
partial, so treat a template containing pivots as unproven.

**Practical consequence:** pin the template. Do not chain each output into the
next run's template, or any such loss compounds across generations. One pristine
template, copied fresh each time, takes the round trip exactly once.

## What the new recipe does

29 steps against 43.

| Step | Fate |
|---|---|
| Import and clean the download | unchanged |
| Filter rows (Phase B) | unchanged |
| Import 6 lookup files, trim, filter, clean | **gone** - read 3 sheets from the template instead |
| Enrich, shape, sort, sentinel | unchanged |
| Export 7 sheets | **replaced** by copy-template-and-swap-one-sheet |
| Create 13 named ranges | kept - see below |
| Seed and fill formulas | source is now the template itself |
| Format 7 sheets | only `VMS Data` - the rest kept their formatting |

The trimming and whitespace cleaning of lookups disappears entirely. Those steps
existed to cope with the raw split-out files exporting as padded 42x9 blocks with
stray whitespace cells. The template's sheets were written by a recipe and are
already clean.

## New: `template_file` on export_file

```yaml
processor_type: "export_file"
source_stage: "stg_vms_export_ready"
template_file: "{recipe_parent_dir}/vms_template.xlsx"
output_file: "{output_dir}/{output_basename}.xlsx"
sheet_name: "VMS Data"
```

Byte-copies the template, then replaces only the named sheet. The sheet is
cleared in place rather than deleted and recreated, so it keeps its position in
the tab order.

One thing this path needed that the pandas path gave for free: pandas NA
sentinels raise `Cannot convert <NA> to Excel` when written to cells directly,
so they are converted to blanks first.

## Why the named ranges are still regenerated

The template already has all 13, and they come along with the copy. Keeping the
step is one line of cost and covers the case that matters:

```
template edited: added carrier ZIM, added Product ID 99999
  -> rng_PID  Product_IDs!$C$2:$C$1915  becomes  $C$2:$C$1916
```

Without regeneration the inherited range would still stop at the old last row,
and the new product would sit outside it. That is the `rng_carrier` / `APL` bug
returning through a different door.

## Verified

Editing the template and re-running:

- carrier `ZIM` appeared in the output
- product `99999` appeared, and `rng_PID` extended to match
- **Product Form match rate went from 99.9% to 100%** - adding that product ID
  resolved rows that had been reporting `UNMATCHED`

All six lookup sheets came through byte-identical in row count and header
formatting, the 13 names survived, and `VMS Data` kept position 1.

That is the property the manual workflow had: fix something once, and every
later file inherits it.

## Keep both recipes

`vms_process.yaml` builds everything from the six separate lookup files. It is
how a template gets made in the first place, and how to start over if a template
is ever lost or corrupted.

`vms_process_from_template.yaml` is the day-to-day one.

Build a template by running the full recipe, then trimming the data rows:

```python
import openpyxl
wb = openpyxl.load_workbook('output/<a good output>.xlsx')
ws = wb['VMS Data']
ws.delete_rows(7, ws.max_row - 6)      # keep header + 5 donor formula rows
wb.save('vms_template.xlsx')
```

The donor rows are what `seed_donor_formulas` reads, so do not remove them.

# End of file #
