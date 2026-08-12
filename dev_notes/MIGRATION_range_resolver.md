# Migration: shared range resolver (revised against dev_beta)

Built against `origin/dev_beta` @ `77e48f9`. Note that `dev_beta` and `main`
are **content-identical** — the tree diff between them is empty, and main
carries only the merge commits. Working from `dev_beta` is safe.

This revision corrects two mistakes in the earlier draft, both caused by
working from a partial snapshot instead of the repo:

- `seed_donor_formulas` **is** already registered in `pipeline.py`. The earlier
  instruction to add it was wrong. No change to `pipeline.py` is needed.
- `manage_named_objects` **exists** and is registered. See "Overlap" below.

---

## Extract and go

Every file in this archive is complete and ready to overwrite its counterpart.
Two files were modified from their `dev_beta` versions; the rest are new.

| File | Status |
|---|---|
| `_helpers/range_patterns.py` | new |
| `_helpers/excel_range_resolver.py` | new |
| `_helpers/defined_name_validator.py` | new |
| `processors/seed_donor_formulas_processor.py` | modified — one import line |
| `processors/manage_named_objects_processor.py` | modified — write half + scope bug fix |
| `tests/test_seed_donor_formulas_basic.py` | modified — one import line |
| `tests/test_excel_range_resolver.py` | new |
| `tests/test_defined_name_validator.py` | new |
| `tests/test_manage_named_objects_write.py` | new |
| `processors/generate_column_config_processor.py` | modified — 3 bug fixes |
| `tests/test_generate_column_config_fileops.py` | modified — stale assertions + exit |
| `tests/test_generate_column_config_processor.py` | modified — exit convention |
| `tests/test_seed_donor_formulas_functional.py` | modified — exit convention |
| `TEST_EXIT_CONVENTION.md` | new — revised written rule |

---

## One manual deletion

`excel_recipe_processor/processors/_helpers/formula_patterns.py` is now empty
of consumers and should be deleted:

```sh
git rm excel_recipe_processor/processors/_helpers/formula_patterns.py
```

It held three patterns. A repo-wide grep found that only one had any consumer:

| Pattern | Consumers found | Disposition |
|---|---|---|
| `excel_column_ref_rgx` | `seed_donor_formulas_processor.py`, `test_seed_donor_formulas_basic.py` | moved to `range_patterns.py`, both imports repointed |
| `excel_cell_coord_rgx` | none | dead; superseded by `cell_ref_rgx` |
| `excel_range_ref_rgx` | none | dead; superseded by `range_ref_rgx` |

The two dead patterns were not carried over verbatim. Their replacements in
`range_patterns.py` also accept absolute markers, which the originals did not:

```
excel_cell_coord_rgx    ^[A-Z]{1,3}\d+$              rejects "$A$1"
cell_ref_rgx            ^\$?[A-Z]{1,3}\$?\d{1,7}$    accepts both
```

Confirm nothing else referenced them before deleting:

```sh
grep -rn "formula_patterns\|excel_cell_coord_rgx\|excel_range_ref_rgx" --include="*.py" .
```

The only expected hit is a local variable named `formula_patterns` at
`inject_formulas_processor.py:534`, which is unrelated to the module.

---

## The write half of `manage_named_objects`

All five stubbed operations now work, plus one new operation.

| Operation | Before | Now |
|---|---|---|
| `export_all` | working | unchanged |
| `list_objects` | working | unchanged |
| `export_filtered` | `NotImplementedError` | implemented |
| `import_all` | `NotImplementedError` | implemented |
| `import_filtered` | `NotImplementedError` | implemented |
| `validate_yaml` | `NotImplementedError` | implemented |
| `copy_direct` | `NotImplementedError` | implemented |
| `create_from_columns` | did not exist | **new** |

### `create_from_columns`

The operation that motivated the resolver. Ranges are computed from column
names against real data extent on every run, so they cannot fall behind the
data the way a hand-maintained range does.

```yaml
- step_description: "Define lookup ranges on the generated workbook"
  processor_type: "manage_named_objects"
  operation: "create_from_columns"
  target_file: "output/vms_lookups.xlsx"
  # OPT - 'error' (default), 'replace', or 'skip'
  on_existing: "replace"
  ranges:
    - name: "rng_PID"
      sheet: "Product_IDs"
      columns: ["Product ID"]
      row_mode: "data"
    - name: "rng_payterms"
      sheet: "Sales_Orders"
      # Adjacent entries expand to cover intervening columns
      columns: ["Payment Terms", "Deposit Application"]
      row_mode: "data"
      # Measure extent from a dense column, not a sparse one
      anchor_columns: ["SO No."]
    - name: "rng_carrier"
      sheet: "Region-Carrier"
      columns: ["Carrier"]
      # Cannot go stale when a human appends a row
      row_mode: "full_col_no_header"
```

Per-range keys: `name`, `sheet`, `columns`, `row_mode`, `header_row`,
`anchor_columns`, `expand_span`, `force_column_names`, `on_missing`,
`absolute`, `scope`.

Step-level keys: `target_file`, `ranges`, `on_existing`, `name_validation`.

### Policy defaults, and why

`on_existing` defaults to `'error'`. Replacing a name silently is a form of
data loss, so opting into `'replace'` is deliberate. Recipes that regenerate
their output file will not hit this, since a fresh file has no names.

`name_validation` defaults to `'house'` for `create_from_columns` and
`'excel'` everywhere else. Names a recipe authors should follow the
convention; names read out of a foreign workbook are already valid by
construction and must not be rejected for style.

### Pipeline ordering

Named ranges must exist before formulas referencing them, or Excel resolves
those formulas to `#NAME?` on open. openpyxl writes both without complaint,
so the breakage only appears when a human opens the file:

```
export_file -> manage_named_objects -> inject_formulas -> format_excel
```

### Tables are not created

`import_all` and `copy_direct` report table definitions under
`skipped_tables` rather than attempting to build them. Recreating a table
requires a data region of matching dimensions already in place, which this
processor cannot guarantee. Named ranges, formulas, and lambdas all transfer.

---

## Bug fixed: sheet-scoped names were invisible

`extract_local_objects()` scanned `workbook.defined_names` for entries with a
non-null `localSheetId`. In openpyxl 3.1 (3.1.5 is current) sheet-scoped names
moved onto the worksheet and no longer appear in `workbook.defined_names` at
all, so that loop could never match and every local name was silently dropped
from exports.

Now reads `worksheet.defined_names` directly. Verified against openpyxl 3.1.5:

```
wb.defined_names.keys()          -> ['rng_glob']
wb['Data'].defined_names.keys()  -> ['rng_loc']
```

Worth knowing that all eleven names in the VMS lookup files are
workbook-scoped, so this bug did not affect that extraction.

---

## generate_column_config: three bugs, previously masked

These were hidden because the module's inverted exit code made a failing run
look like a passing one. See `TEST_EXIT_CONVENTION.md`.

**1. pandas 3.0 incompatibility.** `read_csv(..., infer_datetime_format=False)`
at line 223. The parameter was removed in pandas 3.0 (3.0.2 is current), and it
was redundant anyway — `nrows=0`, `dtype=str`, and `parse_dates=False` on the
same call already prevent inference. Removed.

**2. Existence checking ran at construction time.** `_validate_file_operation_config()`
called `Path(file_path).exists()`, which runs during `__init__`. That breaks two
things:

- Capability discovery instantiates every processor from `get_minimal_config()`,
  whose paths are placeholders. `--list-capabilities` reported
  `Could not load capabilities` for this processor.
- Variable substitution resolves paths *after* construction, so any recipe using
  `source_file: "downloads/export_{date}.xlsx"` was rejected before `{date}`
  could be substituted.

Existence checking moved to `_validate_input_files_exist()`, called at the top of
`perform_file_operation()`. Required-parameter and extension checks stay at
construction, since neither depends on runtime state.

**3. Missing `get_operation_type()` override.** Every other FileOps processor
declares one (`excel_formatting`, `formula_injection`, `named_objects_management`).
This one fell through to the base default. Now returns `column_config_generation`.

### Test assertions were stale, not wrong-headed

Commit `2343e08` (2025-08-12, "Rename some output keys") renamed the YAML output
keys and moved them under `settings.variables`. The tests were last touched
2025-08-11 and still asserted on the old flat names:

| Old assertion | Current output |
|---|---|
| `raw_columns:` | `settings.variables.var_columns_original` |
| `desired_columns:` | `settings.variables.var_columns_to_keep` |
| `rename_mapping:` | `settings.variables.var_columns_to_rename` |
| `columns_to_create:` | `settings.variables.var_columns_to_create` |
| `recipe_section:` | `example_recipe:` |

One more representational change: interior empty columns are preserved as
`"Empty: N"` placeholders rather than bare empty strings (commit `3f0908a`).
The trimming behaviour is correct — the column *is* kept, trailing ghosts *are*
dropped — only the marker changed. Assertions updated to match.

Also note the recipe section emits a `rename_columns` step only when renames
exist, so that indicator was removed from the unconditional assertion list.

---

## Overlap with `manage_named_objects`

`named_objects_patterns.py` defines `valid_excel_name_rgx` as
`^[A-Za-z_][A-Za-z0-9_]*$`, which overlaps `defined_name_validator`. The two
were left separate on purpose, but the overlap should be resolved when the
write half of that processor is built.

The existing pattern is more permissive than Excel in two ways and stricter in
one:

- Rejects periods, which Excel permits (`rng.PID` is a legal name)
- Rejects a leading backslash, which Excel permits
- Does not catch names Excel reads as cell references (`Q1`, `TAX24`)
- Has no 255-character cap

That is harmless today: `manage_named_objects` only ever **reads** names out of
existing workbooks, where they are already valid by construction. Validation
only starts to matter when names are written. Since `_execute_import_all`,
`_execute_import_filtered`, and `_execute_copy_direct` all currently raise
`NotImplementedError`, nothing writes yet.

Recommendation: when the write operations are implemented, have them call
`validate_defined_name()` and retire `valid_excel_name_rgx`. Doing it now would
change the behaviour of working read paths for no benefit.

---

## Verify

```sh
python tests/test_excel_range_resolver.py        # expect 13/13
python tests/test_defined_name_validator.py      # expect 7/7
python tests/test_seed_donor_formulas_basic.py   # expect 3/3
python tests/test_manage_named_objects_write.py  # expect 9/9
python tests/test_manage_named_objects_processor.py
excel-recipe-processor --list-capabilities | grep -E "seed_donor|named_objects"
```

# End of file #
