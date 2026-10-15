# External ties: audit + sever (2026-09-14)

Built and tested against `origin/dev_beta` at `5cd7728`. Two new
processors over one shared inventory, plus a four-line registration in
`core/pipeline.py`. This archive supersedes `erp_audit_external_ties.tgz`
(same audit files, unchanged except `external_ties_rgx.py` gained the
sever patterns).

## Files

```
excel_recipe_processor/processors/audit_external_ties_processor.py
excel_recipe_processor/processors/sever_external_ties_processor.py
excel_recipe_processor/processors/_helpers/xlsx_package_rgx.py
excel_recipe_processor/processors/_helpers/xlsx_package_write.py
excel_recipe_processor/processors/_helpers/external_ties_rgx.py
excel_recipe_processor/processors/_helpers/external_ties_inventory.py
excel_recipe_processor/processors/_helpers/external_ties_sever.py
excel_recipe_processor/processors/_examples/audit_external_ties_examples.yaml
excel_recipe_processor/processors/_examples/sever_external_ties_examples.yaml
excel_recipe_processor/core/pipeline.py        (HEAD + 4 lines; see patch)
docs/processors/audit_external_ties.md         (from --export-docs)
docs/processors/sever_external_ties.md         (from --export-docs)
tests/test_audit_external_ties.py
tests/test_sever_external_ties.py
tests/test_sever_external_ties_seams.py
tests/test_sever_external_ties_in_place.py
dev_notes/external_ties_audit_and_sever_2026-09-14.md
dev_notes/pipeline_registration.patch
```

`pipeline.py` in the archive is the dev_beta HEAD file with two
imports and two `registry.register` lines added after
strip_formula_caches. If the working tree has local changes to
pipeline.py, do NOT extract that one file; apply
`dev_notes/pipeline_registration.patch` instead.

## audit_external_ties (read-only)

One `zipfile` read; never writes. Finds: external link parts by `[N]`
index with target and sheet list; every `<f>` carrying `[N]` (sheet,
cell, referenced sheets, cached-value flag, shared-master ref);
defined names with `[N]` or `#REF!`; `[N]` outside sheetData classified
as CF / DV / **unclassified**; chart `<c:f>`; pivot chains sheet ->
table -> cache -> source (local ones listed, not counted as ties);
file hyperlinks; connections; query tables; OLE; orphans (unresolved
indexes, phantom declared links, dangling parts). The log splits
formula cells into retarget candidates (referenced sheet exists
locally) and freeze-or-refuse, noting how many have no cache.

## sever_external_ties (new-file output)

Design rulings from the 2026-09-14 discussion:

- **New file by default, in-place absent.** Output is
  `<stem><output_suffix>_<timestamp><ext>` beside the source (or in
  `output_dir`). An existing output name is refused, never
  overwritten. In-place becomes an explicit opt-in in a later version
  once the new-file mode has proven itself on real files.
- **Retarget first.** `[N]Sheet!` -> `Sheet!` when every named sheet
  exists locally; `[N]!Name` -> `Name` when a local defined name
  exists. Quoting preserved. Applies to formulas, defined names, CF,
  DV, chart series.
- **Default policies: freeze / drop.** Unresolved formulas freeze to
  the cached value (type attribute kept; shared groups freeze whole);
  unresolved names, CF rules, DV rules drop (emptied CF wrappers
  removed, DV count corrected). Each carrier can be set to `refuse`.
- **Forced refusals** regardless of policy: array/data-table
  formulas, chart series that do not retarget, external pivot
  sources, any surviving `[N]` in an unknown carrier, freeze targets
  with no cached value.
- **All-or-nothing.** Plumbing (externalReferences, link rels, link
  parts, overrides, calcChain) is removed only when zero `[N]`
  survives. Any refusal -> no output file; the JSON report is still
  written with the full `refused` list.
- **Post-write verification.** The new file is re-inventoried and
  must be free of links, `[N]` carriers and orphans, or it is deleted
  and the step fails.
- **Report buckets:** `fixed` (retargeted / frozen / dropped, with
  before and after), `refused` (kind, where, reason, text), `limbo`
  (file hyperlinks, connections, query tables, OLE - left in place),
  `plumbing_removed`, `before_summary`, `after_summary`.

Key vocabulary (no generic names): `files`, `output_dir`,
`output_suffix`, `timestamp_format`, `retarget_if_possible`,
`unresolved_formula_policy`, `unresolved_defined_name_policy`,
`unresolved_conditional_formatting_policy`,
`unresolved_data_validation_policy`, `fail_on_limbo`, `report_file`,
`name_cap`.

Recipe ordering note: sever must run BEFORE strip_formula_caches when
both appear, because freeze needs the cached values strip removes.

## Seam testing round (same day) - three real bugs found and fixed

`tests/test_sever_external_ties_seams.py` (10 tests) drove the seams
after the first five passed. Three findings changed the surgery:

1. **Cached values were scanned as carriers.** A cell whose cached
   VALUE was the text `[1]x` (a string formula result) tripped the
   post-resolution survivor scan and refused a clean file. Survivors
   are now scanned only in carrier parts (workbook.xml, worksheets,
   charts, pivot cache definitions), with `<v>` and `<is>` bodies
   blanked first. Shared strings, comments and docProps are never
   scanned: literal data is not a link.
2. **Orphan indexes retargeted.** `[12]Local!A1` with only two
   declared links was retargeted because `Local` exists. Retargeting
   now requires a DECLARED link index, and when the link part lists
   its source sheets, the named sheet must be in that list. Anything
   else falls to the carrier policy and is reported (a local sheet
   the source never had is a coincidence, not a match).
3. **x14 extLst carriers were half-classified.** The inventory mapped
   `x14:dataValidation` to `data_validation` by suffix, but the main-
   namespace DV pattern cannot rewrite it, so it surfaced only as an
   anonymous survivor. x14 forms now classify as `other:x14:...` and
   refuse by element name.

One fixture bug (missing content-type overrides made the test
package unloadable by openpyxl) was in the test, not the tool.

Also confirmed by the seam tests: two links where only one
retargets; a mixed formula freezes whole rather than half-rewriting;
`t="e"` caches freeze with style kept; Excel-order attributes; a
shared slave with no cache refuses and names itself; chart series
retarget or refuse; external pivot sources refuse; a workbook with no
calcPr gets exactly one; `retarget_if_possible: false` freezes
everything; a multi-file step writes the good file, skips the bad
one, fails, and reports a list; openpyxl can load and re-save the
output; XML entities inside formulas survive retargeting; a link part
with no `<sheetNames>` still permits retargeting.

## In-place mode and the write-and-verify proof (same day, piece 1 of the transplant work)

Three structural changes, no behavior change to the surgery itself:

1. **`_helpers/xlsx_package_rgx.py`** now holds the package-generic
   grammar (cells, formulas, relationships, calcPr, CF/DV elements,
   pivot/chart elements, part classifiers, attribute pulls).
   `external_ties_rgx.py` keeps only the `[N]` grammar and externalLink
   plumbing. The coming transplant processor imports the generic module
   by name, so no second copy of a cell pattern can exist.
2. **`_helpers/xlsx_package_write.py`** is the one write path for
   zip-level surgery, both modes. It writes a hidden temp beside the
   target (same directory so the final rename is atomic; real
   extension so a second parser will open it), then verifies:
   `testzip`; parts claimed removed are absent; the part set equals
   original minus removed plus added; **every part the plan did not
   claim to change is byte-identical to the original** (SHA-256);
   the caller's own check (sever: post-write inventory clean);
   optional openpyxl load. Only then is the temp renamed - to the
   new output path, or over the original after a `.severbak` copy.
   Any failure deletes the temp; nothing is replaced and no backup
   is taken.
3. **`sever_external_ties` gains `write_mode: new_file | in_place`**
   (default new_file) and `verify_with_openpyxl` (default false).
   In-place refuses when a `~$name.xlsx` lock is present, when a
   `.severbak` already exists (a rerun must not bury the true
   original), or when `output_dir` / `output_suffix` /
   `timestamp_format` are also given. `SeverPlan` records
   `changed_parts` (any `_set_text` that altered bytes) and
   `removed_parts`; those two sets are the surgery's claim.

`tests/test_sever_external_ties_in_place.py` (6 tests) covers the
happy path (backup is the untouched original, source holds the
result, no temp left), lock-file and stale-backup refusals, a refusal
leaving no backup, a deliberately tampered unclaimed part failing in
both modes with the source untouched, a claimed-removed part still
present failing, the new-file-only keys refused with in_place, and
the openpyxl check. Two findings during that round: the temp name
must keep the `.xlsx` extension or openpyxl refuses it, and the
still-present check has to run before the part-set check for its
message to be reachable.

The generic recipe carries `write_mode` as an external variable
(`--set write_mode new_file|in_place`; the stub always passes it) and
the stub has `--in-place`. Schema `choices` validation runs after
substitution, so the string form is checked properly.

What "certain of the lack of deleterious effects" still needs is the
check no code can run: Excel opening the result without a repair
prompt, on real files, in new-file mode, a few times. The in-place
write is then the same risk plus a rename.

## Verification (exit codes)

```
PYTHONPATH=. python3 tests/test_audit_external_ties.py     8/8  exit 0
PYTHONPATH=. python3 tests/test_sever_external_ties.py     5/5  exit 0
PYTHONPATH=. python3 tests/test_sever_external_ties_seams.py 10/10 exit 0
PYTHONPATH=. python3 tests/test_sever_external_ties_in_place.py 6/6 exit 0
PYTHONPATH=. python3 tests/test_strip_formula_caches.py    8/8  exit 0
PYTHONPATH=. python3 tests/test_examples_validate_against_schemas.py  exit 0
PYTHONPATH=. python3 tests/test_all_processor_examples.py  exit 0
PYTHONPATH=. python3 tests/test_schema_export.py           4/4  exit 0
PYTHONPATH=. python3 tests/test_declaration_lambda_and_registry.py  3/3 exit 0
pyflakes on every new module                               clean
python3 -m excel_recipe_processor --validate <recipe>      passes for both steps
```

Scale: 75,000 rows x 10 columns, 7,500 `[1]Ext!` formulas retargeted
and plumbing removed in 1.85 s end to end (inventory, surgery, write,
re-inventory).

## Known limits of v1 (reported, not hidden)

- Chart series can only be retargeted; freezing a series to a literal
  cache is possible but deferred until a real file needs it.
- `x14:` extLst variants of CF/DV carrying `[N]` land in `unclassified`
  and refuse. Add rules when one shows up in a real file.
- External pivot cache sources refuse; the cache records part cannot
  be regenerated without Excel.
- The link part's cached `sheetDataSet` is discarded with the part;
  frozen cells already carry their own `<v>`, so nothing is lost.

## Not done, on purpose

- `--export-docs` / `--export-schemas` regenerate nine unrelated pages
  whose descriptions drifted before this work. Only the two new pages
  are in the archive.
- No in-place mode. No transplant. No stub/recipe: run both steps on
  a real processed VMS the user has copied into, and on a real
  sheet-copy result, and read the JSON before deciding what v2 needs.

# End of file #
