# audit_external_ties — read-only external-ties inventory (2026-09-14)

Built against `origin/dev_beta` at `5cd7728`. All new files plus a
two-line registration in `core/pipeline.py`.

## Why an audit first

The 2026-09-14 discussion settled three related pieces of work:

1. **Sever/repair** — a step that goes further than Excel's Break
   Links: retarget `[N]Sheet!` references to a local sheet of the same
   name where one exists, and freeze / drop / refuse everything else,
   per carrier kind.
2. **Tab transplant** — replace one ERP-generated tab inside a
   workbook the user has kept editing (pivots and all), by zip-level
   part surgery so the destination never passes through openpyxl.
3. **Pivot staleness** — pivots on a transplanted tab keep working if
   the tab name is stable; the only exposure is a fixed-range source
   that no longer covers grown data, and column renames. Setting
   `refreshOnLoad` on the affected caches covers the refresh itself.

All three need the same inventory of where ties live. The audit ships
that inventory alone, read-only, so the later rewrite logic is written
against categories actually seen in real files.

## Files

```
excel_recipe_processor/processors/audit_external_ties_processor.py
excel_recipe_processor/processors/_helpers/external_ties_inventory.py
excel_recipe_processor/processors/_helpers/external_ties_rgx.py
excel_recipe_processor/processors/_examples/audit_external_ties_examples.yaml
excel_recipe_processor/core/pipeline.py        (HEAD + 2 lines; see patch)
docs/processors/audit_external_ties.md         (from --export-docs)
tests/test_audit_external_ties.py
dev_notes/audit_external_ties_2026-09-14.md
dev_notes/pipeline_registration.patch
```

`pipeline.py` in the archive is the dev_beta HEAD file with the two
lines added after the strip_formula_caches import and registration.
If the working tree has local changes to pipeline.py, do NOT extract
that one file — apply `dev_notes/pipeline_registration.patch` instead.

## The problem grid the inventory covers

| Section | Where it looks | Tie? |
|---|---|---|
| `external_links` | `workbook.xml` externalReferences → rels → `externalLinkN.xml` (+ its rels for the target path) | yes |
| `formula_refs` | `<f>` text carrying `[N]`, per worksheet; enclosing cell by walking back to `<c ` | yes |
| `defined_names` | `workbook.xml` names carrying `[N]` or `#REF!`; `localSheetId` resolved to a sheet name | yes |
| `sheet_features` | `[N]` outside `sheetData`, classified by nearest enclosing carrier: CF, DV, or **unclassified** | yes |
| `chart_refs` | `<c:f>` in chart parts carrying a bracket | yes |
| `pivots` | sheet rels → pivotTable → its rels → cacheDefinition → `worksheetSource` | only when the source is external |
| `file_hyperlinks` | sheet hyperlink rels, `TargetMode="External"`, not http(s)/mailto | yes |
| `connections`, `query_tables`, `ole_objects` | part presence | yes |
| `orphans` | `[N]` with no reference; declared references nothing uses; `externalLink*` parts nothing points at | yes |

Local-source pivots are listed but not counted as ties: the transplant
pre-flight needs `source_sheet`, `source_ref` and `refresh_on_load`
even when everything is local.

An `unclassified` sheet feature is the deliberate hole in the grid:
any `[N]` carrier the classifier does not name is reported loudly
with a snippet rather than dropped. That is where the freeze / drop /
refuse vocabulary of the sever step will be extended from.

## Retarget candidates

The log splits formula cells into those whose referenced sheet name
exists in the audited workbook (retarget candidates for the repair
step) and those that can only be frozen or refused, and notes how
many of the latter carry no cached value — nothing to freeze to. That
last count is the reason sever must run BEFORE strip_formula_caches
in any recipe that uses both.

## Verification

```
PYTHONPATH=. python3 tests/test_audit_external_ties.py     # 8/8, exit 0
PYTHONPATH=. python3 tests/test_strip_formula_caches.py    # 8/8, exit 0
PYTHONPATH=. python3 tests/test_examples_validate_against_schemas.py   # exit 0
PYTHONPATH=. python3 tests/test_all_processor_examples.py  # exit 0
PYTHONPATH=. python3 tests/test_schema_export.py           # 4/4, exit 0
PYTHONPATH=. python3 -m excel_recipe_processor --validate <recipe>  # passes
```

Scale: a 75,000-row × 10-column sheet with 7,500 `[1]` formulas
inventories in 0.16 s (one regex pass per part; no per-cell callback).

Read-only guarantee is tested by SHA-256 before/after on an
openpyxl-authored file.

## Not done, on purpose

- `--export-docs docs/processors` and `--export-schemas md` both
  regenerate nine OTHER pages whose descriptions drifted from the
  schemas before this work (conditional_format, excel_data_validation,
  format_excel, import_file, …). Only the new page is in the archive;
  run the generators when convenient to pick up the drift.
- No sever/repair or transplant code. Next step is running the audit
  on a real processed VMS the user has copied into, and on a real
  sheet-copy result, and reading the JSON.

# End of file #
