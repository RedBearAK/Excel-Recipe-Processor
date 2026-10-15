# erp_metadata: frontmatter for every ERP YAML

`dev_notes/2026-10-14_erp_metadata_frontmatter.md`

## The ask

Two things surfaced on the same afternoon. First, a session validated the
VMS recipe against ERP `main` while the recipe had been written against
`dev_beta`, and the seven `unknown key` errors that resulted were
puzzling until the branch skew was spotted. Second, Kris asked how a cold
reader could tell that the named-lambda library the recipe imports is
static data and not a second recipe that "does something" - the file
format already enforces that (the importer requires the library sections
and the loader requires `recipe:`), but nothing in either file SAYS it.

Kris's ruling: a manual tag, not a minimum-requirement claim; optional
stamping later; the comparison reported by default; keys that read right
out of context, not short ones under a generic parent; and the one
include file so far gets migrated now, with other recipes warned at the
beginning and end of a run until they are populated as they are touched.

## What changed

Every ERP YAML can open with one top-level block:

```yaml
erp_metadata:
  erp_file_kind: recipe
  last_validated_via_erp_version: "20261014.2"
```

`core/erp_metadata.py` owns it: `inspect_erp_metadata(document,
expected_kind, source_label)` validates the block with the processors'
own `Schema` / `validate_config` (closed mapping, unknown key with a
nearest-name suggestion, `erp_file_kind` from `('recipe',
'named_objects')`, the version in the `YYYYMMDD.N` shape `_version.py`
uses) and returns an `ErpMetadataReport` with the lines to log.

- Absent block: a warning at the start of the run and a reminder at the
  end. Never fatal - an old recipe keeps running.
- Present and well-formed: the version comparison is always logged.
  Same version: info. Runner newer: info with a nudge to re-validate and
  re-tag. Runner OLDER than the tag: a warning at start AND end - "keys
  this file uses may not exist here yet; update ERP before trusting
  unknown-key errors". That is the afternoon's confusion, in one line.
- Present but malformed, or the wrong kind for the door it came through
  ("declares erp_file_kind: named_objects, but a recipe was expected
  here - this is a named-objects library, not a recipe"): fatal, from
  the loader, before any step runs.
- Not the first top-level key: an info note; convention puts it first,
  where frontmatter would be.

Wired in `core/recipe_pipeline.py` (`load_recipe` inspects and logs the
start lines; both completion paths - a real run and `--validate` - log
the end reminder) and in `manage_named_objects` `_validate_yaml_structure`
(expected kind `named_objects`).

### named_objects files

The library format's old `metadata` block (`source_file`, `export_date`,
`total_objects`) was written by `export_all` as provenance and never read
on import - `total_objects` was not verified. It is folded into
`erp_metadata` under names that say what they are:

```yaml
erp_metadata:
  erp_file_kind: named_objects
  last_validated_via_erp_version: "20261014.2"
  named_object_count: 14
  exported_from_workbook: book.xlsx        # export_all writes these three;
  exported_on: 2026-10-14T20:00:00         # a hand-authored library has
  exported_with_openpyxl_version: 3.1.5    # no workbook to cite
```

`named_object_count` is now VERIFIED against the four sections on
import; a mismatch names the real composition. `export_to_yaml_format`
writes `erp_metadata` first and no longer writes `metadata`; the
extraction's internal working dict is unchanged. An old file with a
`metadata` block still imports, with a note to migrate. A file with a
top-level `recipe` key handed to the import is refused by name.

`import_all` also logs the composition of what it imported ("14
lambdas, 0 named ranges, 0 named formulas, 0 named tables - static
definitions, written into the workbook's Name Manager"), so the log
itself says the step moved data, not control.

## Files

- `core/erp_metadata.py` (new)
- `core/recipe_pipeline.py`
- `processors/manage_named_objects_processor.py`
- `tests/test_erp_metadata.py` (new, 5 tests)
- `_version.py` 20261014.2

Recipe side, delivered as loose YAML: `vms_process.yaml` gains the block
after its header comment; the library is renamed
`vms_process_named_lambdas.yaml` (Kris, so it sits beside the recipe in
a file manager), its `metadata` block replaced by `erp_metadata` with
`named_object_count: 14`, and the recipe variable updated.

## Verification

- `tests/test_erp_metadata.py` 5/5: absent is reported not fatal; the
  three version comparisons with their levels and end-reminder presence;
  eight rejections (unknown key with suggestion, bad kind, missing kind,
  two bad version shapes, both wrong-door cases, a provenance key on a
  recipe); the export-built block round-trips; not-first is a note only.
- Regression modules green: manage_named_objects (processor, write,
  profile), all_processor_examples, new_recipe_pipeline_integration,
  recipe_validation_phase, recipepipeline_functionality,
  pipeline_self_discovery, add_calculated_column_next_workday.
- Live `--validate` of vms_process.yaml: "erp_file_kind recipe; last
  validated via ERP 20261014.2, running the same" then 128 steps, 0
  warnings. With the tag set to 20261101.0: the OLDER warning at start
  and end. The library imports with kind named_objects and 14 objects;
  a count of 15 against 14 fails naming the composition.

## Not done, on purpose

- `--stamp` (write the running version into the file after a clean
  validate). Kris ruled stamping optional and manual; the reporting is
  the payoff and that is on by default. Add when a hand-edit cycle gets
  tiresome.
- A schema version for the recipe format itself. `last_validated_via_
  erp_version` already pins the one version that matters; a second
  thing to keep in sync has no payoff yet.
- Renaming `recipe:` to something tool-specific. 242 example blocks, 12
  modules and every live recipe would move for a labelling problem the
  block solves in one line.

# End of file #
