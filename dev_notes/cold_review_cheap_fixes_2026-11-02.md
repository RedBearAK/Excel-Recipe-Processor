# Cold-review follow-up: the cheap bucket (2026-11-02)

dev_notes/cold_review_cheap_fixes_2026-11-02.md

A reviewer with no history on the project (core modules, README, docs,
one example file; no processors, no config_schema, no workbook_session)
produced a design and maintainability review. Kris's rulings on it and
the fixes that followed are recorded here so the next reader does not
relitigate them. Version 20261102.2.

## Rulings on the review (Kris, 2026-11-02)

- Deprecation windows for renamed keys: NO. Alpha; the recipe vocabulary
  may change; the loader names the retired key and an LLM can migrate a
  large recipe. README now says so (Conventions section).
- Default `source_stage` to the previous step: NO. Each step names what
  it works on; the next step may work on something else entirely, and a
  step can be moved as long as its source still exists where it lands.
- Merging processors that share data shape (verify_*, profile_*,
  lookup/merge/group): NO. The separations exist so a result can be
  found by purpose without reading a dictionary.
- `step_description` as ceremony: NO. It is the handle for everything.
- `run()` per family instead of the isinstance ladder: not now. It
  changes nothing visible and only pays off when a fifth family arrives.
- Workbook as a named object: true observation; the recipes already set
  the path once in a variable, and any number of workbooks may be in
  flight, so variables stay the mechanism.
- Bare-brace variable syntax: AGREED, and `{{ }}` preferred (changes
  both ends visibly; double braces never occur in a regex or an Excel
  formula, so nothing needs escaping). Queued as its own session; not in
  this delivery.
- Undeclared stage only a warning while declared-unused is an error:
  AGREED, fixed below.
- License references: GPL-3.0 is correct; the MIT mentions were starter
  defaults. Fixed below.

## Fixed in this delivery

### Documentation and metadata
- README badge and `pyproject.toml` license/classifier: GPL-3.0.
- README: 48 processors (audit_external_ties, sever_external_ties were
  missing from the table); repository and issues links replaced the
  `your-repo` placeholders; alpha/vocabulary note added.
- `format_excel_examples.yaml`: "NEW" tags and the "will be warned and
  skipped" claim removed; revision date 2026-11-02.
- `X_broken_tests/README.md`: says what the folder is (a parking lot).
- `current_capabilities.json` refreshed: it had drifted at the
  2026-10-14 `next_workday` delivery and `test_capabilities_snapshot_drift`
  had been failing since.
- `core/pipeline.py` docstring now says what the module holds
  (registration and capability reporting); the orchestrator is
  `recipe_pipeline.py`. Unused imports dropped.

### Fail-loud stragglers
- `on_error` vocabulary was split: the schema accepted
  `halt | skip | continue` while the pipeline knew `halt | continue |
  log_and_continue | skip_remaining`. `skip` validated clean and ran as
  halt after a warning; two real actions were unreachable from a
  validated recipe. `ErrorAction` now lives in `config_schema` and the
  schema key's choices derive from it. An unknown `settings.on_error`
  (the one path the schema does not see) is a load error, not a warning.
- `{date:FMT}` with a bad format wrote the FORMAT STRING into the output
  name. The warn-and-return branch was nearly dead: strftime on
  glibc/macOS passes an unknown directive through ('%Y-%m-%Q' ->
  '2026-11-%Q') and a spec with no directive comes back verbatim. The
  check is now on the result ('%' surviving, or unchanged) and raises
  `VariableSubstitutionError` - which the validation phase surfaces, so
  a bad date format now fails `--validate`.
- Undeclared stage: an error whenever the recipe declares ANY stages
  (it has opted into the drift check, so the check must be complete);
  still a warning - now saying the check is off - for a recipe that
  declares none, so the small generic recipes without a stages block
  keep working. One line in `recipe_validation.py` makes it fully strict
  if that is ever wanted.

### Dependency direction
- `core/recipe_pipeline.py` imported `mirror_print`, `attach_log_file`
  and `discard_early_log_buffer` from `core/main.py`, the CLI entry
  point. That block (133 lines, its own module state) is now
  `core/run_log.py`; `main.py` imports the names from there, so CLI code
  and anything else that reached them through `main` still works. The
  pipeline's six lazy imports became one module-level import.
  `tests/test_log_file_output.py` pokes the module state and now targets
  `run_log`.

## Tests
Full suite 151/151 (parallel map, then the changed modules serially).
Two sandbox-only failures (`pyarrow` absent) cleared by installing it.
Planted faults against `vms_process.yaml`, each refused by `--validate`
with the defect named: `on_error: skip` in settings; a misspelled
`save_to_stage`; a `{date:YYY-MM}` in an output name.

## Not done (deliberately)
- `{{ }}` delimiter migration: own session.
- Singleton core / `RunContext`: correct observation, no cost at one
  recipe per CLI run.
- `format_excel` key nesting, `definitions:` block: not yet.

# End of file #
