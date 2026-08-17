# Test-suite repair campaign: 83/108 to a trustworthy 107/108 (2026-08-17)

dev_notes/NOTES_2026-08-17_test_suite_repair_campaign.md

Continuation of HANDOFF_2026-08-16_test_failure_survey.md. All six
buckets are closed. This note records what changed, the framework bugs
found along the way, the false-green audit, and the repo-side commands
that complete the delivery.

## Final score, and why it is trustworthy

107/108 test modules pass by exit code, run SERIALLY with direct
`PYTHONPATH=. python3 tests/...` invocations. The one red is
test_load_stage_processor, which tests a deleted processor and is
resolved by the deprecation rename below.

The score is stronger than the old 83 in a way the number hides: an
exit-wiring audit found NINE modules that printed internal failures
while exiting 0 - false greens inside the original 83. Every module's
final output was grepped for failure marks after the last sweep: zero
marks anywhere. Exit codes and internal verdicts now agree.

## Repo-side commands to run at delivery (TGZs cannot delete/rename)

```
git mv tests/test_load_stage_processor.py tests/__DEPRECATED__test_load_stage_processor.py
git rm excel_recipe_processor/config/examples/recipe_settings_examples.yaml
```

The second completes commit e6acc80's half-applied sweep (see below);
the TGZ ships the file at the new config/_examples/ path.

## Framework bugs found and fixed (the real treasure)

1. **pandas 3 dtype breakage, FOUR sites.** String columns report
   dtype 'str' under pandas 3, so `dtype == 'object'` comparisons
   silently reject every string column. Confirmed and fixed in:
   filter_terms_detector auto-detection (both the text and categorical
   classifiers - detection returned EMPTY on all-string data), and
   sort_data's `_apply_standard_sort` and `sort_by_multiple_criteria`
   (ignore_case silently folded nothing). All four now use
   `pd.api.types.is_string_dtype()`. A repo-wide grep for the pattern
   found no other live sites; merge_data already used the safe form.

2. **NA-destroying case fold.** sort_data's ignore_case used
   `astype(str).str.lower()`, which turns None into a sortable literal
   string - na_position silently broke the moment the dtype fix made
   folding active. Both sites now use an NA-preserving
   `map(lambda v: v.lower() if isinstance(v, str) else v)`.

3. **config/examples orphaned for 13 months.** Commit e6acc80
   (2025-07-29, "Change processor examples folder name") swept BOTH
   path constants in processor_examples_loader.py to `_examples` but
   only the processors/ folder was renamed on disk. Every
   `--get-settings-examples` call since served the hardcoded builtin
   fallback instead of the authored yaml. File now lives at
   config/_examples/; the loader's stale error strings were fixed too.
   Failure class: sweep-partial-application (same family as the
   sheets-rename incident).

4. **CLI --detailed crash.** The capabilities renderer assumed
   `parameters` values are dicts; filter_terms_detector supplies plain
   strings -> `'str' object has no attribute 'get'`, rc 1. Fixed with
   an isinstance guard in core/main.py.

5. **Lost designed CLI behavior restored.** The all-usage-examples
   view lost its opening RECIPE SETTINGS section in a renderer
   rewrite; the help text lost the `Use "settings"` alias instruction
   and its epilog example. Both restored; the not-found message now
   tips the settings alias.

6. **Guided guards for bare TypeErrors.** Missing `input_file`
   (import) and missing `output_file` (export) reached `Path(None)`
   and died with raw TypeErrors; both now fail with guided
   StepProcessorErrors.

7. **export_file ignored separator/encoding.** FileWriter implements
   them; the processor never forwarded them, so `separator: ';'`
   silently produced a comma CSV. Now forwarded.

8. **Retired-key rejections (per the no-silently-ignored-keys
   ruling).** `stage_description` -> guided error, guard promoted to
   BaseStepProcessor so every processor inherits it (descriptions live
   in the declared stages block). `check_column_data` -> guided error
   (analysis is always on; the flag was read into an attribute and
   never consulted).

9. **normalize_color requires strings.** Unquoted hex in YAML is a
   corruption trap (`000123` parses as int 123, leading zeros vanish,
   str() coercion blessed a plausible wrong color). Shared with
   conditional_format; both suites green.

10. **Stale doctrine strings.** The format_excel phase-path error now
    says 'sheet_name'; generate_column_config no longer suggests the
    retired check_column_data flag.

## Doctrine clarifications the tests now encode

- **FileReader/FileWriter do not substitute variables.** The
  processor layer resolves {date}-style templates BEFORE the I/O
  layer. Reader: unresolved template fails loud with the braces
  visible. Writer: braces are legal filename characters, so an
  unresolved template writes LITERALLY - visible evidence of an
  upstream bug. The retired `variables=` kwarg raises TypeError.
- **Strings are sheet NAMES, ints positional for internal callers.**
  The handoff's calamine "int degrades to str" theory was wrong: the
  numeric-string coercion was deliberately removed 2026-08-14 (it
  shadowed tabs literally named "1"). sheet='1' failing loud is the
  doctrine working.
- **Stage saves are pipeline-driven.** `save_output_data` runs in
  `execute_stage_to_stage()`; direct `execute()` never saves. Tests
  that need the save drive it explicitly.
- **fix_dates outputs Excel-friendly MM/DD/YYYY strings** (data
  preserving), not datetime64 columns.
- **Unresolvable sheets fail loud** in format_excel (the old silent
  skip hid typos); test_nonexistent_sheets now asserts the raise.
- **Backup naming** is `{stem}_erpbkup_{timestamp}{ext}`; two tests
  globbed the old `*.backup*` pattern.

## Example-file convention (per ruling: full-recipe style wins)

Five files converted from the doc-shape (parameter_details with
`required:` data keys) to the copy-paste recipe convention with
settings blocks and `# REQ -`/`# OPT -`/`Default value:` markers:
flush_workbooks, profile_sheets, profile_workbooks,
profile_named_objects, verify_excel_storage. Output contracts and
incident notes survived as comments inside the yaml blocks. Three more
files needed only REQ markers. The __DEPRECATED__ example files are
now skipped by all eight checker globs.

The revision-date checker was redesigned per ruling: presence +
YYYY-MM-DD parse + not-in-future, replacing the hand-maintained
today literal. Six example files gained headers.

## Test-side repairs worth remembering

- **Fixture casualties of the original blanket-rename incident**: data
  columns named 'name' had been swept to 'step_description' in
  test_rename_columns_processor (failing) and test_sort_data_processor
  (booby trap). Both restored.
- **Stage-name collisions across tests** in one module (import) fixed
  with unique stg_-prefixed names.
- **Root-privilege false negative**: an invalid-path test used
  /nonexistent/..., which SUCCEEDS when tests run as root. Replaced
  with a path THROUGH a file (NamedTemporaryFile blocker) - invalid
  regardless of privileges.
- **tuple-return unpacking**: `_read_excel_headers_super_fast` returns
  (headers, analysis); `'x' in headers` on the tuple was the whole
  "Empty: 0" mystery.
- **pd.isna over `is None`**: pandas 3 surfaces missing values as nan
  in tolist(); identity checks against None miss them.
- **test_recipe_validator's hand-maintained required-fields table**
  went stale (lookup_data row). Updated + hazard comment; deriving
  from get_minimal_config() would remove the duplication - design
  call left open.
- **slice_data start_row is 1-based**; a workaround demo used 2 where
  the header row is row 1.

## Exit-wiring audit (the sweep-integrity fix)

Modules that printed verdicts but always exited 0, now wired with
`sys.exit(0 if success else 1)`: import_file, file_reader,
file_writer, sort_data, aggregate_data, clean_data, rename_columns,
config_validation, verify_fixes, header_promotion_issue,
new_comprehensive_test_of_processors. Two integration modules
(new_recipe_pipeline, new_yaml_first) turned out clean - their ❌
lines are handled-failure narration inside passing tests.

## Benchmark policy (Bucket F)

test_openpyxl_performance defaults to a sub-second CORRECTNESS run
(quick functionality + Small scenario, methods-agree only - at that
size the optimized path is SLOWER on fixed overhead, so a speedup
assertion would be meaningless). ERP_RUN_BENCHMARKS=1 opts into the
multi-minute Medium/Large scenarios where the >10x assertion applies.

## Loose ends, deliberately left

- test_seed_donor_formulas_functional writes
  formula_transplant_verification.xlsx into the CWD (repo root when
  run from there). Passing, but a hygiene candidate for tempfile.
- The comprehensive test's filter step now uses a numeric literal
  because substituted variables arrive as strings and greater_than vs
  int64 raises. Whether filter_data should coerce numeric-looking
  strings for comparisons is a design question, not assumed here.
- pytest is now a test-environment dependency (test_basic imports it);
  direct runs remain the authoritative verdicts.

# End of file #
