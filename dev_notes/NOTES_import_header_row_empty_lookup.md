# header_row, regex_replace null mask, empty lookup stages (2026-09-02)

- `import_file` gained `header_row` (1-based, default 1; xlsx/csv/tsv).
  Report exports that lead with title lines import straight to a headed
  frame. This also makes `on_missing_file: create_empty` usable for such
  files: with no slice-and-promote step downstream, `create_empty_columns`
  can declare the real header names.
- `clean_data` `regex_replace` now runs through `_apply_to_text_values`
  like every other text action, so blanks stay blank under pandas 2
  instead of becoming the literal text `nan`. Populated non-text values
  (datetimes in a mixed column) still stringify, which a first-line cut
  on newline-joined cells relies on.
- `lookup_data` no longer halts on an EMPTY lookup stage that carries the
  declared columns; it warns and returns every row unmatched. This closes
  a latent gap: a `create_empty` import could never reach its join, so the
  fail-safe was unreachable. An empty stage WITHOUT the declared columns
  still errors.

Tests: `tests/test_import_header_row.py`, `tests/test_lookup_empty_stage.py`.

Observed, pre-existing, not addressed: a `recipe_parent_dir`-based custom
variable only re-resolves after an external variable is added, so a recipe
with no `required_external_vars` fails at load with "Unresolved variables".
