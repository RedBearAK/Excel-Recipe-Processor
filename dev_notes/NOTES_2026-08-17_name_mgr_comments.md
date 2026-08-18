# Name Manager comments on defined names (2026-08-17)

The Comment column in Excel's Name Manager maps to the `comment`
attribute on the `definedName` element in `xl/workbook.xml`. openpyxl
supports it natively (`DefinedName(comment=...)`) and it round-trips
through save/load. Nothing in the zip-surgery/dynamic-array save path
touches it.

## The field: `name_mgr_comment`

Named explicitly for its destination so it cannot be mistaken for a
YAML-side note. Accepted in two places, one funnel serves both:

- `create_from_columns` range specs (per-range key)
- YAML library objects (`vms_named_lambdas.yaml` and any export),
  alongside the existing `description` field

`description` is unchanged: YAML-side long-form documentation with no
length ceiling. The two fields are independent.

## Funnel behavior (`write_named_object`)

- `_prepare_name_mgr_comment()` flattens all internal whitespace runs
  (folded YAML arrives with newlines; the dialog renders one line) and
  refuses text over 255 characters with a guided error naming the
  limit, the field, and where long-form docs belong. Excel would
  truncate silently; the funnel fails loud instead.
- Validation runs BEFORE the on_existing collision handling, so an
  over-length comment cannot delete the existing name and then die.
- `comment` or `name_manager_comment` in a range spec is caught with a
  guided error naming the real key - never silently ignored.

## Export harvest

All four extraction paths (global ranges, lambdas, formulas,
sheet-scoped names) emit `name_mgr_comment` when a stored comment
exists, so a comment hand-entered in Excel survives the
export -> YAML -> import cycle. Without this, comments would be the
one property that export silently drops. The key is omitted when
absent to keep exported YAML clean.

`validate_yaml` also checks comment lengths, so a library fails at
validation rather than mid-import.

## Excel access trivia (for the log)

Name Manager lives ONLY on the Formulas ribbon. Windows: Ctrl+F3.
Mac (this machine): fn+Cmd+F3 - fn+Ctrl+F3 is intercepted by the
system for the Dock.

## Tests

`tests/test_name_mgr_comments.py` (5 tests): storage + reload,
whitespace flattening, 255 refusal with guidance, wrong-key guidance,
full export/import round-trip. Capabilities snapshot refreshed
(`name_mgr_comments` added to write_features); drift alarm green.

# End of file #
