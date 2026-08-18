# NOTES: excel_data_validation processor (2026-08-14)

New processor writing native Excel data-validation rules: in-cell
dropdowns, numeric/date/time/text-length bounds, custom-formula gates,
input prompts, and styled error alerts. Session-aware FileOps processor
modeled on conditional_format. Design settled in-conversation before any
code (house rule); this file records the decisions, the sharp edges, and
what to eyeball on the first real output.

## Files in this delivery

- `excel_recipe_processor/processors/excel_data_validation_processor.py` (new)
- `excel_recipe_processor/processors/_helpers/excel_data_validation_rgx.py`
  (new - ISO date / clock time bound recognition)
- `excel_recipe_processor/processors/_helpers/range_patterns.py` (patched -
  appended `spill_anchor_ref_rgx`; that module is the declared single home
  for range-addressing patterns, and `cell_ref_rgx` / `range_ref_rgx` were
  reused from it rather than duplicated)
- `excel_recipe_processor/processors/_examples/excel_data_validation_examples.yaml` (new)
- `excel_recipe_processor/core/pipeline.py` (patched - import + registration,
  alphabetical position)
- `tests/test_excel_data_validation_processor.py` (new - 4/4 passing, house
  style, real files written and reopened)

## Vocabulary decisions (with rationale, for the future sweeps)

- **`apply_to_ranges` is ALWAYS a list, no singular `range` key.** The
  value is plural by nature (Excel's own sqref is a list), one-element
  lists are costless in YAML, and string-or-list polymorphism is exactly
  what the sheet-addressing sweep bled to remove. `range` / `ranges` /
  `sheet` all draw guided errors naming the canonical key. If a later
  standardization sweep sets a framework-wide string-or-list convention,
  this processor changes with the sweep - it starts clean.
- **List sources are three flat, mutually exclusive keys** (`values_list`,
  `list_from_named_range`, `list_from_spill_ref`), not a typed sub-dict.
  Excel has exactly these sources; there is nothing to extend toward.
  Zero or multiple given draws a guided error listing all three.
- **Bounds spell intent, not storage slots.** `operator` + `minimum` /
  `maximum` for between / not_between; `operator` + `compare_to` for the
  six comparisons. `formula1` / `formula2` are translation targets only -
  which slot means what depends on the operator, so the storage names
  fail the naming doctrine.
- **`whole_number`, not Excel's bare `whole`** - PROVISIONAL. The user
  deferred this ("ask about the whole number thing again later. We might
  tweak it"). Renaming later is a one-dict change plus examples/tests.
- **Canonical types:** list, whole_number, decimal, date, time,
  text_length, custom. All seven ship in v1: openpyxl treats them
  uniformly (type / operator / formula1 / formula2), so the marginal cost
  over list-only was near zero once the vocabulary existed.
- No standalone on-disk twin (unlike declare_dynamic_formulas): the
  processor already targets an on-disk `target_file` like format_excel,
  so there is no second entry point to build.

## Sharp edges (do not simplify)

- **OOXML `showDropDown` is INVERTED**: attribute true means SUPPRESS the
  in-cell arrow. openpyxl even aliases it `hide_drop_down`. Config
  `show_dropdown: true` (the default) maps to attribute-ABSENT;
  `show_dropdown: false` maps to `showDropDown=True`. Verified against
  openpyxl 3.1.5 source; test asserts the inversion.
- **Leading `=` is stripped** from every stored formula (named ranges,
  spill refs, bounds, custom) because Excel's own files omit it inside
  `<formula1>`. The 2026-08-14 probe stored `'=rng_customers'` and it
  round-tripped through openpyxl, but storage survival is not Excel
  acceptance - stripping matches Excel's native storage, so it is the
  safer default. Covered by the first-output eyeball below.
- **ISO date / clock time bounds convert** to `DATE(y,m,d)` /
  `TIME(h,m,s)` formulas; Excel does not evaluate raw ISO strings in DV
  bounds. Anything starting `=` or matching neither pattern passes
  verbatim (cell refs, named ranges, explicit formulas).
- **Inline list guards**: items may not contain commas or double quotes
  (Excel's quoted inline form cannot represent them), and the joined
  quoted form must fit the 255-character DV formula limit. All three
  draw guided errors pointing at `list_from_named_range`.
- **`$` stripped from sqref ranges** before `dv.add()`. openpyxl
  normalizes them away anyway (verified), but the stored sqref should not
  depend on library behavior.
- **Enforcement scope**: Excel DV fires on MANUAL entry only - paste,
  fill-down, and formula results bypass it silently. It is a guardrail
  for the human end user, not a pipeline integrity check (that is
  verify_data's job). Stated in capabilities and the examples header.

## OPEN: first-real-output Excel eyeball checklist

Same class as the conditional_format `_xlfn` question. On the first
production workbook carrying these rules, open in real Excel and check:

1. **Spill ref `#`**: does a `list_from_spill_ref` dropdown populate, or
   does Excel want `ANCHORARRAY(...)`? If it fails, the fix lives in the
   dynamic-array zip-rewrite layer (core/dynamic_array_metadata.py), not
   here.
2. **Named-range dropdown** populates from its lookup tab (the
   `=`-stripping decision).
3. **Warning-style alert** actually offers the override path (Yes/No),
   distinguishing it from stop.

## Verified by tests (real files, reopened with openpyxl)

- Inline list quoted (`"Open,Closed,Pending"`), multi-area sqref
  (`B2:B100 D2:D100`) accumulated space-separated per OOXML.
- Named range and spill ref stored `=`-stripped, `#` intact.
- Interval bounds land in formula1/formula2 with `between`; comparison
  operator names map to Excel camelCase.
- `DATE()` conversion, `$B$1` passthrough.
- showDropDown inversion, allowBlank, prompt and error-alert fields.
- Seven guided-error cases including both retired keys and the
  comma-in-item redirect.

## Queue interaction

This retires the "data_validation processor" NEXT UP item from
HANDOFF_2026-08-14. The recipe-authored interactive mini-report pattern
(inject_formulas + declaration + manage_named_objects + this dropdown) is
now complete as a capability class. Next per the queue: wire verify_data
into the CMA recipe.

# End of file #
