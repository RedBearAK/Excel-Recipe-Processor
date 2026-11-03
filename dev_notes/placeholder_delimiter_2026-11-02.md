# Placeholder delimiter: `{name}` -> `{{name}}` (2026-11-02)

dev_notes/placeholder_delimiter_2026-11-02.md

Version 20261102.3. The cold review's strongest finding, Kris's ruling,
and the migration.

## Why

Every placeholder - `{name}`, `{str:name}`, `{list_str:name}`,
`{date:FMT}`, `{col:Header}` - used single braces, which is also what a
regex quantifier (`\d{2,4}`), an Excel array constant (`{1,2,3}`) and a
Python dict in a pandas expression use, and there was no escape. The
variable layer had to know about `col:` (a processor's syntax) only
because they shared a delimiter, and the typo detector had to guess
whether a lone `{` was a mistake. `vms_process.yaml` already carried one
collision (`\d{2}` in a `pandas_formula`) that worked only because the
quantifier starts with a digit.

Ruling (Kris, 2026-11-02): doubled braces, `{{ }}` - both ends change
visibly, so a half-migrated file is obvious; double braces never occur
in a regex, an Excel formula or Python source, so nothing needs
escaping. Verbatim after the colon: the text between `{{col:` and `}}`
is the name, untrimmed and unquoted, as Excel's own `Table1[ Header ]`
works; a header with a trailing space is referenced by typing it.
Quoting inside the braces was considered and rejected - the tokens
already sit inside YAML quoting and Excel string literals, and a third
quote layer is noise for a case that has never occurred. The retired
single-brace form is a hard stop, naming the doubled form.

## What changed

Four parsers recognize placeholders; all now share one grammar module.

- `core/variable_substitution_rgx.py`: the live patterns
  (`TYPED_VARIABLE_RGX`, `SIMPLE_VARIABLE_RGX`,
  `FORMATTED_VARIABLE_RGX`, `COLUMN_PLACEHOLDER_RGX`), the retired-form
  detector (`RETIRED_SINGLE_BRACE_RGX`: identifier-first single-brace
  tokens only, so `{2,4}`, `{1,2,3}` and `{'a': 1}` are not matched),
  the mismatch/padding patterns, and `retired_form_message()`.
- `core/variable_substitution.py`: the three passes use the patterns;
  the typo detector is rewritten (retired form -> fatal; one side
  doubled; unbalanced `{{`/`}}`; padding `{{ x }}`; empties; bracketed
  types in either brace count); `_tok()` spells placeholders in messages
  so no f-string needs quadruple braces. `RetiredPlaceholderSyntaxError`
  (a `VariableSubstitutionError` subclass) exists so the tolerant
  settings-variable re-resolution path can catch it first and halt
  instead of warning. The detector runs on ANY brace, so a retired token
  is caught even where no live token exists.
- `core/column_tokens.py`: `{{col:` ... `}}`, verbatim name, retired
  `{col:` refused; `col_tok()` for messages.
- `processors/_helpers/inject_formulas_rgx.py`: `column_placeholder_rgx`
  IS the core `COLUMN_PLACEHOLDER_RGX`; `opaque_span_rgx` doubled. The
  range resolver and the formula injector use it unchanged.
- Fast-path checks: the three "anything to substitute?" gates stay on a
  single `{` (so the retired form reaches the detector); the two "still
  holds a placeholder?" checks (`config_schema` allow_unresolved,
  pipeline unresolved-variable report) are on `{{`/`}}`.
- `core/recipe_pipeline.py`: `settings.variables` re-resolution halts on
  the retired form (it warned-and-continued on any failure; a recipe
  whose only old token was in a settings variable would have run with
  the token unsubstituted).

Not changed: the range resolver still `.strip()`s the name it looks up
and compares against stripped header cells (consumer-side matching,
pre-existing); the tokenizer is verbatim. The migration changes the
delimiter only.

## The sweep

`dev_notes/sweep_placeholder_braces.py FILE...` (`--check` to preview).
Idempotent. Doubles identifier-first single-brace tokens only. Refuses
to write if the result, with every brace run collapsed to one, differs
from the original the same way - so it provably changes braces and
nothing else. In `.py` files it rewrites STRING and COMMENT tokens only,
via `tokenize`: the first version was line-based and doubled 29 dict
comprehensions (`{name: expr for ...}` has exactly a placeholder's
shape) before the tokenize audit caught it. f-strings are skipped and
their lines reported - a placeholder spelled in one needs four braces,
by hand (eight lines across three tests). Comments holding a
commented-out f-string are skipped too. `.format()` templates are a
hand check: `test_seed_donor_fill_down.py` builds formulas with
`'B{row}'.format(row=...)`, which is not a placeholder.

Swept: 48 package/test `.py` files, every `_examples/*.yaml` (both
directories), `examples/`, `README.md`, `docs/`. `vms_process.yaml`: 589
tokens; `vms_process_named_lambdas.yaml`: none. Historical
`dev_notes/*.md` were deliberately NOT swept - they record the syntax
of their day; the ledger gained an entry instead.

## Proof

- `tests/test_placeholder_delimiter.py` (6 checks, ~40 assertions):
  live forms; single braces as text (quantifier, array constant, dict,
  lone braces); every retired form refused naming the doubled one;
  mismatch, padding, unbalanced, empty, bracketed; tokenizer verbatim
  (trailing-space name kept); injector and resolver patterns incl. a
  colon inside a name.
- Full suite 151/152 before packaging (the one is the version backstop,
  cleared by this bump); the earlier failures were all test-side
  artifacts of the sweep meeting Python templating, each listed above.
- `vms_process.yaml` before the sweep: `--validate` halts at the first
  retired token, naming `{{recipe_parent_dir}}`. After: 135 steps, 0
  warnings. The swept file collapses byte-for-byte to the original.
- Drill on the 261102 merged download through the real processors: the
  future-dates expression (six `{{col:}}` columns in nested `np.where`)
  flagged 11 rows, matching an independent pandas count; the
  missing-tracking export + format pair produced the same 5-row,
  navy-header, zoom-125 file as before the change.

## Follow-ups

- Other recipes (CMA invoices, contracts, tech-bin generics) are not in
  the session: run the sweep script on each, then `--validate`. The
  retired form halts the run, so a missed file cannot pass silently.
- `_examples/__DEPRECATED__*.yaml` (two files) were swept along with the
  rest; they are candidates for the same deletion as the processors.

# End of file #
