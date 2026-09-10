# {col:Header} in range fields, and two substituter faults it uncovered

`dev_notes/2026-09-10_column_placeholders_in_ranges.md`

## The ask

The VMS recipe's Van_List tab has three filter pick blocks right of the
spill. When two columns joined the spill, every directive addressing
those blocks - two conditional-format rules, a cell format, three
dropdown validations - said "N2:P5" literally and had to be re-lettered
by hand. Kris: there is an opportunity to use column references so the
directives keep their integrity.

## What changed

`resolve_column_placeholders(text, worksheet, header_row, context, lock)`
in `_helpers/excel_range_resolver.py`, beside the header lookup it uses.
Three processors call it at apply time, on the target sheet's header row:

- `conditional_format` - `range:` (formulas already resolved {col:};
  now the literal target does too, UNLOCKED: "N2:P5", since openpyxl
  strips "$" from range strings anyway)
- `format_excel` - `cell_formats[].cells` (resolved into a copy of the
  rule before the helper's A1 check, so the recipe config is not mutated)
- `excel_data_validation` - `apply_to_ranges` (the config-time A1 regex
  now accepts the placeholder form; resolution happens at apply)

A missing header is a StepProcessorError naming the header, the sheet
and the available headers. A literal range passes through untouched.

The recipe can now say
    range: "{col:Filter: SALE TYPE1}2:{col:Filter: Process Year}5"
and mean the block wherever it sits.

## Two pre-existing faults the first test run hit

Neither is in the new code; both are in variable substitution, and the
placeholder-in-range recipe was the first to write a string shaped
"{col:A}2:{col:B}5" through it.

1. The typo heuristic ("word:name}" with no "{" before the colon)
   walked back from the colon in "}2:{" and hit the earlier "}" first,
   so it refused "{col:Start}2:{col:End}5" as a missing opening brace.
   A colon whose next character is "{" is a range separator, never a
   half-typed reference; it is now skipped. "{x}:{y}" was refused by the
   same path.

2. Masked by (1): the simple-variable pass skipped any "{name}"
   immediately followed by ":" as if it were part of a format spec
   ("{name}:something" is not a syntax; "{name:fmt}" is, and the simple
   pattern cannot match that anyway). Once (1) let "{x}:{y}" through,
   it came back "{x}:2" - half substituted, silently. The skip is gone.
   "{first}2:{last}5" -> "N2:P5" now, which is what a recipe building an
   A1 range from two variables would expect.

Both pinned in test_enhanced_variable_substitution.py, including the
real typo the heuristic exists for, which is still refused.

## Checked

- tests/test_column_placeholders_in_ranges.py: the SAME recipe run
  against a sheet with the block at N:P and again at P:R lands every
  directive on the right cells both times; literal ranges unchanged; a
  missing header is a clear error.
- The whole suite: 137 of 137 modules.
- The only positional reference left in the VMS recipe's Van_List
  formatting is the spacer column's width, because its header is a
  single space, which strips to nothing and cannot be named. That is
  a letter in one variable, and it is the only one.


# End of file #
