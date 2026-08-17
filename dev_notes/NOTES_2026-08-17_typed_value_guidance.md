# Typed-variable guidance: multi-layer protection and discovery (2026-08-17)

dev_notes/NOTES_2026-08-17_typed_value_guidance.md

Per ruling: prevent the string-vs-numeric ordering crash in the limited
cases where it occurs, guide users to the ALREADY-IMPLEMENTED typed
variable references, and announce the feature in enough places that it
cannot stay undiscovered (this session's investigator initially did not
know it existed).

## The feature being announced

Typed references at the USE site substitute actual typed values:
{int:name}, {float:name}, {list:name}, {dict:name}, {bool:name},
{str:name} (core/variable_substitution.py, whole-scalar fullmatch).
Untyped {name} substitutes TEXT. Conversion happens at the reference,
loudly on failure, and works identically for YAML-declared, CLI
--variable, and interactive-prompt variables.

## Layer 1: prevention guards in filter_data (framework)

Two guard clauses in filter_data_processor.py, per the guard-clause
doctrine (make the type unambiguous BEFORE the operation):

- _require_numeric_comparison_value: the four scalar ordering
  conditions (greater_than/less_than/greater_equal/less_equal) reject
  string values BEFORE pandas sees them - a string there is never a
  valid operand, so nothing legitimate is disturbed. The error names
  the filter, column, and received string, teaches {int:}/{float:},
  and routes DATE comparisons to pandas_expression (the ordering
  conditions coerce the column numerically and cannot compare dates).
- _require_numeric_list_members: the eight min/max-in-list conditions
  reject string members, naming the lexical trap ('9' > '100') and
  teaching {list:}.

Equality conditions are deliberately UNguarded: default equals is
case-insensitive and string-normalizes both sides by design ('150'
matches 150), and case_sensitive equals keeps exact semantics.

## Layer 2: tests/test_filter_typed_value_guidance.py (4/4)

- guided error fires on all four scalar conditions, naming column,
  value, and both typed syntaxes
- list-member guard fires with the lexical-order warning and {list:}
- the SAME recipe fixed by '{int:min_sales}' end to end - with the
  variable arriving as a CLI-style STRING '120'
- equality semantics pinned untouched (both default and case_sensitive)

## Layer 3: discovery in examples

- filter_data_examples.yaml: new typed_variable_thresholds_example -
  full recipe with teaching comments on WHY typed references prevent
  errors when the value is not meant to be a string, covering both
  {int:} threshold and {list:} membership; revision date bumped.
- recipe_settings_examples.yaml (the --get-settings-examples source):
  the variables block now documents typed references, and its own
  min_order_amount footgun (quoted "500") is corrected to 500 with an
  inline reference hint; revision date bumped.
- integration_notes variable_substitution entry mentions typed
  references.

## Layer 4: living documentation in the test suite

test_new_comprehensive_test_of_processors' multi-processor workflow
reverted from the numeric-literal sidestep to '{int:min_sales}' with a
comment explaining what the untyped form would do - the suite now
exercises the typed path on every run.

## Incidental repairs (found while editing the examples file)

- The documented "date comparison" filter example (greater_than vs
  "2024-01-01") could never have worked: ordering conditions coerce
  the column with pd.to_numeric. Replaced with a numeric tier example
  plus a pointer to pandas_expression for dates.
- filter_data_examples.yaml carried DUPLICATE top-level
  integration_notes keys; YAML last-wins was silently discarding the
  first block's five notes. Merged into one block - nine note keys
  now live.

## Collateral verified green

test_filter_data_comprehensive, test_filter_typed_value_guidance,
test_new_comprehensive_test_of_processors (100%),
test_interactive_variables, test_comprehensive_variable_substitution,
test_all_processor_examples, test_usage_examples, test_external_yaml.

# End of file #
