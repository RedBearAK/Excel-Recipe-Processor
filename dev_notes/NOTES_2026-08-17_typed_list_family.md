# Typed list-reference family: list_int/float/str/any (2026-08-17)

dev_notes/NOTES_2026-08-17_typed_list_family.md

Per ruling: no bracketed syntax; expand the underscore family; catch
brackets as a typo; and force acknowledgement of member typing - bare
{list:name} is RETIRED in favor of {list_any:name} for intentionally
mixed/untyped members.

## The vocabulary

    {list_int:name}    members converted to int, loudly on failure
    {list_float:name}  members converted to float, loudly on failure
    {list_str:name}    members converted to str
    {list_any:name}    pass-through; the author acknowledged untyped
    {list:name}        RETIRED - guided error names all four

Container semantics unchanged: the variable must already BE a list
(CLI --variable strings still fail loud at the container check; there
is deliberately no parsing path from a CLI string to a list). Member
conversion errors name the member index, value, and target type.
{dict:name} untouched.

## Why underscores over {list[int]:name} (decision record)

Unknown underscore variants ({list_itn:x}) fail loud through the
EXISTING typed-reference path for free, forever. Unknown or half-typed
bracket variants would pass through as literal text - the worst
failure mode in the house book - and need their own malformed-syntax
net. Brackets also make unquoted YAML a flow-mapping parse error while
underscores stay inert. Three concrete named member types cover every
realistic need; a composition mini-grammar buys generality nobody
asked for.

## The bracket typo net

Because the bracketed syntax is intuitive enough to type accidentally,
it is caught as a typo instead of ignored: patterns in the new
core/variable_substitution_rgx.py (house _rgx convention) detect both
the complete {word[...]: shape and dangling {word[... forms (covers
{list[int:x}), raising "not ERP vocabulary" with the underscore family
in the message. Wired at the head of _detect_variable_syntax_typos.

## Breaking-change sweep (46 sites, 10 files)

All bare {list:...} references updated repo-wide. Column-name lists
became {list_str:...}; a numeric fixture in the comprehensive
variable tests was typed honestly as {list_int:...} (its assertion
result[99] == 99 caught the blanket choice - the sweep verified per
site, per the blanket-rename incident doctrine); error-path probes
became {list_any:...} to keep exercising their original failure
modes. Also swept: filter guard messages (now teach
{list_int:}/{list_float:}), generate_column_config's emitted recipe
snippets, capability/docstring examples in variable_substitution and
recipe_pipeline, and all three example yamls including the
typed_variable_thresholds_example (whose in_list value is now
{list_int:allowed_tiers} with a comment contrasting {list_any:...}).

## Tests

New tests/test_typed_list_references.py (5/5): per-family member
conversion with type assertions, indexed conversion failure, bare-list
retirement guidance naming all four replacements, container check on
CLI strings, bracket net on both complete and dangling forms.

Collateral green (11 modules): both filter suites, all three variable
suites, comprehensive processors (100%), all three example checkers,
generate_column_config, verify_columns.

## Open question, deliberately unbuilt

The general unresolved-braces catchall (any {...} surviving all
substitution passes fails loud) remains available if wanted; this
delivery scoped to the bracket shapes per ruling.

# End of file #
