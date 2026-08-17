# NOTES: typing modernization - judged, not blanket (2026-08-16)

Per the ruling: NOT a purge. The dying PEP-585-era aliases migrate to
natural syntax; typing citizens that are staying (Any) remain where
they are the honest contract; guard clauses stay the preferred
narrowing mechanism over annotation.

## What the inventory actually showed

Raw grep counts lied: Dict(19)/Tuple(1)/Set(2) had ZERO imports -
those were docstring prose. The real code surface: Any in 21 modules,
Optional in 5, Union in 5, List in one reader. 9 modules changed;
the other 14 import only Any and were left untouched.

## The transform

Bracket-aware (Optional[dict[str, int]] -> dict[str, int] | None;
Union split on top-level commas), applied file-wide so docstring
List[/Dict[ mentions modernize too, then the typing import recomputed
per file: 'from typing import Any' where Any survives as a real
contract (execute(data: Any) validated by guards - the doctrine
already in action), the line REMOVED where nothing remains
(filter_terms_detector, openpyxl_excel_reader now typing-free).
Leftover audit is AST-BASED - annotations only - after two false
trips on docstring Args prose shaped like annotations
('required_fields: List of...'). Per-file ast.parse before every
write.

## Verification and three PRE-EXISTING failures (not this sweep)

Targeted tests for every transformed module: fill_data, split_column,
aggregate, add_subtotals, diff_data, variable substitution (frozen
timestamp + comprehensive), all PASS by exit code. Three failures
were proven pre-existing by rerunning against origin/dev_beta's OWN
bytes, which fail identically:
- test_calamine_reader 4/5: positional sheet index reaches the reader
  as the string '1' through FileReader routing
- test_filter_terms_detector_processor 6/7
- test_interactive_variables 4/6
All three predate this change and deserve a repo-side look (this
sandbox may also differ environmentally, e.g. optional calamine).

Not in scope, deliberately: per-site Any elimination. Any at the
execute()/get_config_value() boundaries is the true contract; removing
it buys nothing the guard clauses inside are not already providing.
