# NOTES: _xlop optional parameters + lambda helpers, harvested and
# pinned (2026-08-14)

Second harvest (test-named-lambdas-lets.xlsx, now
tests/fixtures/harvest_2026-08-14_named_optional_helpers.xlsx) closed
every open storage-grammar question. The refusal-pending-harvest
discipline paid out in full: the optional-parameter guess candidates
were `[_xlpm.y]` and plain `_xlpm.y` - Excel's actual form is NEITHER.

## Harvested facts, all byte-pinned in Oracle C of the corpus test

- OPTIONAL LAMBDA PARAMETERS store the DECLARATION as `_xlop.name` -
  a THIRD prefix, brackets gone entirely - while every in-scope
  OCCURRENCE stores as `_xlpm.name` (harvest E1:
  _xlfn.LAMBDA(_xlpm.x,_xlop.y,IF(_xlfn.ISOMITTED(_xlpm.y),...))(5)).
  The transformer now emits this, refuses required-after-optional
  ordering (Excel's own rule), refuses bracket syntax on LET names,
  and stays idempotent through the _xlop intermediate.
- All three NAMED forms (multi-param lambda, named LET, LET inside a
  lambda body) match our generated grammar byte-for-byte, '='-less.
- Calling cells store the BARE name (HarvestLam(3,4), HarvestLet) as
  t="array" formulas; the pipeline provably leaves such calls alone.
- PIVOTBY and PERCENTOF confirmed `_xlfn.` (they were family-inferred;
  the xlsxwriter differential had flagged them as its table lags 2024
  functions) - PIVOTBY's eta argument stores `_xleta.SUM` as expected.
- MAP and REDUCE harvested `_xlfn.`; SCAN/BYROW/BYCOL/MAKEARRAY added
  alongside them, verified by the xlsxwriter differential rather than
  by family inference alone.
- A scalar-result LET cell (harvest L1) stores as a PLAIN formula, no
  t="array" - noted; our declare-everything-injected approach is
  already Excel-verified and unchanged.
- Name Manager showing #VALUE! for named lambdas is Excel's normal
  display for a lambda awaiting arguments - not a defect.

Full suite after integration: 4/4, 3/3, 2/2, 3/3, 3/3, 2/2, 4/4, 6/6
across the eight modules, direct python3 runs. Corpus now carries
THREE oracles: xlsxwriter's serializer and two real-Excel harvest
fixtures, with every generated grammar this project emits pinned to
at least one of them.
