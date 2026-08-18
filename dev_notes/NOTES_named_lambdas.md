# NOTES: named LAMBDAs - the supported path around the _xlpm gap (2026-08-14)

Answer to "is LET/LAMBDA a problem for the ERP tool, and what about
named manager entries?" - with a latent-bug fix the question flushed out.

## The state of play

- CELL-INJECTED LET/LAMBDA: refused fail-loud by the injector (their
  declared names need _xlpm. storage prefixes it does not implement;
  stored bare they are repair-bait). _xlpm support in the injector
  remains queued.
- NAMED LAMBDAS via manage_named_objects: SUPPORTED TODAY, and the
  better architecture anyway. The yaml_file import path translates
  human syntax to the harvest-verified stored grammar:
      definition: 'LAMBDA(v, IF(v="", "", v))'  +  parameters: ["v"]
      -> =_xlfn.LAMBDA(_xlpm.v,IF(_xlpm.v="","",_xlpm.v))
  Cells then CALL the name - fnBlankSafe(FILTER(...)) - which contains
  no LAMBDA(/LET( text for the guard, stores verbatim, and evaluates
  its argument ONCE. Define once, call clean everywhere: a workbook
  function library as a recipe capability.
- NAMED LET / LET inside lambda bodies: still a gap - the translator
  prefixes only the DECLARED lambda parameters; LET would introduce
  additional _xlpm names it does not know about. The injector guard
  covers cell-level LET; hand-authored named formulas containing LET
  remain the one unguarded corner (avoid until _xlpm lands).

## Latent bug fixed: blanket legacy prefixing

_add_excel_prefixes carried a pre-standardization hand-rolled
"common functions" list that _xlfn.-prefixed LEGACY names (SUM, IF,
VLOOKUP, MAX...) - every one of those stored prefixed is an unknown
identifier to Excel. Harvest evidence for the correct form: Excel
stores =GROUPBY(...,LAMBDA(x,SUM(x))) with SUM BARE. The bug hid
because round-trips prefer excel_definition and skip translation;
hand-authoring a definition (this session's fnBlankSafe) exercised it.

Fix follows shared-machinery doctrine: the body now goes through the
injector's map-based prefix_future_functions (legacy bare, future
prefixed), and parameter _xlpm.-prefixing gained full token boundaries
plus string-literal safety via apply_outside_strings (re-runs never
double-prefix; a parameter name inside a quoted string is never
touched; comma-space stripping also skips strings now).

Tests: test_named_lambda_translation.py 2/2 - harvest-grammar cases
including bare-legacy + prefixed-future in one body, param-in-string,
token boundaries, and a byte-identical Excel->human->Excel round trip.

## The library pattern (demo-proven, interactive_test.xlsx)

1. A hand-authored lambda library YAML (named_lambdas.yaml): metadata +
   the five standard sections; lambdas in human syntax.
2. manage_named_objects import_all after export.
3. Cell formulas call by name. Export_View A2 in the demo:
       =fnBlankSafe(FILTER(rng_vms_all,rng_vms_saletype="Export",...))
   replacing the double-FILTER IF wrapper - single evaluation, cleaner
   cell, all through supported paths.

The production recipe still carries the (working) double-FILTER form;
switching it to the library pattern is a two-splice change awaiting
the user's call.
