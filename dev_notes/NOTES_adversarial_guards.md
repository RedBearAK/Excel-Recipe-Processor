# NOTES: adversarial guards for the storage pipeline (2026-08-14)

The oracle corpus pins the happy paths; these modules ATTACK the
pipeline so it cannot stray from the forms that survive Excel. Direct
python3 runs are the authoritative score, per house rule.

## tests/test_xlpm_adversarial.py (3/3)

- OUTPUT INVARIANTS, applied to every case: string literals
  byte-preserved (multiset equality); output parens BALANCED (count
  can legitimately grow - '#' -> ANCHORARRAY adds a pair - imbalance
  cannot); brace/bracket counts preserved; no chained prefixes
  (_xlpm._xl / _xleta._xl / _xlfn._xl except the legitimate
  _xlfn._xlws.); every _xlfn name present in the map (kills the
  blanket-prefixing regression class); every LAMBDA/LET declaration
  slot _xlpm.-prefixed; literal '#' never survives outside strings;
  idempotence under BOTH transforms.
- HOSTILE INPUTS: unbalanced parens, cell-ref-shaped names (A1, R,
  R1C1), boolean-literal names, empty/numeric declarations all refuse
  with guidance; construct keywords inside longer identifiers
  (MYLAMBDA, LETTERS, LAMBDANESS) and construct-free inputs pass
  through byte-identical; an unterminated string INSIDE a construct
  refuses (parens can never close - refusing beats guessing) while a
  construct-free dangling string passes through; duplicate parameters
  transform consistently (Excel rejects them at its own layer).
- SEEDED FUZZ: 300 deterministic (seed 20260814) compositions of
  nested LAMBDA/LET, shadowing, hostile strings ("LAMBDA(x)", "Z1#",
  escaped quotes), sheet-quoted refs, array constants, spill refs,
  legacy+future calls - every survivor holds every invariant, every
  refusal is a ValueError, and any failure prints the formula verbatim
  for promotion to a named regression.

## tests/test_storage_grammar_differential.py (2/2)

- MAP DIFFERENTIAL: all 29 exercisable FUTURE_FUNCTION_PREFIXES
  entries written through xlsxwriter (use_future_functions) and read
  back from stored bytes - EXACT agreement, including the
  _xlfn._xlws. duo, corroborating the whole map against an
  Excel-validated table. Finding: xlsxwriter leaves GROUPBY / PIVOTBY
  / PERCENTOF bare (their table lags 2024 functions). GROUPBY is
  covered by our real-Excel harvest; PIVOTBY and PERCENTOF are
  family-inferred and now sit on the harvest request.
- WHOLE-FILE AUDIT: audit_stored_grammar() sweeps every stored
  surface (cell <f>, DV formula1/formula2, definedName content) for
  the violation classes this project actually shipped and repaired:
  stored leading '=', literal '#', unmapped _xlfn, bare declaration
  slots, chained prefixes. Runs against the demo output and the
  harvest fixture; importable - natural promotion target is a
  verify_excel_storage processor step recipes could run on their own
  outputs.

## Hardening the antagonism forced (xlpm_name_storage.py)

Declared names shaped like cell references (A1-style, bare R/C,
R1C1-style) and the boolean literals are now REFUSED - Excel forbids
them as names, and prefixing occurrences of a name like 'A1' would
also hit real references in scope. ANCHORARRAY joined the prefix map
proper: the spill transform emits it, recipes may now write it
explicitly, and grammar audits recognize it as legitimate.

Full suite this round: 4/4, 3/3, 2/2, 2/2, 3/3, 2/2, 4/4, 6/6 across
the eight session modules, direct runs.
