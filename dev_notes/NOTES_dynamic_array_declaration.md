# Dynamic-array declaration: retiring the implicit-intersection @

Date: 2026-08-13
Scope: new core module + processor, session/pipeline wiring, one found bug

## The problem, finally stated correctly

Excel 365 displays a leading `@` on any STORED formula whose result could be
an array - one containing XLOOKUP, say - unless the file declares the
formula dynamic-array-aware. openpyxl cannot write that declaration, so
every formula this framework injects or seeds opened wearing an `@` the
user never typed.

Three markers were confused along the way; for the record:

- `@` (implicit intersection): Excel's display for a LEGACY-form formula
  that could return an array. Harmless for per-row scalars, but never typed
  by the user. Not stored in the file at all - pure display.
- `t="array"` alone: declares a legacy Ctrl+Shift+Enter array formula.
  Renders with `{braces}`, and edits behave differently under plain Enter.
  A false statement about a per-row scalar formula. (An earlier session
  shipped `array_formula: true` on this theory; reverted, and the
  inject_formulas docstring now carries the correction.)
- `t="array" ref` + `cm="1"` + the XLDAPR `xl/metadata.xml` part: declares
  a MODERN dynamic-array formula. No braces, no `@`. This is what Excel
  itself writes for a hand-typed formula of this shape.

## Evidence

`export_destinations.xlsx` (Excel-authored) stores two formulas differently
in the same sheet:

- `G2` `=XLOOKUP("China", ...)` - literal lookup value, provably scalar:
  plain `<f>`, no markers.
- `H2` `=IFS(... XLOOKUP(AZ2, ...) ...)` - same shape as the recipe's
  World Region formula: `cm="1"` + `<f t="array" ref="H2">` + metadata.

The `xl/metadata.xml` Excel wrote there is BYTE-IDENTICAL to what
xlsxwriter emits for `write_dynamic_array_formula()` - two independent
confirmations of the target bytes. The part is embedded verbatim in
`core/dynamic_array_metadata.py` as `EXCEL_METADATA_XML`; do not reformat.

## What was built

`core/dynamic_array_metadata.py` - shared guts. Reads a finished xlsx
(path or in-memory buffer), marks qualifying formula cells, adds the
metadata part and its `[Content_Types].xml` / workbook-rels registrations,
writes via a sibling temp file then move. Untouched members are copied
byte-identical; the sheet rewrite is targeted regex surgery on the matched
`<c>` elements only (patterns in `core/dynamic_array_metadata_rgx.py`),
precisely so nothing else in the sheet XML can change.

Two entry points over the same guts:

1. At-save (recipe-wide): `declare_dynamic_formulas: true` in settings.
   Every session save runs workbook -> BytesIO -> declaration -> disk, so
   the file NEVER exists on disk in the @-prone form.
2. On-disk: the `declare_dynamic_formulas` processor, for files already
   written - a flushed output, or a donor carrying inherited `{CSE}` cells,
   which the pass "completes" by adding the missing `cm` half. Refuses to
   run on a path the session still holds in memory.

## The safety line (why the default vocabulary is what it is)

Only formulas containing functions that POSTDATE dynamic arrays are marked
(XLOOKUP, XMATCH, LET, LAMBDA, FILTER, SORT, UNIQUE, ... - see
`DYNAMIC_ERA_FUNCTIONS`). Such a formula was necessarily authored in
dynamic-array-aware Excel, so the declaration states a fact and cannot
change results.

Pre-dynamic-array array-capable functions - INDEX, OFFSET, INDIRECT - are
DELIBERATELY excluded: a legacy formula like `=INDEX(B2:C5,,F1)` relies on
implicit intersection to yield one value, and declaring it dynamic would
make it SPILL - a silent change of computed values. If such a formula shows
an `@`, that `@` is potentially meaningful; leave it. Callers who know a
file's formulas are recipe-authored can extend per call
(`extra_functions:` on the processor).

## Found bug (pre-existing, fixed here)

`WorkbookSession.flush_all()` ended with `cls.reset()`, which also cleared
`_deferred`. Consequence: after any mid-run `flush_workbooks` step, the
session silently dropped to save-per-step mode for the rest of the run -
the batching the session exists for, quietly lost, with no symptom except
runtime. Flush now empties the caches but preserves the mode flags;
`reset()` still clears everything at run start and on the failure path.

## Verification state

- `tests/test_dynamic_array_metadata.py`: 5/5 (marking, byte-identical
  metadata part, scalar formulas untouched byte-for-byte, idempotency +
  openpyxl reload, loud failure on an unrecognized metadata part).
- End-to-end through the real pipeline, both entry points: XLOOKUP cells
  emerge as `<c r="C2" cm="1"><f t="array" ref="C2">`, COUNTIF neighbors
  stay plain, metadata part present.
- Full suite: 20 failures, each confirmed failing identically on a
  pristine dev_beta clone (pre-existing baseline).
- NOT verified: an actual Excel open. That is the one remaining check -
  open the first declared output and confirm no `@` and no `{braces}` on
  World Region / SALE TYPE1 before trusting the pass. Pre-2019 Excel will
  show marked cells as `{CSE}`; that is Microsoft's intended degradation.

## Recipe changes riding along

- `vms_process.yaml`: `declare_dynamic_formulas: true` in settings; the
  World Region "the @ is unavoidable" comment replaced with the retirement
  story and a do-not-re-add warning for `array_formula: true`.
- `cma_invoices_process.yaml`: the `vms_processed` prompt removed; the
  Export_Summary_CMA import now reads the standalone
  `lookup_source_files/export_summary_cma.xlsx` the VMS recipe writes on
  every run (new `vms_summary_lookup` variable, sibling-relative). The
  Fishery-Group-for-Major-Species change in that summary is a non-event
  here: this recipe reads only Booking and Carrier Tracking No.

## Provenance marking (2026-08-13, second pass)

The first real run settled an open question: Excel DOES @-prefix a
plain-stored `_xlfn.IFS` even when every branch returns a scalar (SALE
TYPE1 came up as `=@IFS(...)`). Excel's analysis reacts to the function,
not the branch types - the earlier theory that scalar branches escape the
`@` was wrong, and the earlier sighting of `@` on SALE TYPE1 was real, just
tangled up with the donor's braces at the time.

IFS predates dynamic arrays, so it cannot join the safe-by-construction
vocabulary (a legacy IFS could depend on implicit intersection). The
always-safe automation is PROVENANCE, not analysis: `cm="1"`+XLDAPR means
"authored dynamic-array-aware", and for cells `inject_formulas` itself
wrote, that is true by construction for ANY function - the declaration
makes the cell behave exactly as if the user had typed the formula into
Excel 365, which is the stated purpose of injection. An explicit `@`
written by a recipe author survives marking untouched, so there is no
recipe formula whose meaning the mark can change.

Mechanics: `_store_formula` (now the ONE live-write funnel - the
single-cell and range writers previously bypassed it with direct
`cell.value` writes, an Opus-5-era inconsistency fixed here, along with a
docstring stranded below executable code in `_apply_formula_to_range`)
records each live write; `_inject_formulas` compresses the cells to
`(column, first_row, last_row)` runs and registers them with
`WorkbookSession.register_injected_formulas()`. At save, the session hands
the registry to the declaration pass, which resolves tab names to sheet
part names through `xl/workbook.xml` + its rels (part numbering is not
guaranteed positional) and marks every registered cell that holds a
formula, regardless of vocabulary. Registry entries are consumed per file
at save, and cleared by flush_all/reset.

Scope and limits, stated honestly:

- All seven injected columns now get the declaration, including the
  scalar SEARCH/COUNTIF Test columns. Marking them is not what Excel's
  own parsimony would do, but it is a true statement and behaviorally
  identical in modern Excel; the alternative - deciding which functions
  "need" it - is the fragile analysis this design exists to avoid.
- Formulas from any other source (donors, templates, hand edits) still
  get only the conservative vocabulary. For those, provenance is
  unknowable and the vocabulary is the only claim we can stand behind.
- Dead-mode injections register nothing; `awaken` mode does not register
  its awakened cells (known gap, unused by these recipes - awakened
  formulas qualify only through the vocabulary).
- The report line now shows `(N by injection provenance)`; the log is the
  audit trail for which ground a mark stood on.

The `extra_functions` option and the standalone processor keep their
prior semantics for on-disk files. No recipe change was needed: the
existing `declare_dynamic_formulas: true` setting covers SALE TYPE1
automatically through the registry.



## Documentation currency pass (2026-08-13, third pass)

- `declare_dynamic_formulas_examples.yaml` created (the new processor had
  no examples file; the loader reported file_missing and the examples test
  counted the gap).
- `inject_formulas`: the stale INLINE `get_usage_examples` dict (predating
  the external-YAML convention, silently shadowing the YAML file) replaced
  with the standard loader call; `get_capabilities` rewritten for `{col:}`
  placeholders, `fill_down`, `_xlfn` translation, provenance registration,
  and the array_formula caveat; the examples YAML gained a
  named_columns_fill_down_example and the new formula-object keys.
- `format_excel`: `tab_color` added to capabilities and a
  tab_color_example added to the YAML.
- `seed_donor_formulas` capabilities now name `force_column_names`;
  `import_file` capabilities note the automatic calamine fast path.
- `core/file_reader.py`: the CALAMINE_AVAILABLE detection sat BETWEEN
  import lines (executable code before `import logging`); imports
  reordered per convention, module docstring gained its path line.
## Packaging repair (2026-08-13, fourth pass)

`pip install .` previously produced a package declaring NO dependencies:
setup.py's install_requires was an empty commented stub, python_requires
said ">=3.7" (never true - even pandas 2.0 needs 3.8), and pyproject.toml
had no [project] table. Fixed with pyproject.toml as the metadata home:

- [project] table with dynamic version (from _version.py) and dynamic
  dependencies (from requirements.txt) - the curated files stay the single
  sources of truth and the wheel cannot drift from them. Verified by
  building the wheel: all nine runtime deps present incl. python-calamine,
  Requires-Python >=3.10, console script intact.
- requires-python = ">=3.10": where the verified ecosystem lives (suite on
  3.12+, pandas 3 needs 3.10+); package syntax alone would allow older,
  but that combination is untested and unsupported.
- setup.py reduced to a shim with a warning against re-adding metadata.
  Its stale extras (mypy included) are gone; the dev extra in pyproject
  mirrors requirements-dev.txt minus the "-r" include the setuptools file
  parser cannot read - the two must be updated together (commented at both
  sites... at the pyproject site).
- [tool.mypy] removed: requirements-dev.txt had already removed mypy
  itself with the documented no-typing-module rationale; a config with
  disallow_untyped_defs=true described a project this is not.
- [tool.black] target-version updated py310-py313. NOTE: black and isort
  remain declared dev tools, but RUNNING them would rewrite the codebase
  against project conventions (length-sorted imports, aligned registry
  entries). Left declared pending a deliberate decision.
- _version.py __description__ replaced (was the "A Python package for..."
  placeholder); it now matches the pyproject description.
- requirements-dev.txt needed no change for calamine: it inherits
  requirements.txt via "-r".

## Tool removal (2026-08-13, fifth pass)

black, isort, flake8, sphinx, sphinx-rtd-theme, and pre-commit removed from
requirements-dev.txt and the pyproject dev extra; [tool.black] config
removed. None were ever adopted (no configs or docs builds exist for them),
and the formatters were worse than dead weight: running black or isort
would rewrite the codebase against the house conventions - length-sorted
imports, aligned registry entries. Early-scaffolding suggestions, never
used, now recorded in requirements-dev.txt's Removed section with per-tool
rationale. Dev deps are now pytest + pytest-cov, full stop.

## Recipe element count (2026-08-13, sixth pass)

The end-of-run summary now logs a ballpark "elements" total parsed purely
from the recipe YAML - no processor was touched. Rule: every enumerated
LIST ITEM in a step's config counts as one element at every nesting depth;
dict keys and scalar options count zero; a list-free step floors at 1.
On the current recipes: vms_process 292 elements / 60 steps, cma_invoices
112 / 28. Accepted limits, stated in the docstring: "{list:variable}"
strings count as 1 regardless of expansion, and the number measures what
the author wrote, not runtime work - which is what keeps it stable across
data sizes and therefore comparable between runs. The per-processor
semantic count remains a future item (count_step_elements in
core/recipe_pipeline.py would become its fallback).

## slice_data transpose (2026-08-13, seventh pass)

New slice_type: "transpose" - headers-aware, matching Excel's Paste
Special > Transpose on a labelled table: the label column's values become
the new header row, the old headers become a new first column
(old_headers_column_name, default "Field" since the source has no name for
what its own headers represent). Fails loud on duplicate labels, blank/NaN
labels, and name collisions - each would silently corrupt column
addressing downstream. Mixed-type rows come back object-dtype (inherent to
any transpose); clean_data re-coerces where it matters. 4/4 focused tests
incl. a double-transpose round trip. While in the module: import order
fixed, module docstring path line added, and the typing-module Any
annotation removed per convention.

Queued next per discussion: verify_data (value-level fail-loud rules -
generalizes the CMA unresolved-lookup horizon item), then
conditional_format, then unpivot_data. Merge/split-per-group noted as
back-burner (Simple-Excel-Merge exists as prior art).

## conditional_format processor (2026-08-13, eighth pass)

New processor writing native Excel conditional-formatting rules - live
rules that re-evaluate as the user edits. Canonical ERP condition
vocabulary (filter_data's names plus between/not_between/duplicates/unique
in the same style); Excel-native spellings accepted as aliases with a
warning naming the canonical form, once, at validation.

Formula rules reuse the inject_formulas convention (row-2 authoring,
{col:} placeholders) with two deliberate differences: placeholders resolve
$-LOCKED (row-wise tests must not drift as Excel translates a rule across
its range), and formulas get the _xlfn prefix pass because openpyxl stores
rule formulas verbatim - an unprefixed modern function would make a rule
silently never fire. That prefix claim awaits one Excel eyeball on a rule
that actually uses a modern function; current recipe rules use only
classic functions.

Text/blank/duplicate/unique conditions emit one Excel rule per named
column (their formulas anchor to a range's first cell, and duplicates are
per-column domains by design); comparison conditions share one rule across
a combined multi-range. List order = priority; stop_if_true supported;
color_scale and data_bar included.

Shared plumbing extracted on the way: _helpers/excel_color_support.py now
owns color normalization (format_excel delegates; the never-firing
try/except ImportError around webcolors was not carried forward), and
prefix_future_functions moved into inject_formulas_functions for both
processors. 5/5 focused tests; format_excel suite verified byte-identical
to the pristine baseline after the extraction.

vms_process.yaml gains the proving-ground rule: entire-row highlight where
Contracts is non-empty and Price is empty - the live twin of the
No_Price_Product_Summary tab, deliberately without that tab's exclusions.

## Reshape pair: columns_to_rows / rows_to_columns (2026-08-13, ninth pass)

Two complementary data processors (registry now at 38). columns_to_rows
demotes header columns into label/value rows - a LAYOUT change, never a
summarization, and deliberately not named "unpivot" since the operation
recovers nothing and the old name implied it did. Distinct from
slice_data's transpose, which rotates the grid keeping one dimension in
the headers; this eliminates the header dimension. Safety choice: pandas
melt silently DROPS columns claimed by neither list; the processor halts
naming them instead.

rows_to_columns is the inverse - a pivot WITHOUT aggregation, enforced:
each (id, label) pair must map to at most one value, duplicates halt
naming the offending pairs and pointing at aggregate_data/pivot_table for
deliberate aggregation. New columns keep first-appearance label order
(months stay in arrival order). Explicit id_columns must claim every
remaining column or the step halts - nothing vanishes silently.

5/5 tests, the centerpiece being the round trip: wide -> long -> wide
restores columns, order, and every value exactly.

conditional_format follow-ups deferred by decision: clear_existing (any
"clear existing" semantics need their own careful design), top/bottom-N,
above/below average, icon sets, case-sensitive text, date operands.

## Capabilities listing polish (2026-08-13, tenth pass)

--list-capabilities emitted "Empty formatting list - no sheets will be
formatted" at the top: the lister instantiates every processor from its
minimal config, and format_excel's omitted 'formatting' entirely, drawing
the construction-time warning. The minimal config now carries one sheet
entry - minimal AND quiet. Also shortened the five new processors'
capability descriptions to single terminal lines; the trimmed nuance
already lives in each processor's other capability keys.

## verify_data + verification ledger (2026-08-13, eleventh pass)

Value-level verification, sibling of verify_columns. Rules carry per-rule
severity - warn (default: count + sample logged, run continues) or halt.
The condition vocabulary is filter_data's ENTIRE set, borrowed live: a
shim FilterDataProcessor instance lends _apply_filter, violations are the
index difference from the satisfied rows, and the two processors cannot
drift. in_stage covers the referential case ("every Carrier must exist in
the carriers lookup" - the CMA unresolved-lookup guard is now one rule).
Sources: a stage, or a file sheet (session-held files read live,
pre-save). Loud caveat documented: formula cells in files this framework
wrote have no cached values, so file-mode rules on injected formula
columns see blanks - verify formula inputs in stages.

core/verification_ledger.py accumulates outcomes; recipe_pipeline resets
it at run start and logs the 🔎 summary line at completion. 5/5 focused
tests (one initially failed on a FIXTURE artifact worth remembering:
pandas drops a trailing all-empty row on read_excel, so a single-column
sheet whose last cell is blank reads back one row short).

HANDOFF_2026-08-13_thread_state.md added: orientation layer for the next
session, incl. the test-debt triage and the CMA wiring as next step.

## stage-key harmonization (2026-08-13, twelfth pass)

Real confusion found (and personally demonstrated: a smoke test earlier
this session wrote 'stage' inside an in_stage rule and got the
stage_name error). Three naming families exist: the data-flow trio
(source_stage/save_to_stage/lookup_stage), the stage_name family
(settings declarations + rule/sub-config REFERENCES in filter_data,
aggregate_data, group_data), and a bare 'stage' used only by the verify
pair. Resolution: the verify processors now read 'source_stage' (they
read a stage as their subject - data-flow family), while rule-level
references stay filter_data's 'stage_name' (a different role, a
different key, each consistent with its family).

- verify_data: renamed outright (brand new, nothing deployed); bare
  'stage' at step level errors with the correct key named, and 'stage'/
  'source_stage' inside a *_stage RULE errors pointing at 'stage_name'.
- verify_columns: 'source_stage' canonical; deployed 'stage' still works
  with a warning naming the canonical (the conditional_format alias
  pattern). VMS recipe and both examples files moved to canonical.

Remaining inconsistency, deferred deliberately: copy_stage/create_stage
name their DESTINATION 'stage_name' where the flow family says
save_to_stage (copy_stage's own comment acknowledges it). Deployed
semantics, wider blast radius - a later harmonization decision, not a
drive-by.

## stage-key standardization sweep (2026-08-13, thirteenth pass)

Bare 'stage' eliminated entirely, no aliasing - the user's call: stage_name
exists precisely to disambiguate, backward compat carries no value in a
one-user project. Final key families: the data-flow trio (source_stage /
save_to_stage / lookup_stage) for step-level flow; 'stage_name' ONLY for
declarations (settings stages) and references (filter rules, aggregate/
group source_configs); nothing else.

- verify_columns: the short-lived alias removed; bare 'stage' now errors
  naming source_stage. Its test fixtures moved to canonical.
- copy_stage: REWRITTEN, because the audit found it latently broken at
  runtime - it never set requires_source_stage=False, so the pipeline ran
  it through execute_stage_to_stage, whose unconditional save_output_data
  raised on the missing save_to_stage right after the internal save.
  (requires_save_to_stage only informs recipe-LOAD validation.) Now:
  source_stage in, save_to_stage out, execute passes through, and an
  overridden save_output_data honors description/overwrite. Its examples
  file had a THIRD spelling ('target_stage') the processor never read -
  examples were already broken against their own processor; fixed.
- create_stage: destination renamed to save_to_stage (its
  requires_source_stage=False branch never auto-saves, so the internal
  save simply reads the new key). requires_save_to_stage=False flag
  removed - no longer true.
- Both processors give guided errors on step-level 'stage_name', naming
  save_to_stage and the standardization date.
- recipe_pipeline's --stop-after check reads save_to_stage only.
- CMA recipe's create_stage step and both processors' examples updated
  (settings declarations in those examples correctly KEEP stage_name).
- Both dedicated test modules updated: create was a pure key rename; copy
  tests now compose execute + save_output_data as the pipeline does. Both
  pass, as do verify_columns/verify_data/comprehensive_variable_substitution.

## Convention survey (2026-08-13, fourteenth pass)

Programmatic drift audit across all 39 processors - full findings in
SURVEY_2026-08-13_processor_conventions.md. Fixed: a ~185-line DEAD
DUPLICATE FormatExcelProcessor living inside core/base_processor.py
(no imports anywhere; removed); nine processors erroring on usage
examples for lack of the method (BaseStepProcessor now provides the
external-YAML default, making ~28 boilerplate copies deletable and
probably making test_usage_examples repairable); seed_donor's examples
YAML missing its required description key; and the base get_capabilities
default that was a stale format_excel copy-paste. Surveyed for decisions:
sheet-key drift ('sheet' vs 'sheet_name'), manage_named_objects' config
keys colliding with processor names, and the ~18-module mechanical style
debt (path lines, typing imports, end markers). Full battery green after
the base surgery; format_excel suite still byte-identical to pristine.

## Survey follow-ups (2026-08-13, fifteenth pass)

manage_named_objects: 'import_file' and 'export_file' turned out to name
the SAME thing - the YAML definitions file - from opposite directions,
never both in one operation. Collapsed to one key, 'yaml_file', matching
the processor's own content-type precedent (vba_file); direction follows
the operation. Guided error on the retired keys cites the survey;
examples and the passing-baseline test module updated (its fixtures used
the old keys).

Mechanical pass: docstring path lines and end-of-file markers scripted
into all 17 lacking processor modules (path line after the summary line;
deprecated module skipped); all modules compile, full battery green,
both recipes validate. Survey's remaining open item: typing-module
imports in 16 modules, deferred to its own pass.

## Sheet-addressing sweep executed (2026-08-14, sixteenth pass)

Doctrine v2 implemented end to end. New shared recognizer:
_helpers/sheet_addressing.py + sheet_addressing_rgx.py - ONE
resolve_sheet_ref() for every processor (names; ?sheet_NNN? tokens,
case-insensitive, 1-4 digits, bounds-checked live; numbers warned and
treated as NAMES), plus reject_token_for_creation() for export.

Adopted: import_file (sheet -> sheet_name, default '?sheet_001?', the
file_reader isdigit hack DELETED - a tab literally named "1" is reachable
again, proven in tests); format_excel (entry key sheet -> sheet_name,
active_sheet -> active_sheet_name, local resolver retired, and the
debug-level SILENT SKIP on unresolvable sheets is now a loud halt naming
the tab and the available list - proven by e2e); conditional_format and
verify_data (sheet -> sheet_name, token-capable in both session and disk
paths); inject_formulas (sheets -> sheet_names, entries resolved through
the recognizer; None/'all' semantics kept); export_file (creation sites
reject tokens with the address-vs-create explanation; its structural
'sheets' entry-list key deliberately kept - it lists entry DICTS, not
names, so it was never part of the polymorphism problem).

Recipes: 40 sheet keys renamed across both, active_sheet_name landed,
inject lists renamed, and vms's download_sheet variable became
"?sheet_001?" - the one production index use, now honest. Both validate;
token e2e (import + format + CF + active sheet all by token) green;
sweep caught one refactor slip (dropped sheet_spec assignment) and one
follow-on (validator's known-keys list) - both fixed and covered.

Deferred, recorded: pluralizing conditional_format / verify_data /
format entries to sheet_names broadcast lists (a feature, not a rename -
and the lookup-tab consolidation idea died anyway: those entries differ
per-tab in tab_color); seed_donor/generate_column_config source_sheet/
target_sheet keys left as-is (role-prefixed, name-only, never
polymorphic).

## Real-run fix: named-objects range key + escape warning (2026-08-14)

First production run after the sheet sweep halted at step 58: the
recipe-wide sheet -> sheet_name rename correctly caught the
manage_named_objects RANGE DEFINITIONS, but that processor's sub-config
vocabulary was missed by the sweep. Fixed forward per doctrine: range
entries take 'sheet_name' (guided error on 'sheet'), and the sheet is
resolved through the shared recognizer, so ?sheet_NNN? tokens work in
range definitions too. Examples renamed; named-objects test module still
passing; step-58's exact shape (the recipe's real range list against a
session workbook with the real tab names) verified clean.

Also: the two sheet_addressing helper docstrings contained the forbidden
character list with a backslash, drawing a SyntaxWarning on 3.12+ at
import - both docstrings are now raw strings.

Sweep-audit lesson recorded: sub-config vocabularies (range entries,
formatting entries, rule dicts) each need explicit enumeration in a key
sweep; grepping step-level get_config_value sites alone misses them.

# End of file #
