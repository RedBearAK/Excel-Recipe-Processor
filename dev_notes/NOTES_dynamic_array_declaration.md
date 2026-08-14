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

# End of file #
