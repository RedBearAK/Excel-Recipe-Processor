# HANDOFF: 2026-08-14 - end of the dynamic-arrays / standardization thread

Orientation layer for the next session. Companion detail:
NOTES_dynamic_array_declaration.md (pass-by-pass) and
SURVEY_2026-08-13_processor_conventions.md (convention audit + doctrines).

## Session-start ritual

Fresh clone of dev_beta from
https://github.com/RedBearAK/Excel-Recipe-Processor.git - the user pushed
this thread's changes, and the TGZ (erp_framework_current.tgz, extracts at
repo root) is the same superset. Recipes live in the user's Dropbox
projects; current copies of vms_process.yaml and cma_invoices_process.yaml
were delivered in-conversation. Sandbox pattern that worked: clone at
/tmp/sync, pristine clone at /tmp/pristine for baseline-failure parity
checks (~20 modules fail on pristine; do NOT chase them - triage is in the
survey doc).

NOTE: the previous thread had client-side rendering failures (whole turns
and file cards invisible). If cards fail: previously-rendered cards point
at the same output paths and download current bytes.

## State: verified working in production

Full 60-step VMS run clean end-to-end AFTER the sheet-addressing sweep:
dynamic-array declarations (no @ anywhere - Excel-verified), all seven
formula columns recipe-injected (donor retired), conditional_format
row-highlight live-verified, tab color families, No_Price summary (8 cols,
3 exclusions), named ranges, CMA lookup file export. The CMA recipe
validates but has not had a post-sweep production run yet - expect it
clean, but eyeball the first one.

## Vocabularies now canonical (all guided-error enforced)

- Stage keys: source_stage / save_to_stage / lookup_stage for flow
  (copy_stage + create_stage included); stage_name ONLY for declarations
  and rule/sub-config references. Bare 'stage' is gone.
- Sheet keys: sheet_name (address one existing tab - a real name or the
  ?sheet_001? index pseudo-name, case-insensitive, 1-4 digits, delimiters
  from Excel's forbidden charset so collision is impossible);
  sheet_names (address several; plain string lists, names/tokens only);
  sheets_to_create (export's entry list - dicts with sheet_name +
  data_source; sibling of columns_to_keep). Bare 'sheet', 'sheets',
  polymorphic name-or-index acceptance, and file_reader's isdigit hack
  are all gone. One recognizer: _helpers/sheet_addressing.py.
- manage_named_objects: yaml_file (was import_file/export_file); range
  definitions use sheet_name and accept tokens.
- Numbers as sheet refs: treated as NAMES with a warning naming the token.

TWO PRODUCTION INCIDENTS from the sweep, both fixed + guarded, lessons in
the notes file: (1) sub-config vocabularies (range entries etc.) need
explicit enumeration in any key sweep; (2) scope renames by the KEY'S
OWNER, never by spelling - export silently degraded to a one-tab 'Data'
workbook when a blanket rename crossed sheets/sheet_names. Export now
refuses both retired spellings loudly.

## NEXT UP (user's chosen priority): data_validation processor

Chosen over import_files ("file globbing handled OK by separate tooling").
Motivating pattern, hand-built by the user with the files' end user:
a cell with a dropdown of SORT(UNIQUE(...)) customer names, driving a
GROUPBY formula on row 3 that re-pivots live on selection. With
inject_formulas + dynamic declaration + manage_named_objects already in
place, the dropdown is the ONLY missing primitive - adding it makes
recipe-authored interactive mini-reports (formulas compute, dropdown
selects, GROUPBY re-pivots) a capability class.

PROBE RESULTS (2026-08-14, /tmp probe - rerun trivially): openpyxl
DataValidation stores all three list sources verbatim and they survive
the load/save session round trip:
  - inline:      formula1='"Open,Closed"'
  - named range: formula1='=rng_customers'   (pairs with lookup tabs)
  - spill ref:   formula1='=$Z$2#'           (# survives storage + round trip)
OPEN Excel-side question (same class as the CF _xlfn one): whether Excel
accepts the literal '#' form in a stored DV or wants ANCHORARRAY - one
eyeball on the first real output settles it; if it fails, the fix lives in
the same zip-rewrite layer as the dynamic-array declaration.

Design direction discussed, not finalized: session-aware FileOps
processor (like conditional_format), sheet_name token-capable via the
shared recognizer, v1 = list type with the three sources, allow_blank /
show_dropdown / input+error prompts; whole/decimal/date types later.
Discuss config shape before building - house rule.

## Queue after that

1. Wire verify_data into the CMA recipe: SHIP REF + BL Ref not_empty
   severity:halt on the enriched stage; Carrier in_stage vs the carriers
   lookup on the VMS side. Retires the oldest horizon item.
2. CMA remainder: drift alarm via diff_data vs previous output; curated
   payables filter (groups containing Open/Overdue; six-credit exclusion
   stays human-managed).
3. Test-debt cleanup thread (triage done, in the survey doc; note
   test_usage_examples is likely REPAIRABLE - its "missing" count was
   about methods, since fixed by the base-class default).
4. Typing-module removal pass (16 modules; the survey's last open item).
5. Back burner: merge/split-per-group (Simple-Excel-Merge prior art),
   page_setup, tab-rename op (doctrine's lazy escape hatch),
   conditional_format extras (clear_existing needs its own careful
   design), per-processor semantic directive counts, squid CA-vs-AK
   Product Origin question.

## Sharp edges (re-read before touching related code)

- pandas 3 CMA SHIP REF coalesce: positive contains "" test, NOT
  not_contains "nan" - dated do-not-simplify comment in the recipe.
- core/dynamic_array_metadata.py XLDAPR bytes are verbatim Excel output -
  never reformat.
- WorkbookSession.flush_all() must not full-reset() (mode flags survive).
- inject_formulas' _store_formula is the ONE live-write funnel;
  provenance-based dynamic declaration depends on it.
- verify_data borrows filter_data._apply_filter via a shim instance -
  deliberate live reuse.
- Country lookup ~43% match on real data is EXPECTED (blank default for
  domestic rows) - not a defect.
- openpyxl is orders slower than pandas for bulk work: formula injection,
  header reads, and zip surgery only.

# End of file #
