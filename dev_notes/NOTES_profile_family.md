# NOTES: the profile_* family + width inheritance (2026-08-15)

Design converged over four exchanges; the settled principles first,
because they will govern future family members:

1. A STAGE IS ONE SHEET (one DataFrame - read, to-be-written, or
   mid-operation). Sheet-shaped facts and workbook-shaped facts are
   DIFFERENT SUBJECTS with different natural keys (Column-keyed vs
   Sheet-keyed) - that is the processor split. Memory-vs-disk is NOT a
   split: the framework already hides that seam (workbook cache,
   target_file), so profiling entries just point at source_stage OR
   input_file and land in one output, identified by the Source column.
2. FAMILY NAMING sorts together: profile_files, profile_sheets, and
   the planned profile_workbooks. file_metadata RENAMED to
   profile_files (breaking change, house preference) - it already had
   the family shape: plural inputs, one identity-keyed output stage,
   consumed as ordinary data (the Sources tab is literally its export).
3. NO "APPLY" SIBLINGS. Profiles are ordinary stages; consumers are
   existing processors with directives reading the OUTPUT CONTRACT by
   column name. Contract growth rule: future facts APPEND columns,
   never rename.

## profile_sheets v1

Facts per (Source, Column): Position, Width, Dtype, Blank_Count,
Distinct_Count, Row_Count. Width uses the SHARED clamp extracted to
_helpers/column_width_scan.py and now imported by format_excel's
auto-fit too - parity is tested, not promised (scan of a frame equals
auto-fit of the same data written plain). Distinct_Count is distinct
NON-BLANK values: the multi-input parity test caught that an empty
string is a value in memory but nothing on disk after an Excel round
trip, so counting it made stage-side and disk-side profiles of the
SAME data disagree - the metric was underspecified, not either
measurement wrong. PLANNED (openpyxl-side): number-format census and
header-style survey of disk sheets, for mirroring seed formatting
onto views the way widths inherit now.

## The consumer: format_excel column_widths_from_stage

Sheet option; applies AFTER auto-fit and BEFORE explicit width rules,
so inheritance beats auto-fit (a spill view's auto-fit only ever sees
headers) while a stated width still wins - the escape hatch stays.
Matches by HEADER NAME, silently skips headers absent from the
profile. Multi-Source profiles need column_widths_source; exactly one
Source needs nothing; guided errors list what is available.

## Recipe effect (vms_process.yaml, 70 steps)

One profile_sheets step (min 10 / max 40, mirroring tpl_sizing) after
the view-frame step; stg_vms_sheet_profile deliberately NOT freed (the
format step consumes it post-export). Exp_View traded tpl_sizing plus
FIVE accumulated width patches (Customer, Product Name, Workflow,
Terms, INCOTERMS - one eyeball-driven fix at a time, all the same
disease) for the single inheritance line. Demo-verified: 77 widths
inherited, explicit SALE TYPE1 rule still wins, audit CLEAN. Every
future view tab reuses the same profile stage for free. The
file_metadata step and stage in the recipe renamed to match the family.

## Deferred

profile_workbooks (Sheet-keyed: position, state, color, extents; plus
a named-objects listing) - its own delivery. audit_stored_grammar()
remains the seed of the separate verify_excel_storage idea;
workbook-side PROFILING and storage VERIFICATION stay distinct tools.
