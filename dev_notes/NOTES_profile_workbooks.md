# NOTES: profile_workbooks + the split verdicts (2026-08-15)

## Design verdicts this settled (user-ruled)

1. **profile_named_objects SEPARATE from manage_named_objects: yes.**
   Not a duplication of manage's YAML export - a different output
   CURRENCY. manage owns workbook<->YAML round trips and the
   translation machinery; the profile sibling will emit a Name-keyed
   STAGE for in-pipeline consumers (diff_data drift on names would
   have caught the fn_blank_safe repair-deletion as "name vanished
   since last run" instead of a user-reported #NAME?). It shares
   manage's extraction helpers underneath. PLANNED - wants a small
   helper-extraction refactor first.
2. **profile_workbooks justifies itself WITHOUT the names bucket.**
   Sheet-keyed facts are the drift-alarm payload; named objects and
   VBA appear as COUNT/FLAG tripwires only, repeated per row - their
   catalogs are differently-keyed contracts.

## v1 shipped

Sheet-keyed contract (append-only): Workbook, Sheet, Position, State,
Tab_Color, Max_Row, Max_Col, Frozen_Panes, Zoom_Percent, DV_Count,
Named_Object_Count, Has_VBA (zip-level vbaProject.bin check). Plural
workbooks input; v1 reads from DISK - the anchor consumer reads the
previous run's file, which only exists on disk; cached-workbook
profiling is a documented follow-up awaiting a real consumer.

Tests 2/2: facts against a purpose-built workbook (hidden + veryHidden
states, colors, freeze, zoom 125, a DV rule, one defined name), and
the ANCHOR CONSUMER exercised for real - two deliberately-drifted
workbooks profiled and pushed through diff_data (data-in-data-out
API), drift surfacing on both changed sheets. The drift_alarm_example
in the examples file is the recipe-ready three-step shape.

## Family state

profile_files (files on disk), profile_sheets (per-column, stage or
disk), profile_workbooks (per-sheet) - shipped, sorting together.
profile_named_objects - planned, next in family. Wiring an actual
drift-alarm pair of steps into the VMS/CMA recipes stays a queue item
(it needs a previous-output retention convention, which is a recipe
decision, not a framework one).
