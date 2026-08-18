# NOTES: profile_named_objects - the family's fourth member (2026-08-15)

## The split, executed as ruled

manage_named_objects and profile_named_objects divide by OUTPUT
CURRENCY: manage owns workbook<->YAML library round trips and the
storage-translation machinery; profile emits a Name-keyed STAGE for
in-pipeline consumers. NOT duplication - one truth underneath: the
classification and human-translation logic moved to
_helpers/named_objects_extraction.py (detect_object_type,
translate_lambda_to_human, clean_formula_for_display), and manage's
methods became thin delegates. Regression proof: the translation test
module passes unchanged through the delegates.

## Contract (Name-keyed, append-only)

Workbook, Name, Object_Type (lambda/formula/range/constant/table),
Scope ('global' or owning sheet), Hidden, Definition (STORED text
VERBATIM - the byte truth, right for drift), Human_Definition
(lambdas translated, prefixes stripped), Parameters. One row per
defined name plus one per worksheet table.

## Two real defects the tests caught before shipping

1. openpyxl's TableList.items() yields (name, REF STRING), not Table
   objects - the first draft dereferenced .ref off a string.
2. openpyxl 3.x keeps SHEET-SCOPED names on the worksheet's own
   defined_names collection, not the workbook's - the first draft was
   blind to them. Both collections are walked now; the facts test
   pins a sheet-scoped name explicitly.

## The anchor consumer, replayed as a test

test_name_drift_alarm rebuilds the 2026-08-14 incident: a copy of the
workbook with the stored lambda DELETED (exactly what Excel's repair
did to fn_blank_safe) is profiled and diffed against the original via
diff_data on Name - the vanished name surfaces as a diff row. The
incident class is now detectable pre-eyeball; the recipe-ready
three-step shape is name_drift_alarm_example in the examples file.

## Family complete (as planned)

profile_files, profile_named_objects, profile_sheets,
profile_workbooks - sorting together in the capabilities listing, all
descriptions within the 80-char cap, every member carrying the same
principles: plural inputs, identity-keyed stage out, no apply
siblings, append-only contracts. Recipe wiring of the two drift
alarms (workbook-shape and names) remains the queue item awaiting the
previous-output retention convention.
