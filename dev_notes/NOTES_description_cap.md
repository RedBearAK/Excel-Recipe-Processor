# NOTES: processor description cap - 80 chars, ENFORCED (2026-08-15)

The capabilities listing prints ~26 chars of padded name plus the
description; the 144-char excel_data_validation line that prompted
this wrapped everywhere. HOUSE RULE: get_capabilities()['description']
is AT MOST 80 CHARACTERS, one clause plus at most a parenthetical.
A long description is a description doing a docstring's job - the
detail belongs in the module docstring or the examples file.

Made mechanical, not advisory: tests/test_processor_descriptions.py
sweeps EVERY registered processor (missing/empty also fails), so a
new processor with a long description fails at commit time instead
of waiting for an eyeball. 15 descriptions rewritten to fit (the two
worst were 144 and 116 chars); profile_files also GENERALIZED - the
old text ("...for provenance tabs") described its original use case
rather than what it does; family phrasing now parallels
profile_sheets: "Per-file metadata discovery (e.g., ...)".

All 41 processors within cap on the enforcement sweep.
