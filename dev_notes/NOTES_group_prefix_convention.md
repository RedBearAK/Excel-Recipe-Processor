# NOTES: group-prefix-plus-underscore naming convention (2026-08-14)

HOUSE RULE (user-set): grouping prefixes on named objects are ALWAYS
followed by an underscore - rng_, fn_, tbl_, fml_ - so the functional
part of the name reads cleanly after the group tag, and each group
clusters in Excel's autocomplete popups. fnBlankSafe was renamed
fn_blank_safe accordingly (library YAML, VMS recipe, demo).

ENFORCED, not just documented: HOUSE_GROUP_PREFIXES in
defined_name_validator.py, checked at the 'house' name_validation
level (the default for recipe-authored names). The trigger is the
REAL mistake pattern only - a group prefix continued in camelCase
(fnBlankSafe, rngCustomers, tblOrders) draws a guided error naming
the corrected form; ordinary words that merely begin with the same
letters (fnord_thing, final_total) pass, because a lowercase
continuation is not a group name. Extend HOUSE_GROUP_PREFIXES when a
new object family earns a prefix.

Convention test added to test_named_lambda_translation.py (3/3).
Regenerated demo verified: fn_blank_safe defined and called,
whole-file grammar audit CLEAN.

Side observation from the user, recorded: the Name Manager showed
{...} for the named lambda instead of #VALUE! once the workbook had
a live call in it - plausibly the calc engine holding an evaluated
lambda/array value rather than an uncalled-lambda error. Cosmetic
either way; neither display affects storage.
