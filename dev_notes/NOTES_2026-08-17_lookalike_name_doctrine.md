# LET/LAMBDA name doctrine: nothing that reads as a reference

dev_notes/NOTES_2026-08-17_lookalike_name_doctrine.md

SUPERSEDES the same-day first doctrine TGZ (this delivery carries the
terminal-digit refinement throughout; push this one).

Ruling after the g0 incident: thoroughly prevent, defend against, and
GUIDE both humans and LLMs away from coining LET/LAMBDA declared
names that could ever be mistaken for cell references. Excel forbids
the real class outright, and short letter+digit temporaries (g0, x2,
tmp1) are a documented habit of LLMs writing formulas - the
natural-feeling scratch name IS the forbidden shape.

## The rule, as refined same-day

TERMINAL-digit: a digit run at the very END of a name directly after
letters is the ONE position that can complete a cell-reference shape,
so that is what the guard refuses. Letters AFTER digits can never be
a reference - a1b, q3total, col2name are legal (second ruling: the
first cut refused digits-after-letters ANYWHERE, which was
over-strict for exactly that class). Defined OBJECT names keep the
stricter everywhere-separated house rule from the older ruling - the
two name spaces now diverge deliberately, with the divergence stated
in range_patterns beside the two patterns (unseparated_digit_rgx for
object names, terminal_unseparated_digit_rgx for declarations).

Two refusal layers in _validated_declaration, at parse time in EVERY
storing path (library import AND cell injection):

1. Hard-Excel class: A1-style ($-tolerant), R1C1 forms including
   RC / R2C / RC2, bare R / C.
2. Terminal-digit class: total2, ABCD1, a1b2, x2y3 - even shapes
   Excel would technically allow. The message names the digits, shows
   the separated fix built from the name itself ('total_2'), offers
   the letters-after form, and offers word alternatives. The guidance
   IS the point: an LLM tripping the guard receives the correction in
   the error text.

## Comment layer

xlpm_name_storage docstring; vms_named_lambdas.yaml header (delivered
alongside, beside the prefix convention); let_lambda_naming entries in
both inject_formulas and manage_named_objects example files (surface
in both CLI views). All state the terminal rule and the safe
letters-after-digits class explicitly.

## Tests

test_xlpm_adversarial class test: 12 refusals (adds a1b2, x2y3), 11
accepts (adds a1b, q3total, col2name, x2y), every refusal checked for
reference guidance in its message. Full ring green; storage drill
clean with the library's 'raw' intermediate.

## Sorting question (same review, ruled: no change)

Static tab order is pandas' key sort; the views' GROUPBY sorts the
same seven keys and is the closest Excel can get - rows shift only
where key values differ purely in capitalization (Excel sorts
case-insensitively; pandas separates upper from lower). Spills are
read-only; resorting = copy, paste as values, as ruled.

# End of file #
