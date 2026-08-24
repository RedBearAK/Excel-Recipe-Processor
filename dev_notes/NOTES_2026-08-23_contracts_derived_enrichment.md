# Contracts refresh phase 2: derived enrichment columns (2026-08-23)

Three DERIVED columns now ride the clean contracts base and the VMS
Contracts tab, red-formatted like every recipe-inserted column. All
three are recomputable from the download plus the DDI decode file on
every run; the hand-maintained columns (Deposit, Deposit App, manual
DDI categories like Prepaid / Internal / Special) are a later
iteration that must NOT be confused with these.

## The columns

- **DDI in Notes** - the cascade: any of the 16 decode strings found
  INSIDE Notes wins (covers source-typed "DDI:" / "DDI=" clauses and
  bare phrases alike - the prefix is just more text around the same
  string), first match in decode-file row order; no hit defaults to
  Email of Invoice, the proven house rule (1,130 of 1,186
  hand-injected assignments were exactly this default).
- **DDI Relevant Date** - exact join of the indicator back through the
  decode file, so the Email of Invoice -> Invoice Date pairing lives
  only in due_date_indicators.xlsx, never hardcoded.
- **Dep In Notes** - "Yes" when Notes contains dep/Dep/DEP
  ((?i) inline flag), mirroring the worksheet IFERROR(SEARCH("DEP"..)).

## Framework: lookup_data match_mode

`match_mode: "lookup_value_within_main_text"` is the data-driven form
of XLOOKUP(TRUE, ISNUMBER(SEARCH(range, cell)), range): lookup rows
tried in stage order, first whose key STRING appears (case-insensitive,
regex-escaped) inside the main column's text wins; unmatched rows keep
NA for default_values. Guards: invalid mode named both modes; stray
join_type refused; lookup-column collision with main data refused.
Default 'exact_key_equality' is the unchanged historical merge.

Also: the "Low match rates" warning now skips columns that carry a
default - when the recipe declares a fallback, a low raw match rate is
the designed shape, not a data problem.

## Verification (sandbox, real 2026-08-17 download)

- Recipe output vs an independently written pandas cascade: 0
  mismatches on all three columns across 1,467 contracts.
- Recipe vs the human's 1,441 hand assignments: 97.4% agreement
  (1,370/1,407 shared). All 37 disagreements fall into expected
  classes: (a) manual judgments the cascade cannot know (Prepaid x10,
  the 7 hand categories, Release-to-Customer read from context) - the
  later manual-enrichment layer; (b) near-miss note text ("proof/draft"
  without spaces, a truncated "...document"); (c) a few human
  inconsistencies (note literally says "Email copy of documents", was
  hand-filed under the longer category).
- Idempotence: rerun against the just-written base reports
  "no differences" - the derived columns round-trip through the Excel
  baseline cleanly (blank equivalence doing its job).

## Near-miss recovery without code

The decode file is the single source of truth AND user-editable:
adding a variant row like "proof/draft documents" (no spaces) with the
same Relevant_Date recovers the near-miss class with zero code or
recipe changes. Row order in the file is match priority.

# End of file #
