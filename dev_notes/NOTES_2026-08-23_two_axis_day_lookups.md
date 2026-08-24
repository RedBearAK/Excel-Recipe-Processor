# Two-axis day lookups: Days_to_Add and Allowance_Days (2026-08-23)

The cashflow formula proved the two numbers only ever appear SUMMED
(base + days_to_add + allowance in every path), which is exactly why
the end user drifted into filling Allowance as another Days_to_Add -
misplacing a value changes no output, so the boundary erodes with zero
feedback. The distinction is maintenance semantics, and the data holds
the axes apart in both directions: one DDI (Email of Invoice) coexists
with NET 5/10/14/15/30, and Allowance varies across DDIs sharing a
terms family. Two single-sheet files, one per axis (house rule: no
multi-sheet lookups):

- **due_date_indicators.xlsx** (DDI axis, 20 rows):
  Due_Date_Indicator, Relevant_Date (now PURE field names),
  Date_Offset_Days (the "+7"/"+10" split out of the old strings),
  Allowance_Days (from the user's pivot: default 4, DP Payment 14,
  Receipt of originals 10, manual categories and Prepaid 0),
  Match_in_Notes. The four manual-override categories (Definite Date,
  Internal, Not Applicable, Special) are rows for JOINING only -
  Match_in_Notes: no keeps their ordinary-English words out of the
  Notes substring scan, where "internal" would false-match real text.
  OPEN QUESTION carried from the pivot: Definite Date -> Routing Port
  ETA looks like a pivot artifact, kept verbatim pending a ruling.
- **payment_terms.xlsx** (terms axis, 13 rows): Payment_Terms_List
  (stripped of source padding) -> Days_to_Add. CASH AGAINST DOCS is
  present with a DELIBERATELY blank day value: the 5 live contracts on
  those terms show a blank Days to Add until a human rules the number,
  which beats a guessed financial figure.

## Clean base changes

Three new derived red columns via the joins: DDI Date Offset Days,
DDI Allowance Days, Days to Add. The empty "Add Days" scaffold is
RETIRED - it was the same concept as Days_to_Add, now concrete; hand
columns are down to Deposit, Deposit App, DDI Manual Override. The
DDI exact join reads the FULL decode, so future manual-override values
resolve to dates/allowances with no further plumbing.

Verified: all three joins 0 mismatches vs independent maps across
1,467 contracts; only the 5 CASH AGAINST DOCS rows blank; seeded hand
values survived the schema transition; idempotent after the expected
one-time CHANGED wave (schema growth + "+7" strings going pure).

Also fixed: select_columns now logs only the blank columns it ACTUALLY
created, not the whole columns_to_create list (existing columns were
always kept untouched; the log just said otherwise).

## Cashflow formula deconstruction (limited notes, by request)

The per-path buffers (+5/+7/+30/+45/+105) are properties of WHICH
EVIDENCE substituted for the missing date - they belong in the future
formula/LAMBDA layer, not in lookups. The complete day model:
critical-date base + Days_to_Add(terms) + Allowance_Days(DDI) +
Date_Offset_Days(DDI) + per-contract override (future) + path buffer.
The formula is planned to decompose into stored LET/LAMBDA named
objects so thousands of cells stop carrying a compacted copy (the
original longer-named version hit Excel's 8KB per-cell formula
ceiling). Exact decomposition undecided. Vocabulary drift to reconcile
first: the formula's "Critical Date" list and the DDI "Relevant_Date"
list are sibling generations, overlapping but not equal (formula-only:
End of Season, Check SO, Title Transfer Date, ETD, ETA Klaipeda;
DDI-only: Ship Date, Release Date, Paid Date).

# End of file #
