# Hand-column scaffolding: Deposit, Deposit App, Add Days, DDI Manual Override (2026-08-23)

Four empty columns now ride the clean contracts base and the VMS
Contracts tab (red-formatted), PRESERVED by Order ID on every refresh:
the human types into clean_contracts_base.xlsx between runs, the next
refresh carries the values forward, new contracts start blank.
Recomputing them would be silent data loss.

Mechanics: the previous base is schema-normalized (select_columns
columns_to_create builds blanks only where missing, so a pre-scaffold
base transitions without an everything-changed diff wave), then
lookup_data joins the four columns from it onto the fresh build. The
diff references the normalized baseline. Verified round-trip: seeded
0.3 / "$8.65/lb" / "Prepaid" survived a full refresh with mixed dtypes
intact and a quiet diff.

## Why these stay manual (the "iffy" analysis, from real data)

- **Deposit** (23 of 1,441 ever filled): TWO unit systems share one
  column - fractions (0.3 = 30%) and dollar amounts - with nothing
  marking which. The Notes text behind them ranges from amount-free
  ("Dep bLoad, Bal 5d Email Docs") to amount-bearing prose ("Advance
  deposit payment of $50,000 received 5/12") where percent vs USD vs
  per-lb is a judgment. A wrong financial number is silently poisonous;
  a blank is a visible to-do.
- **Deposit App** (13 filled): free-text application mechanics
  ("$8.65/lb", "$20,000/FCL") that exist NOWHERE in the download - the
  only source is a human reading the contract documents.
- **Add Days**: never filled once - zero examples to derive a rule
  from. Presumed per-contract grace-day override of the category-level
  Allowance concept (old Sheet2 col K: 4/10/14/0); until a first real
  use exists, any autofill is invention.
- **DDI Manual Override** (successor to the empty "Due Date Indicator"
  scaffold): the ~37 judgment calls the cascade cannot make - Prepaid
  read out of deposit language (x10), Internal / Special /
  Not Applicable / Definite Date, Release-to-Customer inferred from
  context. Tiny sample, inconsistent human ground truth (a note
  literally reading "Email copy of documents" was hand-filed under the
  longer category), and high stakes: a wrong category silently
  redirects which DATE any due-date logic keys on. The cascade already
  agrees with the human 97.4%; automating the last 2.6% has the worst
  error-to-benefit ratio in the whole file.

Cross-cutting: for the derived columns, correctness was verifiable
row-for-row against ground truth; for these four the only ground truth
IS the human (circular), samples are 0-23 rows, formats are free text,
and the semantics are financial. Blank-and-flagged beats
wrong-and-confident - Dep In Notes = "Yes" with an empty Deposit IS
the worklist.

## Parked wiring

Nothing consumes DDI Manual Override yet. The intended rule, when
built: a non-blank override wins over the cascade's DDI in Notes for
that contract (verified coexistence today: 26-SBS-350A carries
override "Prepaid" beside cascade "Email of Invoice"). The 57 legacy
judgments harvested from sales_orders Sheet1 (2) become the initial
fill of this column in a one-time migration - also parked.

# End of file #
