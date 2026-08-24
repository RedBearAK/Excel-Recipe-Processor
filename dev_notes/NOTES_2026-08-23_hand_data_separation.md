# Hand-data separation: pets, cattle, tripwire, escrow (2026-08-23)

The clean contracts base was simultaneously a machine-replaced artifact
and the sole home of irreplaceable hand data - two rotating backups
away from permanent loss. Restructured:

- **contracts_hand_enrichment.xlsx** (pets): Order ID + Deposit +
  Deposit App + DDI Manual Override, only rows carrying values. The
  human types HERE and only here. The refresh recipe READS it
  (fail-loud when absent; it ships seeded) and never writes it - the
  clobbering vector is removed, not mitigated. The red/orange columns
  on the base and Contracts tab are projections overwritten every run.
- **clean_contracts_base.xlsx** (cattle): fully regenerable from the
  download plus the lookups; its backups are convenience only.
- **contracts_hand_enrichment_escrow.xlsx**: machine-written snapshot
  of the hand data each run ACCEPTED, 8 rotating backups. Doubles as
  the tripwire reference and the recovery source.
- **Tripwire**: hand file diffed against the escrow; any DELETED row
  (a truncated save, a wrong file, a bad sync) HALTS the run before
  anything is written or rotated, naming the lost Order IDs. Value
  edits - including clearing a cell - pass; only vanished ROWS trip.
  Intentional removals: delete the escrow once and rerun.

## The seed (from sales_orders.xlsx, one-time)

23 Deposit + 13 Deposit App values keyed from Clean_Contracts_Base,
plus 20 DDI Manual Override rows = every contract where the human's
authoritative DDI record disagrees with the cascade over the raw
notes (Prepaid x9, the EOI false-positive corrections x3, the seven
typed categories, one near-miss text case). 41 rows total.

PROVEN IDENTITY: cascade(raw notes) overridden by this seed reproduces
the human's DDI record with 0 mismatches across all 1,441 contracts.
The earlier "57 judgments" estimate shrank to 20 because most injected
clauses are recoverable from the raw notes by the cascade itself -
only information NOT derivable from notes needs an override.

NOTE: the override column is seeded but still UNCONSUMED - the
override-beats-cascade wiring into DDI in Notes is the next iteration.

## Framework: column_formats background_color

Data-cell fill for named columns (per-cell; refuses whole_column,
where a column-dimension fill would tint a million empty rows). Built
for the very-light-orange (FCE4D6) tint marking hand-maintained
columns as more tenuous than lookups.

## Sandbox verification

Run 1 (no escrow): fail-safe empty escrow, 41 hand rows NEW, escrow
written. Run 2: idempotent, no differences anywhere. Run 3 (two rows
deleted from the hand file): HALT at the tripwire naming both lost
IDs; base and escrow untouched (mtimes unchanged). Run 4 (escrow
deleted = documented escape): passes, escrow rebuilt from the slimmer
file, base diff shows exactly the one contract whose override
vanished. Orange tint + red treatment verified on real cells.

# End of file #
