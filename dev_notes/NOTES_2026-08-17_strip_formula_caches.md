# strip_formula_caches: reclaiming user-saved workbooks

dev_notes/NOTES_2026-08-17_strip_formula_caches.md

Sequel to NOTES_2026-08-17_view_concept_scrapped.md. Excel writes the
cached value of every calculated cell on save, unconditionally
(measured: 3.9 MB authored -> 7.7 MB with views / 4.7 MB without,
after open + save with no changes). This processor reverses that on
CLOSED files: cached RESULTS removed, every FORMULA kept, the file
returned to the formulas-without-caches state openpyxl authors
natively, with fullCalcOnLoad set so Excel recalculates on next open.
Close-without-save proved caches are disposable: recalc does not set
the modified flag, so Excel silently discards results it just
computed.

## Prior art (researched before building)

NO mainstream tool does this. Compressors (NXPowerLite, the online
services, the how-to literature) target images, pivot caches, styles
and phantom formatting; where they touch formulas they do the
OPPOSITE (formulas -> values, destroying recalculability). The
formulas-without-caches file state exists everywhere as openpyxl/
XlsxWriter AUTHORING behavior, never as a reclamation tool. Lessons
taken anyway: (1) always back up before destructive surgery (house:
create_backup default true, .stripbak); (2) the pivot-cache
"regenerate on open" precedent legitimizes the approach; (3) Excel
IGNORES fullCalcOnLoad under calcMode="manual" (XlsxWriter #91) - so
manual is normalized to auto; (4) THE data-loss trap: a formula
referencing an EXTERNAL workbook ([1]Sheet!A1) may have its cached
value as the ONLY copy of the data when the linked file is absent
(documented in ClosedXML and the Anthropic xlsx skill) - such cells
are REFUSED and named, never stripped.

## Safety doctrine (the golden path, antagonistically tested)

- Dyads (<f> + <v>): strip <v>, keep <f>. Ordinary, shared master,
  shared slaves, cached errors (t="e" attr dropped with the value).
- Array spills are NOT dyads: members are value-only cells. The
  anchor's <f t="array" ref="..."> declares the extent; members
  inside an IN-SCOPE anchor's ref are deleted, or blanked in place
  (empty <c s=../>) when styled, preserving per-cell formatting.
- REFUSED and named: external-workbook formulas; formula+inline-string
  cells (not a shape the surgery claims to understand).
- NEVER touched: value-only cells outside every array ref - literal
  data. That is the entire safety boundary, and it is decidable from
  the XML alone.
- calcChain.xml deleted with its content-type override and rel;
  metadata.xml and cm/vm declaration machinery untouched on anchors.
- Atomic write (temp file + os.replace); backup first by default.

## Scoping

Optional 'scope': entries with sheet_names (a LIST) and at most ONE
of cells / columns / rows; separate entries combine. No scope = whole
workbook. An anchor in scope strips its whole spill; an anchor out of
scope leaves the spill untouched. Vocabulary fails loud (multi-
restriction, scalar sheet_names, unknown keys all guided).

## Reporting

Per sheet: dyad caches stripped, spill cells removed, spill cells
blanked with style kept; every refusal NAMED by Sheet!Cell (capped
10 per class with a +N more tail); calc-flag changes; calcChain
removal; bytes before/after with percent reclaimed.

## The bug the real-file drill caught

The first name->part mapping encoded ATTRIBUTE ORDER into the
Relationship pattern. openpyxl writes rels attributes in a different
order than the hand-built fixture assumed, so the mapping matched
nothing and the whole surgery silently no-opped on a real
ERP-authored file (+13 bytes of calcPr, nothing stripped). Rewritten
attribute-order-independent: capture elements, pull attributes
separately. Doctrine for every future element pattern: NEVER encode
attribute order.

## Verification

tests/test_strip_formula_caches.py 8/8 against a hostile hand-built
workbook carrying every cell species (literals str/int/float/bool,
ordinary + shared + error-cached formulas, an external-ref cell, a
legacy array with bare and styled members, a value-only decoy outside
every ref, a second sheet for scoping): golden rule, dyads, spills,
refusal naming, scope enforcement, vocabulary guards, calc flags +
chain, backup + openpyxl round-trip. Plus the real-file drill:
hand-cache the storage drill's injected anchors the way Excel would,
strip, byte-verify formulas intact and caches gone, storage grammar
audit CLEAN afterward. Collateral ring green including the
capabilities snapshot (45 processors), recipe validator, and both
example checkers.

## Tech-bin delivery (outside this TGZ, project files)

strip-excel-caches (stub launcher): iterates *.xlsx directly in the
given folder(s), one recipe run per file, skipping ~$ locks and
.stripbak; .xlsm excluded by doctrine (macro workbooks are not for
unattended surgery). strip_excel_caches.yaml (generic one-step
recipe): takes target_file as a required external variable; stub and
recipe live side by side in the tech bin.

## Production-scale performance (added after the first real run)

The first field run sat silent for 30+ seconds: the rewriter visited
all ~745,000 VMS cells with a Python callback each, paying
reference-parsing regex costs on literal cells that could never be
touched. Three fixes, same correctness (byte-identical results
asserted at scale): (1) a fast bail - a cell with no '<f' on a sheet
with no spill spans returns before ANY parsing, removing ~90% of
callback cost; (2) pass 1 (array-anchor collection) gated on a
't="array"' substring check, so spill-free sheets skip the full scan
entirely; (3) cell addresses parsed LAZILY - whole-workbook runs need
no address unless refusing. Benchmark on a synthetic
9,675x77 / 67,725-formula sheet: 4.5s -> 1.74s sandbox CPU. Sheets
over 100k cells now log a "scanning N cells..." line so long runs
show life. Lesson recorded: drills must include a production-SCALE
fixture, not just a production-SHAPED one.

## Field-run refinements (same day)

Backup order: the first field run showed no .stripbak while the
analysis ground on - the backup had been sequenced after analysis,
right before the write. Ruling: the backup is the FIRST action, before
any read; whatever happens after that line, the original bytes exist
twice. The write itself remains atomic (in-memory copy + temp file +
os.replace), so interrupting at ANY point leaves the original file
untouched - a stray .tmp beside it at worst.

Visibility: the run also sat silent at full CPU. Now: a backup line,
a "Reading file (N bytes)" line, and an unconditional per-sheet
"scanning N cell(s)..." line - silence during legitimate work is its
own defect.

## Heartbeat (field ruling, same day)

The second field run pegged one core for minutes after the scanning
line with no further output - unreproducible in the sandbox, where
the meanest Excel-realistic synthetic (44 MB, long escaped formulas,
cm attrs, sparse styled cells) strips in under 3s. Leading suspect
for the field gap: memory pressure (an 8 GB machine holding the
decompressed workbook + working strings alongside Dropbox and Excel
can swap, which presents exactly as one pegged thread crawling).

Ruling: a long-running processor must issue running commentary every
few seconds. The sheet rewrite is now ROW-CHUNKED - cells never span
<row> elements, so splitting on row boundaries is surgery-neutral
(byte-identical output asserted at scale) - with a time-gated
heartbeat every 5s: cells done/total, rate, and an ETA. A completion
line reports total elapsed when work exceeded one heartbeat. Bonus
diagnostic: if any row range ever behaves pathologically, the
heartbeat pinpoints WHERE it stalls, turning an opaque hang into a
bisectable row number.

## The 500x field slowdown: single-cell array refs (root cause)

The heartbeat's first field data showed a UNIFORM ~700 cells/s
(sandbox: ~380,000/s) - not a stall, a constant per-cell cost. The
user's real file reproduced it in the sandbox instantly where every
synthetic had failed to, and cProfile pinned 80% of runtime in
in_spill().

Root cause, a storage-grammar fact worth remembering: on resave,
Excel rewrites every cm-declared dynamic formula as a SINGLE-CELL
array - <f t="array" ref="AV2:AV2"> - one per cell. 67,726 of them on
the production VMS sheet. Pass 1 dutifully built a 67k-entry spill
list, the fast bail disarmed itself (spans present), and every
literal cell linearly scanned all 67k entries: O(anchors x cells),
billions of comparisons.

Fix: a single-cell ref (first == last) has NO member cells and needs
no span entry - skipped in pass 1. The span list then holds only
genuine multi-cell spills (the user's file: exactly one, the
Cust_List pick-list), the fast bail re-arms, and the real workbook
strips in 3.3s wall: 67,728 caches, 3 spill members, calcChain
removed, 29.2% reclaimed, openpyxl round-trip and grammar audit
CLEAN. Also proven: the heartbeat did exactly its diagnostic job -
the uniform rate ruled out stalls and swap before profiling began.

## Standing limits

Reclaims space in STORED copies; the next user save re-caches (that
is Excel, not the tool). Non-Excel cached-value readers see formulas
without values in stripped files - already true of every ERP-authored
file. Volatile-formula workbooks will prompt to save on every
open-close; that is inherent to volatiles.

# End of file #
