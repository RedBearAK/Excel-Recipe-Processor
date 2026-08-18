# Inline-string consolidation: matching Excel's storage dialect at save

dev_notes/NOTES_2026-08-17_inline_string_consolidation.md

Sequel to the strip-processor size investigation. The 3.9 vs 3.46 MB
authored-vs-stripped riddle resolved to serialization DIALECT:
openpyxl 3.1+ writes every literal string INLINE
(t="inlineStr"><is><t>text</t></is>) - a deliberate write-speed
trade - while Excel stores each unique string ONCE in
xl/sharedStrings.xml with tiny index references. On production data
the difference is enormous: "SALMON", customer names and destinations
repeat thousands of times, and identical content measured 35.6 MB of
raw sheet XML in openpyxl's dialect vs 26.2 MB in Excel's. The
diagnosis was made by ROUND-TRIPPING the user's Excel-lineage file
through openpyxl, which re-serialized to 3,852,381 bytes - reproducing
the authored 3.9 MB to within rounding and giving the verbatim
comparison an upload could not.

## The fix (ruling: intrinsic, not optional)

core/inline_string_consolidation.py rewrites the will-be-written bytes
at EVERY session save: harvest each plain inline string, dedupe into
the shared table, rewrite cells to t="s" index references, register
the part in [Content_Types].xml and the workbook rels. Wired into
WorkbookSession._save_workbook alongside the dynamic-array
declaration in the same serialize-to-memory pipeline: consolidate
always, declare conditionally on the consolidated bytes, write
atomically. Both writers (consolidation and the declaration's
_write_all_members) now pack at compresslevel 9 - the save-side twin
of the strip processor's packing.

Production-scale proof (the round-tripped 3.85 MB artifact): 406,905
inline cells -> 33,371 unique strings, 3,852,381 -> 3,201,905 bytes -
authored output now lands AT the Excel-lineage size, ~18% smaller
than before, in 5.3s sandbox CPU (one-time, at the single end-of-run
save). Reload identity verified across literals and formula texts;
storage drill audit CLEAN on consolidated output; idempotent second
pass returns identical bytes.

## Safety doctrine

- Only single-plain-<t> inline bodies are consolidated; rich-text
  runs or anything unrecognized stay inline byte-untouched, counted
  in the log.
- The <t> element transfers VERBATIM into <si>: escaping and
  xml:space="preserve" survive byte-exactly (pinned in tests with
  &<>-bearing and space-padded strings).
- An existing sharedStrings table (Excel-lineage files under a
  session) merges with entry ORDER AND INDICES preserved, so
  pre-existing t="s" references stay valid; new uniques append.
- No inline strings anywhere -> the original bytes return unchanged.

tests/test_inline_string_consolidation.py 5/5; eleven-suite collateral
ring green including all storage/grammar suites and the examples
checkers.

## The companion-file family (recorded for the question asked)

sharedStrings.xml is the third companion part ERP now speaks, and the
family map is worth having in one place: xl/sharedStrings.xml (string
table - this feature); xl/metadata.xml + cm/vm cell attributes
(dynamic-array declarations - the anti-@ machinery); xl/calcChain.xml
(recalc-order index - disposable, deleted by the strip processor);
xl/styles.xml (ALL formatting indirection - every s= attribute);
xl/theme/theme1.xml (the palette - already patched by
apply_base_theme); plus the meta-companions that bind them:
[Content_Types].xml and the _rels/ graph, which every new part must
be registered in or Excel repairs the file. Further afield and so far
untouched: externalLinks/ (cached link values - the strip processor's
one refusal class), tables/tableN.xml, docProps/, and
xl/vbaProject.bin in .xlsm. The pattern to remember: in OOXML, a
worksheet is mostly INDICES - the actual strings, styles, themes,
declarations and calc order all live in companion parts, which is
exactly why cell-level surgery keeps working: the sheets are the
skeleton, the companions are the flesh.

# End of file #
