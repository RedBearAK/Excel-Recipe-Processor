# inject_formulas: translation, fill-down, named columns, session

Expand at the repo root. WHOLE FILES - replace, don't merge. This archive
REPLACES erp_formula_substitution_and_seed.tgz (both of those fixes are
included here).

## Four changes to inject_formulas

1. RANGE INJECTION NOW TRANSLATES REFERENCES. It previously wrote the
   identical formula text into every cell of a range, so '=B2*C2' applied to
   D2:D6 left every row computing ROW 2's numbers. Now uses openpyxl's
   Translator, exactly as seed_donor_formulas' fill-down does. The existing
   test asserted the old behaviour and has been corrected - it was encoding
   the bug.

2. fill_down: true on a cell target continues the formula to the sheet's
   last data row, so the fill follows however many rows a run produced:

       formulas:
         - cell: "AV2"
           fill_down: true
           formula: "=..."

3. {col:Header Name} PLACEHOLDERS resolve to that column's letter from the
   sheet's header row, in the formula AND in the cell/range target. Excel
   formulas address columns by letter, which is exactly the fragility that
   just broke the VMS donor: inserting one column shifts every letter after
   it and formulas silently read their neighbour's data. Naming the column
   costs nothing and cannot drift.

4. SESSION-AWARE. It loaded and saved the file directly, so under the export
   bridge (where the output lives in memory until run end) it failed with
   "Target file not found". Now uses WorkbookSession like the other file-ops
   processors.

5. FUTURE FUNCTIONS GET THEIR STORAGE PREFIX. Functions added after the
   2007 file format - IFS, XLOOKUP, TEXTJOIN, FILTER and friends - must be
   STORED as _xlfn.IFS, _xlfn.XLOOKUP, _xlfn._xlws.FILTER even though Excel
   displays them without the prefix. Writing the plain name produced a cell
   showing #NAME? with the formula rendered as:

       =@IFS(AND(BB2="Export", ...))

   Recipes now write ordinary Excel syntax and the processor adds the
   prefixes from a map in _helpers/inject_formulas_functions.py. Names not
   in the map are untouched, and an already-prefixed name is left alone, so
   re-running is safe. If a formula ever comes back #NAME? with a function
   not listed, add it to the map - it is deliberately extensible.

6. array_formula: true STORES THE CELL AS A SINGLE-CELL ARRAY FORMULA
   (<f t="array" ref="AV2">). Excel applies IMPLICIT INTERSECTION to a plain
   formula whose result could be an array - one containing XLOOKUP, say -
   and displays it as =@IFS(...). The array marker tells Excel the result is
   deliberate.

   Worth recording how this was diagnosed, because the obvious theory was
   wrong: seeded donor formulas were assumed to carry array metadata through
   the copy, so moving the formula into the donor "should" have avoided the
   @. Comparing the raw XML of a real output showed the seeded SALE TYPE1
   and the injected World Region stored IDENTICALLY - plain <f>, no array
   marker - so the donor route would have changed nothing. SALE TYPE1
   escapes the @ only because all its branches return scalars; the trigger
   is XLOOKUP, not the injection path.

   What remains missing versus Excel's own output is cm="1" plus an
   xl/metadata.xml part, which openpyxl cannot write. The array marker alone
   is expected to suffice; confirm in Excel.

## Also included (from the superseded archive)

- add_calculated_column: expression formulas substitute column names in ONE
  PASS over an alternation, longest first. A per-column loop could not work
  when one name contains another ("Species" inside "Major Species").
- seed_donor_formulas: named columns resolve to each workbook's OWN
  position, so donor and target layouts may differ.

Full suite: 20 failures, baseline.

# End of file #
