# What the 2344 file shows

You were right to insist on the real artifact. Examining a local reproduction
instead of your output was the wrong method, and it sent me chasing Excel
behaviour that was never the problem.

## The array fill-down fix worked

| Column | rows 2,3,4,5,6,7,8,500,8000,8697 |
|---|---|
| Test Fresh | F F F . . F F F F F |
| Test Cans | F F F . . F F F F F |
| Test Dest | F F F . . F F F F F |
| Test Carrier | F F F . . F F F F F |
| **SALE TYPE1** | **F F F . . F F F F F** |

`SALE TYPE1` now carries a formula from row 7 to 8697. In the earlier 0446 file
it was `<c r="BA7" s="5" t="n"></c>` — an empty cell with no formula at all,
which is the pre-fix behaviour.

Your `Test Fresh` correction came through too:
`IFERROR(SEARCH("Fresh",R7),0)` — R, not S.

## The rows 5-6 gap was mine, and is now self-correcting

`row_count: 5` asked for donor rows 2-6; the donor holds 3. Rows 5 and 6 were
never written, and `fill_down` started at `start_row + row_count - 1 + 1` = 7,
stepping straight over them.

Fill now starts from **the last row that actually received a formula**, found per
column, rather than the last row that was requested. A donor with fewer rows than
`row_count` no longer leaves a gap:

```
row_count=5 against a 3-row donor:
  row 2: F F F F F      row 6: F F F F F
  row 5: F F F F F      row 7: F F F F F
```

The `⚠️ Found 10 empty source cells` warning still fires, which is correct — the
mismatch is worth knowing about even though it no longer causes damage.

Setting `row_count: 3` to match the donor silences it and is still worth doing.

## No corruption in the file

Every check passes:

- zip integrity OK
- every XML part parses
- no duplicate cell refs, no out-of-order cells
- 7 sheets declared, 14 defined names (13 ours plus `_xlnm._FilterDatabase`
  from the auto-filter, which is normal)

Two things looked suspicious and are not. Both were verified against a minimal
openpyxl file:

| Construct | Count | Verdict |
|---|---|---|
| `<v></v>` empty cached value | 43,470 | what openpyxl writes for **every** formula cell |
| `<c s="5" t="n"></c>` | 10,104 | what openpyxl writes for a **formatted blank** cell |

The 10,104 are in exactly the 15 columns `format_excel` touches — the number
format and alignment rules walk every data row, which materialises a styled cell
even where the value is blank. Expected, not damage.

## The Excel warning

Nothing in the file accounts for it. "Autorecovery disabled" usually means Excel
could not create its recovery file, which is common for a file opened directly
from a synced folder — and this one is in Dropbox. A 2.7 MB workbook with 43,470
formulas recalculating on load is also a plausible moment for Dropbox to be
touching the file at the same time.

Worth ruling out by copying the output somewhere local and opening it there. If
the warning follows the file, it is the file; if it does not, it was the
location.

# End of file #
