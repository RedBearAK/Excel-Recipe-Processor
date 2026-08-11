# Optional calamine engine for Excel imports

Expand at the repo root. core/file_reader.py is a WHOLE FILE - replace,
don't merge (it also carries verbatim_text_columns and the numeric-string
sheet fix; the copies in erp_verbatim_text_columns.tgz and
erp_verify_columns_and_repeated_dumps.tgz have been refreshed to match).

## Install (each machine, optional)

    pip install python-calamine

Prebuilt PyPI wheels for macOS (arm64 and x86_64) and Linux - pip only, no
brew, no Rust toolchain. WITHOUT the wheel, nothing changes: detection is
a find_spec check at import, and the openpyxl path serves exactly as
before (fallback proven by test).

## What it does

calamine is a Rust spreadsheet READER; pandas has carried it as a
first-class engine since 2.2. Every FileReader Excel import uses it when
present. Value-for-value equivalence proven engine-against-engine across:
dtypes, mixed columns, datetimes (Timestamps, NaT for missing), blank
cells (NaN, never ''), the keep_default_na=False raw path underneath
verbatim_text_columns (literal "N/A" intact), formula cells, and
positional sheet addressing including the numeric-string form. Plus the
full-recipe gate: 13-attribute output snapshot (five data spot checks)
identical before and after.

Measured at 10,500 rows: import 2.6s -> 1.3s, run 12.6s -> 6.5s. On a 51K
merge the ~26s import should land around 5s, putting the whole run near
70 seconds - the end of the optimization arc that started at 4m 6s:

    4m 07s  baseline
    2m 30s  workbook session (one load, one save)
    2m 20s  autofit_scan_rows cap
    1m 28s  export bridge (no disk round-trip)
    ~1m 10s calamine import (projected)

Tests 5/5 (they skip the engine comparisons gracefully where the wheel is
absent, and exercise the fallback instead). Full suite: 20 failures,
baseline.

# End of file #
