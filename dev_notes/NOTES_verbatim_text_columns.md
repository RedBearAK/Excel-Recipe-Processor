# import_file: verbatim_text_columns option

Expand at the repo root. Touches core/file_reader.py (also carries the
earlier import_file get_capabilities description if not yet applied) and
import_file_processor.py; adds an example and a test module (4/4).

    verbatim_text_columns: ["Customer Ref #"]

pandas coerces strings like "N/A", "NA", "NULL" to missing values at import,
so a filter list entry "N/A" can never match - the string is gone before any
step runs. Listing a column here keeps its text verbatim while a genuinely
EMPTY cell still imports as missing, so blank and "N/A" remain
distinguishable. All other columns behave exactly as a plain read (verified
value-for-value), and numeric columns keep their dtype.

Works on Excel, CSV, and TSV paths. A misspelled column name warns and
proceeds rather than halting. Note in the option's own comment: this must be
used for ANY column whose literal N/A-like entries a filter needs to see.

Full suite: 20 failures, baseline.

# End of file #
