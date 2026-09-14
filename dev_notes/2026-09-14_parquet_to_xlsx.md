# parquet_to_xlsx, infer_column_types, and structured CLI values

`dev_notes/2026-09-14_parquet_to_xlsx.md`

The last piece of the zip -> Parquet -> xlsx chain (Kris, 2026-09-13):
a generic recipe that takes a Parquet with every column text and writes
a compact xlsx with "best-guessed dtyped columns that make sense in
Excel as soon as the file is opened".

## infer_column_types

Nothing in ERP looked at a column's CONTENT to decide a type; clean_data
coerces a NAMED column. A generic recipe does not know the names, so a
new processor decides per column, conservatively by content and
generously by name, because the failure that matters is a column made
numeric that should not have been (1.00E+06 for a package number, lost
leading zeros on a lot number):

- dates: every value parses in ONE of date_formats (first that fits all)
- decimals: every value numeric and at least one has a point -> float
- integers: ONLY columns named in integer_columns, when every value is a
  whole number -> Int64 (nullable)
- text_columns never touched; an all-empty column stays text
- all or nothing per column: one unparseable value keeps it text; this
  processor never turns a value into NaN. strict makes a named integer
  column that will not parse an error.
Every decision is logged with its reason. Six tests, the near misses
included (a '.000000' zero is a decimal; 'n/a' in a weight column keeps
it text; '01234' keeps its zeros).

## Structured --set / --var values

CLI overrides were always strings, so a launcher could not hand a
recipe a LIST - and the generic recipe's two exporter-specific lists
have to come from the launcher, the only party that knows the exporter.
A value starting with '[' or '{' is now parsed as YAML (JSON is YAML);
anything else, including a path with a bracket inside it and a value
that does not parse, stays the string it was. Tested both ways.

## The recipe

recipes/parquet_to_xlsx.yaml: import_file (parquet_types: text) ->
infer_column_types (lists from var_integer_columns / var_text_columns,
empty by default: nothing becomes an integer unasked) -> export_file ->
format_excel (header, freeze, filter, widths). Output
{output_dir}/{stem}_HHMMSS.xlsx, the VMS recipe's time-stamp rule.
Compact by construction: inline-string consolidation runs on every
session save. The row limit is NOT the recipe's job; the launcher reads
the Parquet footer (no data loaded) and refuses a file Excel cannot
hold, exit 3, which zip-to-xlsx treats as "keep the Parquet beside the
zip - it is the usable form".

Ran end to end on a 20,000-row synthetic IMS-shaped zip: zip ->
Parquet (6,644 inner quotes doubled) -> xlsx with Pack Date and Pack
Datetime as datetimes, Packages Int64, Net Weight float, every
identifier text, the `40" high` note intact.


# End of file #
