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

parquet_to_xlsx.yaml (kept with the other recipes in Tech_scripts/erp_recipes, not in this repo): import_file (parquet_types: text) ->
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


## read_as_text, and the csv recipe (same day)

Kris wanted the csv version too. The csv reader reads as str and then
`_attempt_numeric_conversion` turns any all-numeric column into numbers
- so 01234 was 1234 and an all-digit package-number column was int64
before any recipe step could protect them. `import_file: read_as_text:
true` skips that conversion (csv / tsv; xlsx cells stringified; Parquet
as parquet_types: text). Both generic recipes use it. Tested: default
loses the zeros, read_as_text keeps them with the null intact.

The csv recipe cannot repair an exporter's undoubled inner quotes -
pandas cannot read such a file at all - so its header says: zip it and
use the zip route, whose converter repairs that on the way through.

Launchers, Kris's names: erp-generic-csv-to-xlsx-coerce-types,
erp-generic-parquet-to-xlsx-coerce-types, erp-generic-zip-to-xlsx-
coerce-types. The zip one calls the Parquet one by its command name.


## The default flipped (same day)

Kris: shouldn't text be the default way to read a csv? Yes. A reader
that changes 01234 to 1234 is a silent change to data at the boundary,
which is the thing every rule in these projects forbids. csv and tsv
columns now arrive as text; `infer_numeric: true` is the old behaviour,
asked for; a csv import with neither key logs one line so nothing
changes silently. Breaking, on purpose: the couple of recipes that
import csv and lean on numbers being numbers need `infer_numeric:
true` or an infer_column_types step. The ERP suite had no such
assumption anywhere - 141 of 142 modules, the one only a batch
timeout - which is its own comment on how little the old default was
relied upon.

Two test adjustments on the way, both mine: test_parquet_io's csv ->
Parquet case now says infer_numeric: true (its subject is the Parquet
typing); the registry guard in test_declaration_lambda_and_registry
grepped the whole module for a pop it forbids in the SAVE path, and
flush_paths pops that registry for a file being CLOSED, after its save
- the guard is now scoped to _save_workbook, as its docstring says.
Two capability descriptions trimmed to the 80-character house cap; the
capabilities snapshot refreshed for the new processor.


# End of file #
