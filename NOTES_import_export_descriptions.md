# import_file and export_file: real capability descriptions

Expand at the repo root. Two processor files.

Neither processor had get_capabilities() at all - the listing's fallback
invented "Import File processor" / "Export File processor" from the class
names. Both now declare themselves like the other 28, with descriptions in
the listing's imperative style and details verified against the code:

  import_file  Import Excel, CSV, or TSV files into stages, with sheet
               selection and variable-substituted paths
               (formats per FileReader: xlsx, xls, xlsm, xlsb, csv, tsv,
               txt-as-tsv)

  export_file  Export stages to Excel or CSV, including multi-sheet
               workbooks, backing up any file being replaced

Also in these files if not yet applied: nothing else - both changes are
purely additive get_capabilities methods.

Full suite: 20 failures, baseline. Discovery: 30/30 clean.

# End of file #
