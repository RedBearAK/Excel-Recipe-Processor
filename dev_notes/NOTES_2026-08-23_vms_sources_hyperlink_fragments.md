# VMS recipe fragments: Sources file links, unhide, EAF3FB swap

Paste-ready fragments for `vms_process.yaml` (2026-08-23 cycle). The
recipe file was not on hand this session, so these are exact fragments
to apply by hand, or upload the current recipe and the edits become a
verified splice with whole-line-unique anchors.

Three edits, in recipe order.

## 1. Sources profile step: emit the Path column

On the `profile_files` step that builds the Sources tab, add one key:

```yaml
    include_full_paths: true
```

The stage gains a `Path` column with the absolute resolved path of
each source file (the DDI decode, the hand enrichment file, the
carrier lookups - every listed input). Default column order becomes
`File, Modified, Size (KB), Path`.

## 2. Sources sheet formatting: unhide and link the Path column

In the `format_excel` step, on the `Sources` sheet entry:

REMOVE this line (the unhide):

```yaml
      sheet_state: "hidden"
```

ADD a column_formats rule (create the `column_formats:` list on the
entry if the templates do not already provide one):

```yaml
      column_formats:
        - # One click opens the source file - the DDI hand enrichment
          # override source especially. Cell text stays the readable
          # path; the stored target is the encoded file URL. First
          # click ever from sandboxed Mac Excel may show a one-time
          # grant-access dialog.
          columns: ["Path"]
          make_hyperlinks: "file_paths"
```

Optional width control if auto-fit makes the Path column excessive on
long Dropbox paths:

```yaml
          width: 60
```

## 3. Contracts tinted columns: FCE4D6 to EAF3FB

On the Contracts sheet rule tinting the hand-maintained columns
(`Deposit`, `Deposit App`, `DDI Manual Override`), change:

```yaml
          background_color: "FCE4D6"
```

to:

```yaml
          background_color: "EAF3FB"
```

The paired `border_style: "thin"` / `border_color: "D9D9D9"` gridline
remedy stays exactly as is - the swatch review kept the ruling
doctrine, only the hue moved (light blue, lightest that still
registers on a normal-brightness display).

## Optional: same Path treatment in the contracts refresh recipe

The diff report Sources tab is already visible. If the same one-click
convenience is wanted there, apply fragments 1 and 2 (minus the
sheet_state removal) to the refresh recipe profile step and its
Sources format entry.

# End of file #
