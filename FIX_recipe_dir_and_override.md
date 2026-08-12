# Running a recipe from anywhere

Two changes to the tool, plus a wrapper script.

## New: `{recipe_dir}` and `{recipe_parent_dir}`

Paths inside a recipe previously resolved against the **current working
directory**, so `lookup_dir: "lookup_source_files"` only worked if you happened
to be standing in the project folder. That is what made the command grow
absolute paths.

Both new variables are absolute, taken from the recipe's own location:

```yaml
settings:
  variables:
    project_dir: "{recipe_parent_dir}"
    lookup_dir:  "{recipe_parent_dir}/lookup_source_files"
    output_dir:  "{recipe_parent_dir}/output"
```

`recipe_dir` is the folder holding the recipe; `recipe_parent_dir` is its parent,
which is the useful one when recipes live in a `recipe_files/` subfolder.

The pipeline now passes `recipe_path` into `VariableSubstitution`, which it
previously did not — so these resolve rather than failing at load with
"Unresolved variables detected".

## Bug: CLI overrides were silently discarded

`--set donor_file X` was accepted, logged as resolved, and then had no effect.

`_re_resolve_custom_variables()` runs after every external variable is added and
re-resolves **every** recipe variable from its original template. A name given on
the command line that also appears in `settings.variables` was overwritten by the
recipe's own value immediately after being set.

The failure mode was quiet: the log said

```
✓ Resolved 3 external variables:
   donor_file = /path/to/the/one/I/wanted.xlsx
```

and the run then used the recipe's value regardless.

Re-resolution now skips any name supplied externally, so an override overrides.

## The wrapper

`vms-process` takes a download and passes everything else through:

```sh
vms-process 260806_VMS_All_2026sh-0800.xlsx
vms-process ~/Downloads/latest.xlsx --set output_dir .
vms-process latest.xlsx --verbose
```

Install as a real command rather than an alias, so it works in scripts, cron and
editor build steps where the RC file is never sourced:

```sh
mkdir -p ~/.local/bin
cp vms-process ~/.local/bin/
chmod +x ~/.local/bin/vms-process
```

Project location comes from `VMS_PROJECT_DIR`, defaulting to
`~/DropBox/Tech_scripts/VMS_Downloads`.

It resolves the download to an absolute path before handing it over. The recipe
anchors its own paths to the recipe location, but the download is given relative
to wherever you are standing, and nothing else in the run shares that reference
point.

It prefers a real `excel-recipe-processor` command if one is installed and falls
back to `python -m excel_recipe_processor`, so it works either way.

## Verified

Run from `/tmp/elsewhere`, a directory with no relationship to the project:

```
vms-process 260807_VMS_All.xlsx
  -> 43 steps, output in the project's output/ folder

vms-process 260807_VMS_All.xlsx --set output_dir .
  -> output beside the download instead
```

Full suite: 20 failures before and after, identical to baseline.

# End of file #
