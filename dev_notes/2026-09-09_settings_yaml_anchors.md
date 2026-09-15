# settings.yaml_anchors

`dev_notes/2026-09-09_settings_yaml_anchors.md`

## What and why

A large recipe repeats structures. The VMS processor now writes three
workbooks that share one header style, and each `format_excel` step
needs its own complete copy of the template, because a template belongs
to the step that declares it - `apply_templates` in another step looks
up a name in an empty dictionary and warns "not found - will be
skipped". Three copies of five lines means three places to edit a colour.

`settings.yaml_anchors` is a home for YAML anchors so the block can be
written once:

    settings:
      variables:
        var_header_green: "548235"
      yaml_anchors:
        - &tpl_lookup_header
          template_name: "tpl_lookup_header"
          header_bold: true
          header_background: true
          header_background_color: "{var_header_green}"
          header_text_color: "white"
          freeze_top_row: true

    recipe:
      - step_description: "Format the glossary file"
        processor_type: "format_excel"
        target_file: "..."
        templates:
          - *tpl_lookup_header

## What the tool actually does

Almost nothing, and that is the point. The YAML parser resolves anchors
and aliases while reading the file, so by the time the recipe reaches
the loader there are no anchors - only the same mapping repeated
wherever it was referenced. The loader:

- accepts the key (it worked before only because unknown settings keys
  are ignored, which is accidental and would break the day settings
  validation tightens);
- checks the SHAPE only - a list or a mapping - so `yaml_anchors: true`
  is reported rather than silently doing nothing;
- warns when it is empty;
- never looks inside. Contents that look like stages, variables or
  steps have no effect (there is a test for exactly that).

## The design argument, recorded

Kris asked whether recipe-level templates made sense, then answered it
better: both a recipe-level template block and a YAML anchor make a step
stop being self-sufficient when copied elsewhere. The anchor is the
lesser evil because the expansion is textual - each step ends up with
the whole block, so the recipe ERP receives is exactly what you would
have typed by hand, and a step lifted out of it still works.

The third option Kris raised and we did NOT build: a recipe-level
template DECLARATION used as a check - every step keeps its own copy,
and the validator refuses a copy that differs from the declared canon.
That is the shape to reach for if stale copies ever become a real
problem; it fits how stage declarations and processor schemas already
work (assert intent, enforce it, do not supply the value).

Anchors and variables are siblings, not competitors: a variable replaces
a SCALAR, an anchor reuses a STRUCTURE. They compose, and the example
above relies on it - the anchored block holds a variable, substituted
per step as anywhere else. Confirmed by running it: both workbooks come
out with fill 00548235.

Variables cannot change value mid-run (every caller of
`add_custom_variable` is in the pipeline's setup phase; no processor can
reach it), so the "aliases share one object" worry does not bite - there
is no per-step value for the shared object to disagree with. Still worth
the end-to-end test, since it is invisible in the YAML.

## Checked before shipping

- 5 new tests in `tests/test_settings_yaml_anchors.py`: aliases already
  expanded at load; seven shapes accepted / rejected / warned; parked
  contents inert; end-to-end run formatting two workbooks from one
  anchored block with the variable inside it resolved; a recipe with no
  such key unaffected.
- The full suite: 135 of 136 modules pass. The one failure,
  `test_all_processor_examples.py`, passes 9/9 on its own and only
  failed under a 60-second batch timeout.
- `vms_process.yaml` validates identically against the patched and the
  pristine loader: 116 steps, 0 warnings, both.
- One wart noticed and NOT touched, since it predates this change: the
  loader prefixes every warning with a warning emoji, and the
  `output_filename` deprecation message embeds a second one, so that
  message prints two. The new message omits it and prints one.


# End of file #
