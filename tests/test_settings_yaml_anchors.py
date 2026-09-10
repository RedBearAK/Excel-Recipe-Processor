"""
Test settings.yaml_anchors: a parking place for YAML anchors.

File: excel_recipe_processor/tests/test_settings_yaml_anchors.py

The mechanism belongs to the YAML parser, not to this tool - by the time
a recipe is loaded every alias has been expanded - so these tests check
the two things the tool IS responsible for: that the key is accepted and
its contents never interpreted, and that the shape is checked so a typo
is reported rather than ignored. The end-to-end cases then prove what
the docs claim: an aliased block reaches the processor intact, a
variable inside it is substituted, and each step gets its own copy.

Run: PYTHONPATH=. python3 tests/test_settings_yaml_anchors.py
"""

import os
import sys
import yaml
import shutil
import tempfile

from openpyxl import load_workbook

from excel_recipe_processor.config.recipe_loader import RecipeLoader


WORK = tempfile.mkdtemp(prefix='erp_anchors_')

RECIPE_WITH_ANCHOR = '''
settings:
  description: "Two workbooks sharing one header style"
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
  stages:
    - stage_name: "stg_rows"
      description: "rows"
      protected: false

recipe:
  - step_description: "Make a tiny stage"
    processor_type: "create_stage"
    save_to_stage: "stg_rows"
    data:
      format: "table"
      columns: ["Term", "Definition"]
      rows:
        - ["anchor", "a YAML feature"]

  - step_description: "Export the first workbook"
    processor_type: "export_file"
    source_stage: "stg_rows"
    output_file: "__WORK__/first.xlsx"
    sheet_name: "Glossary"

  - step_description: "Export the second workbook"
    processor_type: "export_file"
    source_stage: "stg_rows"
    output_file: "__WORK__/second.xlsx"
    sheet_name: "Glossary"

  - step_description: "Format the first workbook"
    processor_type: "format_excel"
    target_file: "__WORK__/first.xlsx"
    templates:
      - *tpl_lookup_header
    formatting:
      - sheet_names: ["Glossary"]
        apply_templates: ["tpl_lookup_header"]

  - step_description: "Format the second workbook"
    processor_type: "format_excel"
    target_file: "__WORK__/second.xlsx"
    templates:
      - *tpl_lookup_header
    formatting:
      - sheet_names: ["Glossary"]
        apply_templates: ["tpl_lookup_header"]
'''


def write_recipe(text: str, name: str) -> str:
    path = os.path.join(WORK, name)
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(text.replace('__WORK__', WORK))
    return path


def settings_only(anchors_line: str) -> str:
    return f'''
settings:
  description: "shape check"
{anchors_line}
recipe:
  - step_description: "nothing"
    processor_type: "create_stage"
    save_to_stage: "stg_x"
    data:
      format: "table"
      columns: ["A"]
      rows:
        - ["1"]
'''


def load_errors(text: str, name: str) -> tuple:
    """(errors, warnings) from the loader's own validation."""
    path = write_recipe(text, name)
    loader = RecipeLoader()
    try:
        loader.load_recipe_file(path)
    except Exception as error:
        return [str(error)], []
    result = loader.validate_recipe_structure()
    return result.get('errors', []), result.get('warnings', [])


def test_the_parser_expands_aliases_before_the_tool_sees_them() -> bool:
    print('\nEvery alias is already expanded when the recipe is loaded: no anchor survives into the data...')
    path = write_recipe(RECIPE_WITH_ANCHOR, 'anchored.yaml')
    with open(path, encoding='utf-8') as handle:
        raw_text = handle.read()
    data = yaml.safe_load(raw_text)
    steps = [step for step in data['recipe'] if step.get('processor_type') == 'format_excel']
    templates = [step['templates'][0] for step in steps]
    text_has_alias = '*tpl_lookup_header' in raw_text
    both_complete = all(template.get('header_bold') is True and 'header_background_color' in template
                        for template in templates)
    same_content = templates[0] == templates[1]
    good = text_has_alias and both_complete and same_content and len(templates) == 2
    print(f"  alias in the file={text_has_alias}; both steps hold the complete block={both_complete}; "
          f"identical={same_content} -> {'OK' if good else 'FAIL'}")
    return good


def test_shape_is_checked() -> bool:
    print('\nA list or a mapping is accepted; a scalar is an error; an empty one warns...')
    cases = [
        ('  yaml_anchors:\n    - &a\n      key: 1\n', 'list', False, False),
        ('  yaml_anchors:\n    first: &b\n      key: 1\n', 'mapping', False, False),
        ('  yaml_anchors: true\n', 'true', True, False),
        ('  yaml_anchors: "some string"\n', 'string', True, False),
        ('  yaml_anchors: 42\n', 'number', True, False),
        ('  yaml_anchors: []\n', 'empty list', False, True),
        ('  yaml_anchors: {}\n', 'empty mapping', False, True),
    ]
    good = True
    for anchors_line, label, expect_error, expect_warning in cases:
        errors, warnings = load_errors(settings_only(anchors_line), f"shape_{label.replace(' ', '_')}.yaml")
        has_error = any('yaml_anchors' in message for message in errors)
        has_warning = any('yaml_anchors' in message for message in warnings)
        if has_error != expect_error or has_warning != expect_warning:
            print(f"  FAIL {label}: error={has_error} (want {expect_error}), "
                  f"warning={has_warning} (want {expect_warning})")
            good = False
    print(f"  {len(cases)} shapes -> {'OK' if good else 'FAIL'}")
    return good


def test_contents_are_never_interpreted() -> bool:
    """Whatever is parked in there - keys that look like stages, like
    variables, like steps - must have no effect at all."""
    print('\nContents are opaque: things that LOOK like stages, variables or steps in there do nothing...')
    text = settings_only(
        '  yaml_anchors:\n'
        '    - &whatever\n'
        '      stage_name: "stg_not_a_stage"\n'
        '      processor_type: "no_such_processor"\n'
        '      variables:\n'
        '        var_not_real: "x"\n')
    errors, warnings = load_errors(text, 'opaque.yaml')
    complained = [message for message in errors + warnings
                  if 'no_such_processor' in message or 'stg_not_a_stage' in message
                  or 'var_not_real' in message]
    good = not complained
    print(f"  complaints about the parked contents={complained or 'none'} -> {'OK' if good else 'FAIL'}")
    return good


def test_an_aliased_template_reaches_both_steps() -> bool:
    """End to end: run the recipe and read the formatting back out of both
    workbooks, including the variable that was inside the anchored block."""
    print('\nEnd to end: the aliased template formats BOTH workbooks, variable inside it substituted...')
    from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
    path = write_recipe(RECIPE_WITH_ANCHOR, 'anchored_run.yaml')
    pipeline = RecipePipeline()
    try:
        pipeline.run_complete_recipe(path)
    except Exception as error:
        print(f"  FAIL recipe raised {type(error).__name__}: {str(error)[:140]}")
        return False
    results = {}
    for name in ('first.xlsx', 'second.xlsx'):
        workbook = load_workbook(os.path.join(WORK, name))
        cell = workbook['Glossary']['A1']
        results[name] = (str(cell.fill.start_color.rgb or ''), cell.font.bold)
        workbook.close()
    greens = all(rgb.endswith('548235') for rgb, _ in results.values())
    bolds = all(bold for _, bold in results.values())
    good = greens and bolds
    print(f"  {results} -> {'OK' if good else 'FAIL'}")
    return good


def test_a_recipe_without_the_key_is_unaffected() -> bool:
    print('\nA recipe with no yaml_anchors key behaves exactly as before...')
    text = RECIPE_WITH_ANCHOR
    lines = text.splitlines()
    start = next(index for index, line in enumerate(lines) if line.strip() == 'yaml_anchors:')
    end = next(index for index in range(start + 1, len(lines)) if lines[index].strip() == 'stages:')
    block = '\n'.join(lines[start + 1:end])
    # keep the template, drop the key: paste the block into both steps
    inline = block.replace('    - &tpl_lookup_header', '      - template_name: "tpl_lookup_header"') \
                  .replace('      template_name: "tpl_lookup_header"\n', '')
    inline = '\n'.join('  ' + line if line.strip() and not line.strip().startswith('- ') else line
                       for line in inline.splitlines())
    without = '\n'.join(lines[:start] + lines[end:]).replace(
        '    templates:\n      - *tpl_lookup_header',
        '    templates:\n      - template_name: "tpl_lookup_header"\n'
        '        header_bold: true\n        header_background: true\n'
        '        header_background_color: "{var_header_green}"\n'
        '        header_text_color: "white"\n        freeze_top_row: true')
    errors, warnings = load_errors(without, 'no_anchors.yaml')
    about_anchors = [message for message in errors + warnings if 'yaml_anchors' in message]
    good = not errors and not about_anchors
    print(f"  errors={errors or 'none'}; anchor complaints={about_anchors or 'none'} -> {'OK' if good else 'FAIL'}")
    return good


def test_the_documented_example_validates() -> bool:
    """The worked example in recipe_settings_examples.yaml is printed by
    --get-usage settings but validated by nothing else; hold it to the
    same standard as a recipe."""
    print('\nThe yaml_anchors_example in the settings examples file is a valid recipe...')
    from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
    examples_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                                 'excel_recipe_processor', 'config', '_examples', 'recipe_settings_examples.yaml')
    with open(examples_path, encoding='utf-8') as handle:
        examples = yaml.safe_load(handle)
    text = examples['yaml_anchors_example']['yaml']
    text = text.replace('reference_a.xlsx', os.path.join(WORK, 'ra.xlsx')).replace('reference_b.xlsx', os.path.join(WORK, 'rb.xlsx'))
    path = write_recipe(text, 'documented_example.yaml')
    loader = RecipeLoader()
    loader.load_recipe_file(path)
    result = loader.validate_recipe_structure()
    steps = yaml.safe_load(text)['recipe']
    both_hold_block = all(step['templates'][0].get('header_background_color') == '{var_header_green}' for step in steps)
    good = result['valid'] and both_hold_block
    print(f"  loader valid={result['valid']} errors={result['errors'] or 'none'}; both steps carry the anchored block={both_hold_block} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_the_parser_expands_aliases_before_the_tool_sees_them,
             test_shape_is_checked,
             test_contents_are_never_interpreted,
             test_an_aliased_template_reaches_both_steps,
             test_a_recipe_without_the_key_is_unaffected,
             test_the_documented_example_validates]
    passed = 0
    try:
        for test in tests:
            if test():
                passed += 1
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
