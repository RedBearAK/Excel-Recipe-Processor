"""
Test the pre-execution validation phase: step schemas and the stage graph.

File: excel_recipe_processor/tests/test_recipe_validation_phase.py

Recipes here are built in memory and passed to validate_recipe with the
real processor registry and a no-op substitution, so the phase is tested
exactly as the pipeline calls it. Errors fail; warnings do not.

Run: PYTHONPATH=. python3 tests/test_recipe_validation_phase.py
"""

import sys

from excel_recipe_processor.core.pipeline import registry
from excel_recipe_processor.core.recipe_validation import validate_recipe


def recipe(steps: list, declared: list = None) -> dict:
    stages = [{'stage_name': name, 'description': name, 'protected': False} for name in (declared or [])]
    return {'settings': {'description': 't', 'stages': stages}, 'recipe': steps}


def imp(stage: str) -> dict:
    return {'step_description': f'import {stage}', 'processor_type': 'import_file',
            'input_file': 'x.xlsx', 'save_to_stage': stage}


def calc(src: str, dst: str, **extra) -> dict:
    step = {'step_description': f'calc {dst}', 'processor_type': 'add_calculated_column',
            'source_stage': src, 'save_to_stage': dst, 'new_column': 'X',
            'calculation': {'pandas_formula': '1'}}
    step.update(extra)
    return step


def exp(stage: str) -> dict:
    return {'step_description': f'export {stage}', 'processor_type': 'export_file',
            'source_stage': stage, 'output_file': 'out.xlsx'}


def run(data: dict):
    return validate_recipe(data, registry, lambda config: config)


def report(label: str, good: bool, detail: str = '') -> bool:
    print(f"  {label}: {'OK' if good else 'FAIL'}{'  ' + detail if detail else ''}")
    return good


def test_clean_recipe_passes() -> bool:
    print('\nTesting a clean recipe...')
    r = run(recipe([imp('stg_a'), calc('stg_a', 'stg_b'), exp('stg_b')], ['stg_a', 'stg_b']))
    return report('no errors, no warnings', r.ok and not r.warnings, f'{r.errors} {r.warnings}')


def test_schema_errors_by_step() -> bool:
    print('\nTesting schema errors reported by step number with suggestions...')
    r = run(recipe([imp('stg_a'), calc('stg_a', 'stg_b', new_colum='Y'), exp('stg_b')], ['stg_a', 'stg_b']))
    joined = '\n'.join(r.errors)
    ok = report('step number named', "step 2 'calc stg_b'" in joined)
    ok &= report('suggestion given', "did you mean 'new_column'" in joined)
    r = run(recipe([imp('stg_a'), calc('stg_a', 'stg_b', calculation_type='first_match'), exp('stg_b')],
                   ['stg_a', 'stg_b']))
    ok &= report('variant keys enforced', any("missing required key 'pandas_rules'" in e for e in r.errors)
                 and any("unknown key 'pandas_formula'" in e for e in r.errors))
    return ok


def test_stage_graph_errors() -> bool:
    print('\nTesting stage graph errors...')
    r = run(recipe([calc('stg_missing', 'stg_b'), exp('stg_b')], ['stg_b']))
    ok = report('read before write', any("reads stage 'stg_missing' before" in e for e in r.errors))
    r = run(recipe([imp('stg_a'), imp('stg_a'), exp('stg_a')], ['stg_a']))
    ok &= report('double write without confirm', any('already written by step 1' in e for e in r.errors))
    steps = [imp('stg_a'), imp('stg_a'), exp('stg_a')]
    steps[1]['confirm_stage_replacement'] = True
    r = run(recipe(steps, ['stg_a']))
    ok &= report('double write with confirm passes', r.ok, str(r.errors))
    r = run(recipe([imp('stg_a'), exp('stg_a')], ['stg_a', 'stg_never']))
    ok &= report('declared never written', any("'stg_never' is never written" in e for e in r.errors))
    r = run(recipe([imp('stg_a'), imp('stg_b'), exp('stg_a')], ['stg_a', 'stg_b']))
    ok &= report('declared never read', any("'stg_b' is written but never read" in e for e in r.errors))
    return ok


def test_undeclared_is_warning_only() -> bool:
    print('\nTesting undeclared stages warn but pass...')
    r = run(recipe([imp('stg_a'), exp('stg_a')], []))
    return report('warning, ok', r.ok and len(r.warnings) == 2, f'{r.warnings}')


def test_schema_less_processor_is_reported_not_failed() -> bool:
    print('\nTesting a schema-less processor passes with a report...')
    step = {'step_description': 'agg', 'processor_type': 'aggregate_data', 'source_stage': 'stg_a',
            'save_to_stage': 'stg_b', 'group_by': ['X'], 'aggregations': [], 'made_up_key': 1}
    r = run(recipe([imp('stg_a'), step, exp('stg_b')], ['stg_a', 'stg_b']))
    ok = report('passes', r.ok, str(r.errors))
    ok &= report('reported as schema-less', 'aggregate_data' in r.schema_less_types)
    step2 = {'step_description': 'agg', 'processor_type': 'aggregate_data', 'save_to_stage': 'stg_b',
             'group_by': ['X'], 'aggregations': []}
    r = run(recipe([imp('stg_a'), step2, exp('stg_b')], ['stg_a', 'stg_b']))
    ok &= report('family stage keys still required', any("missing required key 'source_stage'" in e for e in r.errors))
    return ok


def test_unknown_processor_type() -> bool:
    print('\nTesting unknown processor type...')
    r = run(recipe([{'step_description': 'x', 'processor_type': 'no_such_processor'}], []))
    return report('error', any('unknown processor_type' in e for e in r.errors))


def main() -> int:
    tests = [
        test_clean_recipe_passes,
        test_schema_errors_by_step,
        test_stage_graph_errors,
        test_undeclared_is_warning_only,
        test_schema_less_processor_is_reported_not_failed,
        test_unknown_processor_type,
    ]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as error:
            print(f'  EXCEPTION in {test.__name__}: {error}')
    print(f'\n{passed}/{len(tests)} tests passed')
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
