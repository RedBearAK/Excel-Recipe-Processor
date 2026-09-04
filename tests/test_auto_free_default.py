"""
Tests for the auto_free_stages default and its schema-derived plan (2026-09-04).

tests/test_auto_free_default.py

Runnable with pytest, but written to run standalone and report a score.
A recipe that says nothing gets auto-free; false opts out; true still
works; a stage frees when its last consuming STEP completes; a stage
referenced only through a rule-level stage_name is counted; and - the
audit - every stage_in key every registered processor declares, at any
nesting, is counted exactly once by the plan.
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import excel_recipe_processor.core.pipeline  # registers processors

from excel_recipe_processor.core.base_processor import registry
from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.config_schema import stage_references


def _recipe(extra_settings: dict | None = None) -> dict:
    """Two stages; the first is consumed twice, the second once."""
    settings = {
        'stages': [
            {'stage_name': 'stg_free_test_a', 'description': 'a', 'protected': False},
            {'stage_name': 'stg_free_test_b', 'description': 'b', 'protected': False},
        ],
    }
    settings.update(extra_settings or {})
    return {
        'settings': settings,
        'recipe': [
            {'processor_type': 'filter_data', 'source_stage': 'stg_free_test_a',
             'save_to_stage': 'stg_free_test_b', 'filters': []},
            {'processor_type': 'export_file', 'source_stage': 'stg_free_test_a',
             'output_file': 'x.xlsx'},
            {'processor_type': 'export_file', 'source_stage': 'stg_free_test_b',
             'output_file': 'y.xlsx'},
        ],
    }


def _fresh(recipe: dict) -> None:
    StageManager.cleanup_stages()
    StageManager.initialize_stages(max_stages=20)
    StageManager.declare_recipe_stages(recipe)
    StageManager.plan_auto_free(recipe, registry, lambda config: config)


def test_default_is_on():
    """No setting at all: auto-free is on and the consumer scan ran."""
    print("\nTesting the default...")

    _fresh(_recipe())

    if not StageManager._auto_free:
        print("  ✗ auto-free off with no setting")
        return False
    print("  ✓ auto-free on with no setting")

    expected = StageManager._expected_uses
    if expected.get('stg_free_test_a') == 2 and expected.get('stg_free_test_b') == 1:
        print(f"  ✓ consumer scan counted a=2, b=1")
        return True

    print(f"  ✗ consumer scan gave {expected}")
    return False


def test_false_opts_out_and_true_still_works():
    """auto_free_stages: false disables it; true is the same as the default."""
    print("\nTesting the opt-out and the explicit opt-in...")

    _fresh(_recipe({'auto_free_stages': False}))
    if StageManager._auto_free or StageManager._expected_uses:
        print("  ✗ false did not opt out")
        return False
    print("  ✓ false opts out, no consumer scan")

    _fresh(_recipe({'auto_free_stages': True}))
    if not StageManager._auto_free:
        print("  ✗ true did not enable")
        return False
    print("  ✓ true enables")
    return True


def test_stage_frees_after_last_consuming_step_by_default():
    """Under the default, a stage frees when its last consuming STEP completes.

    Steps 0 and 1 both consume stg_free_test_a; loads do not free anything
    (consuming-step counting, 2026-08-26), the pipeline's step-complete
    hook does.
    """
    print("\nTesting that a stage frees under the default...")

    _fresh(_recipe())
    frame = pd.DataFrame({'x': [1, 2, 3]})
    StageManager.save_stage('stg_free_test_a', frame, description='a')

    StageManager.load_stage('stg_free_test_a')
    StageManager.load_stage('stg_free_test_a')
    if not StageManager.stage_exists('stg_free_test_a'):
        print("  ✗ freed on load; freeing belongs to step completion")
        return False
    print("  ✓ two loads free nothing")

    StageManager.auto_free_after_step(0)
    if not StageManager.stage_exists('stg_free_test_a'):
        print("  ✗ freed after the FIRST of two consuming steps")
        return False
    print("  ✓ alive after the first consuming step")

    StageManager.auto_free_after_step(1)
    if StageManager.stage_exists('stg_free_test_a'):
        print("  ✗ still alive after its last consuming step")
        return False
    print("  ✓ freed after its last consuming step")
    return True


def test_rule_level_stage_name_counts_as_a_consumer():
    """A stage named only in a filter rule's stage_name is a consumer (2026-09-04).

    The declaration key and the rule-reference key are spelled the same;
    inside a step it is always a reference, and missing it freed the
    stage before the filter that needed it.
    """
    print("\nTesting rule-level stage_name is counted...")

    recipe = {
        'settings': {'stages': [
            {'stage_name': 'stg_free_test_main', 'description': 'm', 'protected': False},
            {'stage_name': 'stg_free_test_lookup', 'description': 'l', 'protected': False},
            {'stage_name': 'stg_free_test_out', 'description': 'o', 'protected': False},
        ]},
        'recipe': [
            {'processor_type': 'export_file', 'source_stage': 'stg_free_test_lookup',
             'output_file': 'l.xlsx'},
            {'processor_type': 'filter_data', 'source_stage': 'stg_free_test_main',
             'save_to_stage': 'stg_free_test_out',
             'filters': [{'column': 'k', 'condition': 'in_stage',
                          'stage_name': 'stg_free_test_lookup', 'stage_column': 'k'}]},
        ],
    }
    _fresh(recipe)

    if StageManager._expected_uses.get('stg_free_test_lookup') != 2:
        print(f"  ✗ lookup counted {StageManager._expected_uses.get('stg_free_test_lookup')} use(s), expected 2")
        return False
    print("  ✓ lookup counted twice: the export and the filter rule")

    StageManager.save_stage('stg_free_test_lookup', pd.DataFrame({'k': [1]}), description='l')
    StageManager.auto_free_after_step(0)
    if not StageManager.stage_exists('stg_free_test_lookup'):
        print("  ✗ lookup freed before the filter that references it")
        return False
    print("  ✓ lookup alive for the filter step")
    return True


def _stage_in_paths(schema, prefix: tuple = ()) -> list:
    """Every (path, discriminator settings) at which the schema types a stage_in.

    A path is a tuple of key names with '[]' marking a list_of_mappings
    item; the settings are the discriminator values needed to make a
    variant's keys legal.
    """
    found = []
    tables = [({}, list(schema.keys.values()))]
    for discriminator, table in schema.variants.items():
        for value, variant in table.items():
            tables.append(({discriminator: value}, list(variant.keys.values())))
    for settings, keys in tables:
        for key in keys:
            if key.kind == 'stage_in':
                found.append((prefix + (key.name,), settings))
            elif key.kind == 'mapping' and key.schema is not None:
                found.extend((path, {**settings, **inner}) for path, inner
                             in _stage_in_paths(key.schema, prefix + (key.name,)))
            elif key.kind == 'list_of_mappings' and key.schema is not None:
                found.extend((path, {**settings, **inner}) for path, inner
                             in _stage_in_paths(key.schema, prefix + (key.name, '[]')))
    return found


def _place(config: dict, path: tuple, value) -> None:
    """Set value at path, creating mappings and single-item lists on the way."""
    node = config
    for index, part in enumerate(path[:-1]):
        if part == '[]':
            continue
        following_is_list = index + 1 < len(path) - 1 and path[index + 1] == '[]'
        if following_is_list:
            node = node.setdefault(part, [{}])[0]
        else:
            node = node.setdefault(part, {})
    node[path[-1]] = value


def test_every_declared_stage_in_key_is_counted():
    """The audit: one synthetic step per processor, a sentinel at every
    stage_in key its schema declares, each counted exactly once."""
    print("\nAuditing every processor's stage_in keys against the plan...")

    passed = True
    audited = 0
    for processor_type, processor_class in sorted(registry._processors.items()):
        schema = processor_class.full_schema()
        if schema is None:
            print(f"  ✗ {processor_type}: no schema")
            passed = False
            continue
        paths = _stage_in_paths(schema)
        if not paths:
            continue

        # One step per distinct discriminator setting, so variant keys are legal
        groups = {}
        for path, settings in paths:
            groups.setdefault(tuple(sorted(settings.items())), []).append(path)

        for settings_items, group_paths in groups.items():
            step = {'processor_type': processor_type, **dict(settings_items)}
            sentinels = {}
            for path in group_paths:
                sentinel = f"stg_audit_{processor_type}_{'_'.join(p for p in path if p != '[]')}"
                _place(step, path, sentinel)
                sentinels[sentinel] = path
            recipe = {'settings': {'stages': []}, 'recipe': [step]}
            _fresh(recipe)
            counted = StageManager._expected_uses
            for sentinel, path in sentinels.items():
                if counted.get(sentinel) != 1:
                    print(f"  ✗ {processor_type}: {'/'.join(path)} counted "
                          f"{counted.get(sentinel, 0)} time(s), expected 1")
                    passed = False
            audited += len(sentinels)

    print(f"  {'✓' if passed else '✗'} {audited} stage_in key path(s) across "
          f"{len(registry._processors)} processors")
    return passed


def main():
    """Run every test and report a final score."""
    print("=== auto_free_stages default tests ===")

    tests = [
        test_default_is_on,
        test_false_opts_out_and_true_still_works,
        test_stage_frees_after_last_consuming_step_by_default,
        test_rule_level_stage_name_counts_as_a_consumer,
        test_every_declared_stage_in_key_is_counted,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"  ✗ {test_func.__name__} crashed: {error}")

    StageManager.cleanup_stages()
    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    exit(main())


# End of file #
