"""
Render the declared step schemas as a published reference.

excel_recipe_processor/core/schema_export.py

A model or a person writing a recipe should read ONE document that says,
for every processor, exactly which keys exist and nothing else - the
declarations, not prose that may have drifted. A model writing a NEW
processor should read the same document to see which family its
neighbours sit in and what each family contributes. This module renders
the live declarations (2026-09-04):

  --export-schemas JSON   machine form: families, per-processor merged
                          schema with kinds, required, defaults, choices,
                          variants, at-least-one groups, constructs
  --export-schemas MD     the same as a Markdown reference

Nothing here is hand-written per processor; if a schema changes, the
document changes with it.
"""

import json

from excel_recipe_processor.core.config_schema import (
    FAMILY_BASE, FAMILY_EXPORT, FAMILY_FILE_OPS, FAMILY_IMPORT, FAMILY_TRANSFORM, Key, Schema,
)


FAMILIES = [FAMILY_TRANSFORM, FAMILY_IMPORT, FAMILY_EXPORT, FAMILY_FILE_OPS, FAMILY_BASE]


def key_to_dict(key: Key) -> dict:
    entry = {'kind': key.kind}
    if key.kind == 'list':
        entry['item_kind'] = key.item_kind
    if key.required:
        entry['required'] = True
    if key.default is not None:
        entry['default'] = key.default
    if key.choices is not None:
        entry['choices'] = list(key.choices)
    if key.construct:
        entry['construct'] = key.construct
    if key.description:
        entry['description'] = key.description
    if key.schema is not None:
        entry['schema'] = schema_to_dict(key.schema)
    return entry


def schema_to_dict(schema: Schema) -> dict:
    out = {'keys': {name: key_to_dict(key) for name, key in schema.keys.items()}}
    if schema.at_least_one:
        out['at_least_one'] = [list(group) for group in schema.at_least_one]
    if schema.variants:
        out['variants'] = {
            discriminator: {str(value): schema_to_dict(variant) for value, variant in table.items()}
            for discriminator, table in schema.variants.items()
        }
    return out


def export_schemas(registry) -> dict:
    """Families plus every registered processor's merged schema (or null)."""
    families = {
        family.name: {
            'description': family.description,
            'constructs': sorted(family.constructs),
            'contributes': sorted(family.schema.keys),
        }
        for family in FAMILIES
    }
    processors = {}
    for name, processor_class in sorted(registry._processors.items()):
        schema = processor_class.full_schema()
        processors[name] = {
            'family': processor_class.family.name,
            'writes_stage': getattr(processor_class, 'writes_stage', True),
            'schema': schema_to_dict(schema) if schema is not None else None,
        }
    return {
        'convention': (
            'An evaluated string never sits under a bare key: pandas_formula, pandas_rules, '
            'pandas_default, excel_formula name their dialect. Column-name lists are strings, '
            'never positions. column_names / column_refs pairs exist only in file_ops.'
        ),
        'kinds': {
            'str': 'text', 'int': 'integer (not bool)', 'number': 'int or float', 'bool': 'true/false',
            'list': 'list of item_kind', 'mapping': 'closed mapping with its own keys',
            'open_mapping': 'mapping whose keys are the author\'s data, not vocabulary',
            'list_of_mappings': 'list, each element a closed mapping', 'any': 'unchecked',
            'stage_in': 'name of a stage this step reads', 'stage_out': 'name of a stage this step writes',
            'stage_release': 'list of stage names this step frees',
        },
        'families': families,
        'processors': processors,
    }


def _render_keys(lines: list, keys: dict, indent: int) -> None:
    pad = '  ' * indent
    for name, key in keys.items():
        bits = [key['kind'] + (f" of {key['item_kind']}" if key['kind'] == 'list' else '')]
        if key.get('required'):
            bits.append('REQUIRED')
        if 'default' in key:
            bits.append(f"default {json.dumps(key['default'])}")
        if 'choices' in key:
            bits.append('one of ' + ', '.join(str(c) for c in key['choices']))
        desc = f" - {key['description']}" if key.get('description') else ''
        lines.append(f"{pad}- `{name}`: {'; '.join(bits)}{desc}")
        if 'schema' in key:
            _render_schema(lines, key['schema'], indent + 1)


def _render_schema(lines: list, schema: dict, indent: int) -> None:
    _render_keys(lines, schema['keys'], indent)
    pad = '  ' * indent
    for group in schema.get('at_least_one', []):
        lines.append(f"{pad}- at least one of: " + ', '.join(f'`{g}`' for g in group))
    for discriminator, table in schema.get('variants', {}).items():
        for value, variant in table.items():
            lines.append(f"{pad}- when `{discriminator}` = `{value}`:")
            _render_schema(lines, variant, indent + 1)


def render_markdown(exported: dict) -> str:
    lines = ['# Recipe step schemas', '', 'Generated from the processors\' declared schemas. '
             'Keys not listed here are refused at recipe load.', '', f"Convention: {exported['convention']}", '',
             '## Families', '']
    for name, family in exported['families'].items():
        lines.append(f"- **{name}** - {family['description']}. Contributes: "
                     + ', '.join(f'`{k}`' for k in family['contributes'])
                     + '. Selector constructs offered: ' + ', '.join(family['constructs']))
    lines += ['', '## Kinds', '']
    for kind, meaning in exported['kinds'].items():
        lines.append(f"- `{kind}`: {meaning}")
    lines += ['', '## Processors', '']
    for name, entry in exported['processors'].items():
        check = ' (check: reads, writes nothing)' if not entry['writes_stage'] else ''
        lines.append(f"### `{name}`  - family `{entry['family']}`{check}")
        lines.append('')
        if entry['schema'] is None:
            lines.append('_No schema declared yet; only stage keys are checked._')
        else:
            _render_schema(lines, entry['schema'], 0)
        lines.append('')
    return '\n'.join(lines) + '\n'


# --------------------------------------------------------------------------
# Per-processor pages, generated: description + schema + validated examples
# --------------------------------------------------------------------------

def render_processor_page(name: str, processor_class, exported_entry: dict) -> str:
    """
    One Markdown page for a processor, assembled from sources the test
    suite keeps honest: the capability description, the declared schema,
    and the examples file (whose every step validates against the schema).
    Prose belongs in those sources; nothing here is hand-written per page.
    """
    lines = [f"# `{name}`", '']
    description = ''
    capabilities = {}
    try:
        config = dict(processor_class.get_minimal_config())
        config.setdefault('processor_type', name)
        config.setdefault('step_description', 'docs')
        capabilities = processor_class(config).get_capabilities() or {}
        description = capabilities.get('description', '')
    except Exception:
        description = ''
    family = exported_entry['family']
    check = ' - a check: reads a stage, writes nothing' if not exported_entry['writes_stage'] else ''
    lines.append(f"**Family:** `{family}`{check}")
    lines.append('')
    if description:
        lines.append(description)
        lines.append('')
    notes = {k: v for k, v in capabilities.items()
             if k not in ('description',) and isinstance(v, str) and len(v) > 20}
    if notes:
        lines.append('## Notes')
        lines.append('')
        for key, value in notes.items():
            lines.append(f"- **{key.replace('_', ' ')}**: {value}")
        lines.append('')
    lines.append('## Keys')
    lines.append('')
    lines.append('Generated from the declared schema; keys not listed are refused at recipe load.')
    lines.append('')
    if exported_entry['schema'] is not None:
        _render_schema(lines, exported_entry['schema'], 0)
    lines.append('')
    try:
        examples = processor_class.get_usage_examples(processor_class(config)) if False else None
    except Exception:
        examples = None
    try:
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        examples = load_processor_examples(name)
    except Exception:
        examples = None
    if isinstance(examples, dict):
        blocks = [(k, v) for k, v in examples.items() if isinstance(v, dict) and 'yaml' in v]
        if blocks:
            lines.append('## Examples')
            lines.append('')
            lines.append('Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).')
            lines.append('')
            for key, block in blocks:
                title = key.replace('_example', '').replace('_', ' ')
                lines.append(f"### {title}")
                lines.append('')
                if block.get('description'):
                    lines.append(str(block['description']))
                    lines.append('')
                lines.append('```yaml')
                lines.append(str(block['yaml']).rstrip('\n'))
                lines.append('```')
                lines.append('')
        details = examples.get('parameter_details')
        if isinstance(details, dict) and details:
            lines.append('## Parameter notes')
            lines.append('')
            for key, info in details.items():
                if isinstance(info, dict):
                    desc = info.get('description', '')
                    extra = []
                    if 'default' in info:
                        extra.append(f"default `{info['default']}`")
                    if info.get('required') is True:
                        extra.append('required')
                    tail = f" ({', '.join(extra)})" if extra else ''
                    lines.append(f"- `{key}`{tail}: {desc}")
                else:
                    lines.append(f"- `{key}`: {info}")
            lines.append('')
    return '\n'.join(lines) + '\n'


def export_processor_docs(registry, folder) -> list:
    """Write one page per processor plus an index; return the paths written."""
    from pathlib import Path
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    exported = export_schemas(registry)
    written = []
    index = ['# Processors', '', 'One generated page per processor: description, declared keys, validated examples.',
             'Regenerate with `python -m excel_recipe_processor --export-docs docs/processors`.', '']
    by_family = {}
    for name, entry in exported['processors'].items():
        by_family.setdefault(entry['family'], []).append(name)
    for family in ('import', 'transform', 'export', 'file_ops', 'base'):
        names = by_family.get(family, [])
        if not names:
            continue
        index.append(f"## {family}")
        index.append('')
        for name in names:
            index.append(f"- [`{name}`]({name}.md)")
        index.append('')
    for name, entry in exported['processors'].items():
        page = render_processor_page(name, registry._processors[name], entry)
        path = folder / f"{name}.md"
        path.write_text(page)
        written.append(path)
    (folder / 'README.md').write_text('\n'.join(index) + '\n')
    written.append(folder / 'README.md')
    return written


# End of file #
