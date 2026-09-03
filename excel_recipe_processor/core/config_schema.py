"""
Declared configuration schemas for recipe steps.

excel_recipe_processor/core/config_schema.py

A recipe step used to be a free-form mapping that each processor reached
into with get_config_value(key, default); any key it did not reach for was
silently ignored, so a misspelled option applied its default and the
author learned nothing until the output was wrong (2026-09-03). A step is
now validated against a declared schema BEFORE any step runs: unknown
keys halt with a nearest-name suggestion, wrong types and missing
required keys halt by step number.

Vocabulary (four constructs cover every processor in the repo):

  Key       one accepted key: kind, required, default, choices, description
  Schema    a CLOSED mapping of Keys, optionally with variants - a
            discriminator key whose value selects extra sibling keys
  kinds     scalars (str, int, number, bool), list (of one scalar kind),
            mapping (closed, its own Schema), open_mapping (data keyed by
            the author - column names, rename maps - never vocabulary),
            list_of_mappings (each element against one Schema), any,
            plus stage_in / stage_out (str) so the stage graph is derived
            from the schema rather than from a hardcoded key list

Families contribute their slice of every processor's schema and decide
which column-selector constructs a processor MAY use:

  name_list        a list of column NAME strings - the only selector a
                   Transform processor can hold (a frame has no letters)
  name_ref_pair    column_names / column_refs siblings, at least one
                   present - legal only where a positional Excel ref is a
                   real way to say which column (file-addressing families)
  typed_item_list  ordered entries of {name: ...} or {ref: ...}, for a
                   file-addressing operation whose order matters

A processor declares its own keys with config_schema(); the family's
contribution is merged in by full_schema(). A processor cannot redefine a
family key, and cannot use a construct its family does not offer - both
are checked when the class is DEFINED, not when a recipe runs.
"""

import difflib


SCALAR_KINDS = ('str', 'int', 'number', 'bool')
CONTAINER_KINDS = ('list', 'mapping', 'open_mapping', 'list_of_mappings', 'any')
STAGE_KINDS = ('stage_in', 'stage_out', 'stage_release')
ALL_KINDS = SCALAR_KINDS + CONTAINER_KINDS + STAGE_KINDS

CONSTRUCT_NAME_LIST = 'name_list'
CONSTRUCT_NAME_REF_PAIR = 'name_ref_pair'
CONSTRUCT_TYPED_ITEM_LIST = 'typed_item_list'
ALL_CONSTRUCTS = (CONSTRUCT_NAME_LIST, CONSTRUCT_NAME_REF_PAIR, CONSTRUCT_TYPED_ITEM_LIST)


class SchemaDefinitionError(Exception):
    """A schema is malformed or breaks a family rule. Raised at class definition."""


class Key:
    """One accepted configuration key."""

    def __init__(self, name: str, kind: str, required: bool = False, default=None,
                 choices=None, description: str = '', schema=None, item_kind: str = 'str',
                 construct: str = ''):
        if not isinstance(name, str) or not name.strip():
            raise SchemaDefinitionError(f"Key name must be a non-empty string, got {name!r}")
        if kind not in ALL_KINDS:
            raise SchemaDefinitionError(f"Key '{name}': unknown kind {kind!r}; kinds: {ALL_KINDS}")
        if kind in ('mapping', 'list_of_mappings') and not isinstance(schema, Schema):
            raise SchemaDefinitionError(f"Key '{name}': kind {kind} requires a Schema")
        if kind not in ('mapping', 'list_of_mappings') and schema is not None:
            raise SchemaDefinitionError(f"Key '{name}': kind {kind} does not take a Schema")
        if kind == 'list' and item_kind not in SCALAR_KINDS + ('any',):
            raise SchemaDefinitionError(f"Key '{name}': list item_kind must be a scalar kind, got {item_kind!r}")
        if choices is not None and not isinstance(choices, (list, tuple)):
            raise SchemaDefinitionError(f"Key '{name}': choices must be a list")
        if construct and construct not in ALL_CONSTRUCTS:
            raise SchemaDefinitionError(f"Key '{name}': unknown construct {construct!r}")
        self.name = name
        self.kind = kind
        self.required = bool(required)
        self.default = default
        self.choices = list(choices) if choices is not None else None
        self.description = description
        self.schema = schema
        self.item_kind = item_kind
        self.construct = construct


class Schema:
    """
    A closed mapping of Keys, optionally with variants.

    variants: {discriminator_key: {value: Schema}} - when the discriminator
    holds a value, that value's Schema keys become legal siblings; keys from
    another value's Schema are unknown. A variant Schema may itself carry
    required keys, which are required only under its value.
    """

    def __init__(self, keys: list, variants: dict = None, at_least_one: list = None):
        names = [key.name for key in keys]
        if len(set(names)) != len(names):
            dupes = sorted({n for n in names if names.count(n) > 1})
            raise SchemaDefinitionError(f"Schema declares duplicate key(s) {dupes}")
        for key in keys:
            if not isinstance(key, Key):
                raise SchemaDefinitionError(f"Schema entries must be Key instances, got {type(key).__name__}")
        self.keys = {key.name: key for key in keys}
        self.variants = variants or {}
        for discriminator, table in self.variants.items():
            if discriminator not in self.keys:
                raise SchemaDefinitionError(f"Variant discriminator '{discriminator}' is not a declared key")
            if not isinstance(table, dict) or not all(isinstance(s, Schema) for s in table.values()):
                raise SchemaDefinitionError(f"Variants for '{discriminator}' must map value -> Schema")
        self.at_least_one = list(at_least_one or [])
        for group in self.at_least_one:
            for name in group:
                if name not in self.keys:
                    raise SchemaDefinitionError(f"at_least_one names undeclared key '{name}'")

    def constructs_used(self) -> set:
        used = {key.construct for key in self.keys.values() if key.construct}
        for key in self.keys.values():
            if key.schema is not None:
                used |= key.schema.constructs_used()
        for table in self.variants.values():
            for schema in table.values():
                used |= schema.constructs_used()
        return used

    def merged_with(self, other, label: str = 'schema'):
        """Family contribution first, processor keys second; no redefinition."""
        overlap = set(self.keys) & set(other.keys)
        if overlap:
            raise SchemaDefinitionError(
                f"{label} redefines family key(s) {sorted(overlap)}; family keys are fixed"
            )
        variants = dict(self.variants)
        for discriminator, table in other.variants.items():
            if discriminator in variants:
                raise SchemaDefinitionError(f"{label} redefines family variant '{discriminator}'")
            variants[discriminator] = table
        return Schema(list(self.keys.values()) + list(other.keys.values()), variants,
                      self.at_least_one + other.at_least_one)


# --------------------------------------------------------------------------
# Selector constructs - the only ways a schema may say "which columns"
# --------------------------------------------------------------------------

def name_list(name: str, required: bool = False, description: str = '') -> Key:
    """A list of column NAME strings. Names are strings, never positions."""
    return Key(name, 'list', required=required, item_kind='str',
               description=description or 'Column names', construct=CONSTRUCT_NAME_LIST)


def name_ref_pair(description: str = '') -> tuple:
    """
    column_names / column_refs siblings, at least one present.

    Returns (keys, at_least_one_group); the caller passes both to Schema.
    Legal only in file-addressing families, where a positional Excel ref
    ("A", "BQ") is a real way to name a column and a header named "ETD"
    or "BQ" could otherwise be mistaken for one.
    """
    keys = [
        Key('column_names', 'list', item_kind='str', construct=CONSTRUCT_NAME_REF_PAIR,
            description=description or 'Header NAME strings'),
        Key('column_refs', 'list', item_kind='str', construct=CONSTRUCT_NAME_REF_PAIR,
            description='Positional Excel refs like A or BQ - never header names'),
    ]
    return keys, ['column_names', 'column_refs']


def typed_item_list(name: str, required: bool = False, description: str = '') -> Key:
    """Ordered entries, each {name: ...} or {ref: ...}, for order-sensitive mixed selection."""
    item = Schema([
        Key('name', 'str', description='Header name'),
        Key('ref', 'str', description='Positional Excel ref'),
    ], at_least_one=[['name', 'ref']])
    return Key(name, 'list_of_mappings', required=required, schema=item,
               description=description or 'Ordered column entries by name or ref',
               construct=CONSTRUCT_TYPED_ITEM_LIST)


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------

def _kind_matches(value, kind: str, item_kind: str = 'str') -> bool:
    if kind == 'any':
        return True
    if kind in ('str', 'stage_in', 'stage_out'):
        return isinstance(value, str)
    if kind == 'stage_release':
        return isinstance(value, list) and all(isinstance(v, str) for v in value)
    if kind == 'int':
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == 'number':
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == 'bool':
        return isinstance(value, bool)
    if kind == 'list':
        if not isinstance(value, list):
            return False
        return all(_kind_matches(item, item_kind) for item in value)
    if kind in ('mapping', 'open_mapping'):
        return isinstance(value, dict)
    if kind == 'list_of_mappings':
        return isinstance(value, list) and all(isinstance(item, dict) for item in value)
    return False


def _describe_kind(key: Key) -> str:
    if key.kind == 'list':
        return f"list of {key.item_kind}"
    if key.kind in ('stage_in', 'stage_out'):
        return 'stage name (str)'
    if key.kind == 'stage_release':
        return 'list of stage names'
    return key.kind


def suggest_key(unknown: str, known: list) -> str:
    matches = difflib.get_close_matches(unknown, known, n=1, cutoff=0.6)
    return matches[0] if matches else ''


def validate_config(config: dict, schema: Schema, path: str = '', allow_unresolved: bool = True) -> list:
    """
    Validate a mapping against a Schema. Returns a list of error strings.

    allow_unresolved: a string value holding a {token} placeholder passes
    the kind check where a non-string kind is declared, because variable
    substitution may still turn it into the right shape (a {list_str:...}
    token becomes a list). Validation runs after substitution in the
    pipeline, so the allowance only matters for callers validating raw
    recipe text.
    """
    errors = []
    prefix = f"{path}: " if path else ''
    if not isinstance(config, dict):
        return [f"{prefix}must be a mapping, got {type(config).__name__}"]

    legal = dict(schema.keys)
    active_variant_schemas = []
    for discriminator, table in schema.variants.items():
        value = config.get(discriminator, legal[discriminator].default)
        if value in table:
            variant = table[value]
            for name, key in variant.keys.items():
                if name in legal:
                    errors.append(f"{prefix}variant '{discriminator}={value}' redeclares key '{name}'")
                legal[name] = key
            active_variant_schemas.append((discriminator, value, variant))
        elif value is not None and legal[discriminator].choices is None:
            errors.append(
                f"{prefix}'{discriminator}' value {value!r} has no variant; "
                f"known: {sorted(table)}"
            )

    for name in config:
        if name in legal:
            continue
        hint = suggest_key(name, list(legal))
        suggestion = f"; did you mean '{hint}'?" if hint else ''
        errors.append(f"{prefix}unknown key '{name}'{suggestion}")

    for name, key in legal.items():
        if key.required and name not in config:
            errors.append(f"{prefix}missing required key '{name}'")

    for discriminator, value, variant in active_variant_schemas:
        for group in variant.at_least_one:
            if not any(member in config for member in group):
                errors.append(f"{prefix}variant '{discriminator}={value}' requires one of {group}")
    for group in schema.at_least_one:
        if not any(member in config for member in group):
            errors.append(f"{prefix}requires at least one of {group}")

    for name, value in config.items():
        if name not in legal:
            continue
        key = legal[name]
        child = f"{path}.{name}" if path else name
        if (allow_unresolved and isinstance(value, str) and '{' in value and '}' in value
                and key.kind not in ('str', 'stage_in', 'stage_out', 'stage_release', 'any')):
            continue
        if not _kind_matches(value, key.kind, key.item_kind):
            errors.append(f"{child}: expected {_describe_kind(key)}, got {type(value).__name__}")
            continue
        if key.choices is not None and value not in key.choices:
            errors.append(f"{child}: {value!r} is not one of {key.choices}")
        if key.kind == 'mapping':
            errors.extend(validate_config(value, key.schema, child, allow_unresolved))
        elif key.kind == 'list_of_mappings':
            for index, item in enumerate(value):
                errors.extend(validate_config(item, key.schema, f"{child}[{index + 1}]", allow_unresolved))
    return errors


def stage_references(config: dict, schema: Schema) -> tuple:
    """
    Read the stage names a step reads and writes, from its schema.

    Returns (reads, writes, releases) as lists of stage-name strings,
    walking nested mappings and lists so an export's
    sheets_to_create[].data_source or a combine's insert_from_stage are
    found wherever the schema put them.
    """
    reads, writes, releases = [], [], []
    if not isinstance(config, dict):
        return reads, writes, releases
    legal = dict(schema.keys)
    for discriminator, table in schema.variants.items():
        value = config.get(discriminator, legal[discriminator].default)
        if value in table:
            legal.update(table[value].keys)
    for name, value in config.items():
        key = legal.get(name)
        if key is None:
            continue
        if key.kind == 'stage_in' and isinstance(value, str):
            reads.append(value)
        elif key.kind == 'stage_out' and isinstance(value, str):
            writes.append(value)
        elif key.kind == 'stage_release' and isinstance(value, list):
            releases.extend(v for v in value if isinstance(v, str))
        elif key.kind == 'mapping' and isinstance(value, dict):
            r, w, x = stage_references(value, key.schema)
            reads.extend(r); writes.extend(w); releases.extend(x)
        elif key.kind == 'list_of_mappings' and isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    r, w, x = stage_references(item, key.schema)
                    reads.extend(r); writes.extend(w); releases.extend(x)
    return reads, writes, releases


# --------------------------------------------------------------------------
# Families: what each contributes, and which constructs it offers
# --------------------------------------------------------------------------

COMMON_STEP_KEYS = [
    Key('step_description', 'str', description='Human-readable step name; apostrophe-free by house style'),
    Key('processor_type', 'str', required=True, description='Registered processor name'),
    Key('on_error', 'str', choices=['halt', 'skip', 'continue'],
        description='Per-step override of the recipe error policy'),
]

STAGE_READ_KEYS = [
    Key('source_stage', 'stage_in', required=True, description='Stage to read'),
]
STAGE_WRITE_KEYS = [
    Key('save_to_stage', 'stage_out', required=True, description='Stage to write'),
    Key('confirm_stage_replacement', 'bool', default=False,
        description='Required true to overwrite an existing stage'),
]


class Family:
    """A processor family: its schema contribution and the constructs it offers."""

    def __init__(self, name: str, keys: list, constructs: tuple, description: str,
                 optional_write_keys: list = None):
        self.name = name
        self.keys = list(keys)
        self.optional_write_keys = list(optional_write_keys or [])
        self.schema = Schema(COMMON_STEP_KEYS + keys + self.optional_write_keys)
        self.constructs = set(constructs)
        self.description = description

    def contribution(self, writes_stage: bool = True) -> Schema:
        """
        The family's schema for one processor. A Transform that declares
        writes_stage = False (a CHECK: reads a stage, writes nothing) gets
        the contribution without the stage-write keys.
        """
        if writes_stage or not self.optional_write_keys:
            return self.schema
        return Schema(COMMON_STEP_KEYS + self.keys)


FAMILY_TRANSFORM = Family(
    'transform',
    STAGE_READ_KEYS,
    (CONSTRUCT_NAME_LIST,),
    'Reads a stage, returns a stage; knows columns only by name',
    optional_write_keys=STAGE_WRITE_KEYS,
)

FAMILY_IMPORT = Family(
    'import',
    STAGE_WRITE_KEYS,
    (CONSTRUCT_NAME_LIST,),
    'Creates a stage from outside the pipeline',
)

FAMILY_EXPORT = Family(
    'export',
    STAGE_READ_KEYS,
    (CONSTRUCT_NAME_LIST,),
    'Consumes a stage into a file',
)

FAMILY_FILE_OPS = Family(
    'file_ops',
    [],
    (CONSTRUCT_NAME_LIST, CONSTRUCT_NAME_REF_PAIR, CONSTRUCT_TYPED_ITEM_LIST),
    'Operates on a workbook in place; positional refs are legal here',
)

FAMILY_BASE = Family(
    'base',
    [],
    (CONSTRUCT_NAME_LIST,),
    'Steps that fit no family (stage utilities); declare their stage keys themselves',
)


def check_processor_schema(family: Family, own: Schema, label: str, writes_stage: bool = True) -> Schema:
    """
    Merge a processor's own schema with its family's and enforce the family
    rules. Raised at class definition so a wrong construct never reaches a
    recipe.
    """
    illegal = own.constructs_used() - family.constructs
    if illegal:
        raise SchemaDefinitionError(
            f"{label}: family '{family.name}' does not offer construct(s) {sorted(illegal)}; "
            f"offered: {sorted(family.constructs)}"
        )
    return family.contribution(writes_stage).merged_with(own, label)


# End of file #
