# Typed dict-reference family: dict_int/float/str/any (2026-08-17)

dev_notes/NOTES_2026-08-17_typed_dict_family.md

Per ruling: a repeat of the list fix, done now rather than deferred.
The insight that unlocked it: recipe dict variables are STRING-KEYED
mappings by nature (column names, labels, status codes), so declaring
keys-are-strings as doctrine collapses the key-x-value combination
space to the value axis alone - making the flat underscore family
workable where a bracketed mini-grammar had been rejected.

## The vocabulary

    {dict_int:name}    values converted to int, loudly on failure
    {dict_float:name}  values converted to float, loudly on failure
    {dict_str:name}    values converted to str
    {dict_any:name}    values pass through as declared (acknowledged
                       untyped/mixed/nested)
    {dict:name}        RETIRED - guided error names all four + doctrine

Keys are normalized to str in EVERY family, dict_any included.
Value-conversion errors name the key, value, and target type
("Cannot convert value for key 'b' ... (value: 'abc') to int").
Container semantics unchanged: the variable must already BE a dict;
CLI --variable strings fail loud at the container check (no parsing
path from a CLI string to a dict, deliberately).

## Bracket net extended

The typo net already caught {dict[str,int]:name} structurally (the
patterns match any {word[... shape); its message now teaches BOTH
underscore families, list and dict, with the key doctrine noted.

## Sweep (12 sites, 4 files, per-site per doctrine)

- String-valued mappings (status codes, region/status mappings,
  column rename mapping, large_mapping) -> {dict_str:...}
- Structural probes (empty dict, deeply_nested) and the
  container-mismatch error probe -> {dict_any:...} preserving their
  original semantics/failure modes
- generate_column_config emitted snippet -> {dict_str:...}
- Core capability docs and settings-examples variables comment now
  present both families side by side

## Tests

New tests/test_typed_dict_references.py (6/6): per-family value
conversion with type assertions, string-key doctrine (int keys
normalized in dict_str AND dict_any), keyed conversion failure,
retirement guidance naming all four plus the key doctrine, container
check on CLI strings, bracket net teaching both families.

Collateral green (10 modules) including test_typed_list_references
(the shared bracket-net message change kept its expectations true),
all three variable suites, both config generators, comprehensive
processors, and both example checkers.

# End of file #
