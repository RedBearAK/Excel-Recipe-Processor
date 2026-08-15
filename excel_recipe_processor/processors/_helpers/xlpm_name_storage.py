r"""
_xlpm. name-storage transformer for LAMBDA and LET.

excel_recipe_processor/processors/_helpers/xlpm_name_storage.py

Excel STORES the names declared by LAMBDA and LET with an `_xlpm.`
prefix - at the declaration and at every occurrence within the
construct's scope. Harvest-verified (2026-08-14, data-validation-test
workbook): =GROUPBY(A1:A6,B1:B6,LAMBDA(x,SUM(x))) is stored as
_xlfn.GROUPBY(A1:A6,B1:B6,_xlfn.LAMBDA(_xlpm.x,SUM(_xlpm.x))) - note
the legacy SUM stays BARE while the declared x is prefixed everywhere.
A stored bare name in a declaration slot is not merely #NAME?; it is
grammatically invalid, and Excel's repair strips the whole formula.

Scoping rules implemented (these are Excel's, not conveniences):
- LAMBDA(p1, ..., pN, body): parameters are in scope in the BODY only.
- LET(n1, v1, n2, v2, ..., body): names bind SEQUENTIALLY - nK is in
  scope in vK+1 onward and the body, NOT in v1..vK. So in
  LET(a, b+1, b, 2, a+b) the first b refers to an OUTER name and stays
  bare, while the declaration and the body's b are prefixed.
- Nesting accumulates (closures): LAMBDA(x, LAMBDA(y, x+y)) prefixes x
  inside the inner body too. Shadowing needs no special handling - the
  replacement is identical either way.
- A lambda-valued parameter may be CALLED: LAMBDA(f, f(1)) stores
  _xlpm.f(1), so a following '(' does not suppress the prefix.

Matching is case-insensitive with case-preserving replacement (Excel
identifiers are case-insensitive; occurrences keep the case as typed).
Double-quoted strings and single-quoted sheet names are never touched.
Occurrence boundaries exclude neighbors that would mean the token is
not a plain name: identifier characters, '.', '$' (absolute refs),
'!' (sheet qualifiers).

REFUSED loudly, with guidance:
- Optional parameters (LAMBDA(x,[y],...)): their stored form has not
  been harvested; guessing is how repair dialogs happen.
- Malformed constructs (LAMBDA with fewer than two arguments, LET with
  an even argument count or fewer than three).
- Declared names that are not legal identifiers, collide with the
  LAMBDA/LET keywords, or claim the _xl storage namespace.

Run BEFORE prefix_future_functions: a parameter unwisely named after a
future function must become _xlpm.FILTER before the call-prefixer
walks the text (its lookbehind then skips the '.'-preceded name).
Idempotent for the same reason - already-prefixed names are skipped.
"""

import re


# A declared name: same character class as Excel defined names.
xlpm_identifier_rgx = re.compile(r'^[A-Za-z_\\][A-Za-z0-9_.\\]*$')

# Names Excel itself refuses: A1-style cell-reference lookalikes and
# R1C1-style ones (including bare R and C). Prefixing occurrences of a
# name like 'A1' would also hit real cell references in-scope, so these
# must be refused, not transformed.
xlpm_cell_lookalike_rgx = re.compile(
    r'^(?:\$?[A-Za-z]{1,3}\$?\d{1,7}|[RC]|R\d+C\d+)$', re.IGNORECASE
)

# Head of a construct at a scan position (boundary checked by caller).
xlpm_construct_head_rgx = re.compile(r'(LAMBDA|LET)\s*\(', re.IGNORECASE)

# Characters that, adjacent to a token, mean it is not a plain name.
_BOUNDARY_CHARS = set('ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                      'abcdefghijklmnopqrstuvwxyz'
                      '0123456789_.\\$!')


def transform_xlpm_names(formula: str) -> str:
    """Rewrite LAMBDA/LET declared names into Excel's _xlpm. storage form."""
    if not isinstance(formula, str) or not formula:
        return formula
    return _rewrite_span(formula, ())


def parse_lambda_parameters(formula: str) -> list:
    """The declared parameter names of the FIRST top-level LAMBDA in text.

    For cross-checking hand-authored YAML 'parameters' lists against the
    definition text, which is the single source of truth.
    """
    if not isinstance(formula, str):
        return []
    index = 0
    length = len(formula)
    while index < length:
        char = formula[index]
        if char in '"\'':
            index = _quote_end(formula, index)
            continue
        head = _construct_at(formula, index)
        if head and head[0].upper() == 'LAMBDA':
            open_paren = head[1]
            close_paren = _matching_paren(formula, open_paren)
            args = _split_args(formula[open_paren + 1:close_paren])
            if len(args) < 2:
                return []
            return [arg.strip() for arg in args[:-1]]
        index += 1
    return []


# ----------------------------------------------------------------------
# Scanner internals
# ----------------------------------------------------------------------

def _rewrite_span(text: str, active_names: tuple) -> str:
    """Walk one span: prefix active names in plain runs, recurse constructs."""
    pieces = []
    plain_run = []
    index = 0
    length = len(text)

    def flush_plain():
        if plain_run:
            pieces.append(_prefix_names_in_plain(''.join(plain_run), active_names))
            plain_run.clear()

    while index < length:
        char = text[index]

        if char in '"\'':
            flush_plain()
            end = _quote_end(text, index)
            pieces.append(text[index:end])
            index = end
            continue

        head = _construct_at(text, index)
        if head:
            keyword, open_paren = head
            close_paren = _matching_paren(text, open_paren)
            args = _split_args(text[open_paren + 1:close_paren])
            flush_plain()
            pieces.append(_rewrite_construct(keyword, args, active_names))
            index = close_paren + 1
            continue

        plain_run.append(char)
        index += 1

    flush_plain()
    return ''.join(pieces)


def _rewrite_construct(keyword: str, args: list, active_names: tuple) -> str:
    """One LAMBDA or LET: validate declarations, apply the scoping rules."""
    kind = keyword.upper()

    if kind == 'LAMBDA':
        if len(args) < 2:
            raise ValueError(
                "LAMBDA needs at least one parameter and a body, got "
                f"{len(args)} argument(s)"
            )
        declared = []
        declaration_parts = []
        for raw in args[:-1]:
            name = _validated_declaration(raw, 'LAMBDA parameter')
            declared.append(name)
            declaration_parts.append(_prefix_declaration(raw, name))
        body = _rewrite_span(args[-1], active_names + tuple(declared))
        return f"{keyword}({','.join(declaration_parts + [body])})"

    # LET: name,value pairs then the body - names bind SEQUENTIALLY
    if len(args) < 3 or len(args) % 2 == 0:
        raise ValueError(
            "LET needs name,value pairs followed by a body (an odd count "
            f"of at least three arguments), got {len(args)}"
        )
    parts = []
    in_scope = list(active_names)
    for pair_index in range(0, len(args) - 1, 2):
        raw_name = args[pair_index]
        name = _validated_declaration(raw_name, 'LET name')
        # The value expression sees names declared BEFORE this pair only
        parts.append(_prefix_declaration(raw_name, name))
        parts.append(_rewrite_span(args[pair_index + 1], tuple(in_scope)))
        in_scope.append(name)
    parts.append(_rewrite_span(args[-1], tuple(in_scope)))
    return f"{keyword}({','.join(parts)})"


def _validated_declaration(raw: str, what: str) -> str:
    """A declaration slot's BARE name, or a guided refusal.

    An already-prefixed declaration (_xlpm.v) is prior output of this
    transformer - the construct keyword itself carries no prefix until
    the future-function pass, so a re-run re-enters here. Strip and
    accept it; _prefix_declaration leaves such slots untouched. This is
    what makes the transformer idempotent.
    """
    name = raw.strip()
    if name.startswith('_xlpm.'):
        name = name[len('_xlpm.'):]
    if name.startswith('['):
        raise ValueError(
            f"Optional {what} {name!r} is not supported: the stored form "
            f"of bracketed optional parameters has not been harvested from "
            f"real Excel output, and guessing storage grammar is how repair "
            f"dialogs happen. Use a required parameter with an ISOMITTED-"
            f"free design, or harvest the form first."
        )
    if not xlpm_identifier_rgx.match(name):
        raise ValueError(
            f"{what} {name!r} is not a legal name (letters, digits, "
            f"underscore, period; must not start with a digit)"
        )
    if name.upper() in ('LET', 'LAMBDA'):
        raise ValueError(f"{what} {name!r} collides with the construct keywords")
    if name.upper() in ('TRUE', 'FALSE'):
        raise ValueError(f"{what} {name!r} collides with the boolean literals")
    if xlpm_cell_lookalike_rgx.match(name):
        raise ValueError(
            f"{what} {name!r} looks like a cell reference, which Excel "
            f"forbids as a name - and prefixing its occurrences would hit "
            f"real references in scope"
        )
    if name.upper().startswith('_XL'):
        raise ValueError(f"{what} {name!r} claims Excel's _xl storage namespace")
    return name


def _prefix_declaration(raw: str, name: str) -> str:
    """Prefix the declared name in place, preserving surrounding whitespace."""
    if f'_xlpm.{name}' in raw:
        return raw  # Already this transformer's output (idempotent re-run)
    return raw.replace(name, f'_xlpm.{name}', 1)


def _prefix_names_in_plain(text: str, names: tuple) -> str:
    """Prefix every active-name occurrence in quote-free text."""
    if not names or not text:
        return text
    alternation = '|'.join(
        re.escape(name) for name in sorted(set(names), key=len, reverse=True)
    )
    occurrence_rgx = re.compile(
        rf'(?<![A-Za-z0-9_.\\$!])({alternation})(?![A-Za-z0-9_.\\$!])',
        re.IGNORECASE
    )
    return occurrence_rgx.sub(lambda match: '_xlpm.' + match.group(1), text)


def _construct_at(text: str, index: int):
    """(keyword, open_paren_index) if a construct starts here, else None."""
    if index > 0 and text[index - 1] in _BOUNDARY_CHARS:
        return None
    match = xlpm_construct_head_rgx.match(text, index)
    if not match:
        return None
    return match.group(1), match.end() - 1


def _quote_end(text: str, start: int) -> int:
    """Index just past a quoted literal, honoring doubled-quote escapes."""
    quote = text[start]
    index = start + 1
    length = len(text)
    while index < length:
        if text[index] == quote:
            if index + 1 < length and text[index + 1] == quote:
                index += 2
                continue
            return index + 1
        index += 1
    return length  # Unterminated: treat the rest as literal


def _matching_paren(text: str, open_paren: int) -> int:
    """Index of the parenthesis closing text[open_paren], quote-aware."""
    depth = 0
    index = open_paren
    length = len(text)
    while index < length:
        char = text[index]
        if char in '"\'':
            index = _quote_end(text, index)
            continue
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError("Unbalanced parentheses in formula")


def _split_args(inner: str) -> list:
    """Split on top-level commas: paren/brace/bracket depth and quote aware."""
    args = []
    depth_paren = depth_brace = depth_bracket = 0
    start = 0
    index = 0
    length = len(inner)
    while index < length:
        char = inner[index]
        if char in '"\'':
            index = _quote_end(inner, index)
            continue
        if char == '(':
            depth_paren += 1
        elif char == ')':
            depth_paren -= 1
        elif char == '{':
            depth_brace += 1
        elif char == '}':
            depth_brace -= 1
        elif char == '[':
            depth_bracket += 1
        elif char == ']':
            depth_bracket -= 1
        elif char == ',' and depth_paren == depth_brace == depth_bracket == 0:
            args.append(inner[start:index])
            start = index + 1
        index += 1
    args.append(inner[start:])
    return args

# End of file #
