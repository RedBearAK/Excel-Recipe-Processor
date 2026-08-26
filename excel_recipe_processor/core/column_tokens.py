"""
Column-name references in text channels: one delimited grammar, no
recognition, ever.

excel_recipe_processor/core/column_tokens.py

THE RULING (2026-08-26): raw python with bare column names in recipe
formula: lines was the original design fault - two namespaces in one
undelimited string, forcing the engine to GUESS which words are
columns. Every incident in the family (hash truncation, literal
corruption, a column formatted as position 3,904, method calls
rewritten by a column named sum) was interest on that decision.

The grammar now matches the house convention that has a zero-incident
record in conditional_format and inject_formulas: a column reference
is ALWAYS {col:Column Name}. Uniform for every column - a column
named sum is {col:sum} exactly as Price is {col:Price} - so there is
no collision concept and no disambiguation burden. Bare identifiers
in code are always python. The engine parses delimiters; it never
recognizes names in free text. A bare known column name found loose
in code is refusal-time forensics only: a guided error naming the
{col:} fix, never a substitution.

Name hygiene at the boundary: names containing braces, backticks, or
newlines are refused loudly, never escaped.
"""


class ColumnTokenError(Exception):
    """Raised for malformed formulas or hostile column names."""


FORBIDDEN_IN_NAMES = ('`', '{', '}', '\n', '\r')


def validate_column_name(name: str, context: str = '') -> str:
    """Refuse names that could ever collide with a delimiter."""
    for forbidden in FORBIDDEN_IN_NAMES:
        if forbidden in name:
            shown = forbidden.replace('\n', '\\n').replace('\r', '\\r')
            raise ColumnTokenError(
                f"Column name {name!r} contains {shown!r}, which collides "
                f"with reference delimiters and is refused"
                f"{' (' + context + ')' if context else ''}. Rename the "
                f"column before it enters any formula channel."
            )
    return name


def tokenize_formula(formula: str) -> list:
    """One linear scan into typed segments.

    Returns (kind, text) pairs: 'code', 'literal' (raw, quotes
    included), or 'column' (the name from a {col:Name} placeholder).
    Unterminated literals or placeholders are hard errors - a formula
    that cannot be parsed cleanly must not be guessed at.
    """
    segments = []
    code_start = 0
    i = 0
    n = len(formula)

    def flush_code(end):
        if end > code_start:
            segments.append(('code', formula[code_start:end]))

    while i < n:
        ch = formula[i]
        if ch in ('"', "'"):
            quote = ch
            j = i + 1
            while j < n:
                if formula[j] == '\\':
                    j += 2
                    continue
                if formula[j] == quote:
                    break
                j += 1
            if j >= n:
                raise ColumnTokenError(
                    f"Unterminated string literal starting at position "
                    f"{i}: {formula[i:i + 30]!r}..."
                )
            flush_code(i)
            segments.append(('literal', formula[i:j + 1]))
            i = j + 1
            code_start = i
            continue
        if formula.startswith('{col:', i):
            j = formula.find('}', i + 5)
            if j < 0:
                raise ColumnTokenError(
                    f"Unterminated column reference at position {i}: "
                    f"{formula[i:i + 30]!r}... References are written "
                    f"{{col:Column Name}} with a closing brace."
                )
            name = formula[i + 5:j]
            if not name.strip():
                raise ColumnTokenError(
                    f"Empty column reference {{col:}} at position {i}"
                )
            flush_code(i)
            segments.append(('column', name))
            i = j + 1
            code_start = i
            continue
        i += 1
    flush_code(n)
    return segments


def build_dataframe_expression(formula: str, column_names,
                               frame_symbol: str = 'df') -> str:
    """Translate {col:Name} references into dataframe accesses.

    Code segments pass through UNTOUCHED except for refusal-time
    forensics: a bare known column name loose in code is a guided
    error naming the {col:} form - never a substitution. Literals are
    structurally separate and never inspected.
    """
    import re

    known = [str(name) for name in column_names]
    known_set = set(known)

    pieces = []
    for kind, text in tokenize_formula(formula):
        if kind == 'literal':
            pieces.append(text)
        elif kind == 'column':
            validate_column_name(text)
            if text not in known_set:
                close = [k for k in known if text.lower() in k.lower()
                         or k.lower() in text.lower()][:4]
                hint = f" Near matches: {close}" if close else ''
                raise ColumnTokenError(
                    f"{{col:{text}}} names no column in the current "
                    f"data.{hint}"
                )
            pieces.append(f"{frame_symbol}[{text!r}]")
        else:
            # Bare identifiers are ALWAYS python (the 2026-08-26
            # ruling) - code passes through untouched. Unmigrated
            # column references surface as eval NameErrors, where
            # name_error_guidance() turns them into {col:} guidance
            # with zero false positives.
            pieces.append(text)
    return ''.join(pieces)


def name_error_guidance(error, column_names) -> str:
    """Turn a NameError over a known column into {col:} guidance.

    Returns a guidance string when the missing name matches a column
    (exactly, or as the first word of a multi-word name - python
    reads 'Major Species' as the identifier Major), else ''.
    """
    message = str(error)
    import re
    match = re.search(r"name '([^']+)' is not defined", message)
    if not match:
        return ''
    missing = match.group(1)
    exact = [c for c in column_names if str(c) == missing]
    prefixed = [c for c in column_names
                if str(c).split(' ')[0] == missing and ' ' in str(c)]
    for candidate in exact + prefixed:
        return (f"Bare name {missing!r} looks like the column "
                f"{str(candidate)!r} - column references are written "
                f"{{col:{candidate}}} so names can never be misread "
                f"as code.")
    return ''


def formula_failure_guidance(formula: str, column_names) -> str:
    """Refusal-time forensics for an ALREADY-FAILED formula.

    Scans code segments for bare known-column occurrences and names the
    {col:} fix. Runs only after eval has failed, so it can never accept
    or rewrite anything - it only improves the epitaph.
    """
    import re
    try:
        segments = tokenize_formula(formula)
    except ColumnTokenError:
        return ''
    hits = []
    for kind, text in segments:
        if kind != 'code':
            continue
        for name in column_names:
            name = str(name)
            if not name.strip():
                continue
            pattern = r'(?<![\w.])' + re.escape(name) + r'(?![\w])'
            if re.search(pattern, text):
                hits.append(name)
    if not hits:
        return ''
    worst = sorted(set(hits), key=len, reverse=True)[0]
    return (f"Bare column name(s) {sorted(set(hits), key=len, reverse=True)[:4]} "
            f"in the formula - column references are written "
            f"{{col:{worst}}} so names can never be misread as code.")



# End of file #
