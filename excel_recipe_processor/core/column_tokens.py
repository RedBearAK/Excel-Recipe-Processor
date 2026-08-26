"""
Column-name tokenization: names as explicit typed tokens, never guessed.

excel_recipe_processor/core/column_tokens.py

The 2026-08 incident family (hash-truncated formulas, literal
corruption, a data column formatted as Excel position 3,904, method
calls rewritten by a column named 'sum') shared one root: column names
travelled as bare strings through channels carrying OTHER string-typed
things, and every consumer disambiguated by pattern-matching. This
module ends the class:

- A formula is parsed ONCE, left to right, into typed segments:
  code, string literal, or backticked column token. Nothing is ever
  "recognized" inside free text.
- `Column Name` (backticks) is the ONLY way a column enters a formula.
  The token translates to a dataframe reference at exactly one
  emission point.
- A bare occurrence of a known column name inside a code segment is a
  HARD ERROR with migration guidance - the old ambiguity is not merely
  handled, it is unrepresentable.
- Name hygiene is enforced at the boundary: names containing
  backticks, braces, or newlines are refused loudly, never escaped.
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
                f"with token delimiters and is refused"
                f"{' (' + context + ')' if context else ''}. Rename the "
                f"column before it enters any formula channel."
            )
    return name


def tokenize_formula(formula: str) -> list:
    """One linear scan into typed segments.

    Returns a list of (kind, text) pairs where kind is 'code',
    'literal' (raw, quotes included), or 'column' (the bare name,
    backticks stripped). Unterminated literals or column tokens are
    hard errors - a formula that cannot be parsed cleanly must not be
    guessed at.
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
                    f"Unterminated string literal starting at position {i}: "
                    f"{formula[i:i + 30]!r}..."
                )
            flush_code(i)
            segments.append(('literal', formula[i:j + 1]))
            i = j + 1
            code_start = i
            continue
        if ch == '`':
            j = formula.find('`', i + 1)
            if j < 0:
                raise ColumnTokenError(
                    f"Unterminated column token starting at position {i}: "
                    f"{formula[i:i + 30]!r}... Column names are written "
                    f"`Like This` with a closing backtick."
                )
            name = formula[i + 1:j]
            if not name.strip():
                raise ColumnTokenError(
                    f"Empty column token `` at position {i}"
                )
            flush_code(i)
            segments.append(('column', name))
            i = j + 1
            code_start = i
            continue
        i += 1
    flush_code(n)
    return segments


def bare_name_collisions(code_text: str, column_names) -> list:
    """Known column names appearing bare inside a code segment.

    Word-boundary containment check per name; the caller turns any hit
    into a guided error. This scan GUARDS the strict grammar - it is
    not a recognition mechanism, and nothing is rewritten from it.
    """
    import re
    hits = []
    for name in column_names:
        if not name or not str(name).strip():
            continue
        # A name immediately after a dot is attribute/method syntax
        # (.sum() beside a column named sum) - never column intent
        pattern = r'(?<![\w`.])' + re.escape(str(name)) + r'(?![\w`])'
        if re.search(pattern, code_text):
            hits.append(str(name))
    return hits


def build_dataframe_expression(formula: str, column_names,
                               frame_symbol: str = 'df') -> str:
    """Translate a backtick-grammar formula into an eval-ready string.

    - `Name` tokens become df['Name'] (repr-quoted, so quotes inside
      names are safe).
    - Literals pass through untouched.
    - Code segments are checked for bare column names: any hit is a
      hard error demanding backticks, with the offenders listed.
    - Unknown column tokens error with the available-name list nearby.
    """
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
                    f"Column token `{text}` names no column in the "
                    f"current data.{hint}"
                )
            pieces.append(f"{frame_symbol}[{text!r}]")
        else:
            hits = bare_name_collisions(text, known)
            if hits:
                shown = ', '.join(f"`{h}`" for h in sorted(hits, key=len,
                                                           reverse=True)[:5])
                raise ColumnTokenError(
                    f"Bare column name(s) in formula code: "
                    f"{sorted(hits, key=len, reverse=True)[:5]}. Column "
                    f"names must be backticked ({shown}) so they can "
                    f"never be misread as code. Offending segment: "
                    f"{text.strip()[:60]!r}"
                )
            pieces.append(text)
    return ''.join(pieces)

# End of file #
