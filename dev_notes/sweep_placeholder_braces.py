#!/usr/bin/env python3
"""
Sweep a recipe, example, test or doc from single-brace placeholders to
the doubled form: {col:X} -> {{col:X}}, {name} -> {{name}}, and so on.

dev_notes/sweep_placeholder_braces.py  (Excel-Recipe-Processor)

    python3 dev_notes/sweep_placeholder_braces.py FILE [FILE ...]
    python3 dev_notes/sweep_placeholder_braces.py --check FILE [FILE ...]

Written for the 2026-11-02 delimiter change. Rules:

- Only identifier-first single-brace tokens are touched: {word},
  {word:anything}. A regex quantifier {2,4}, an Excel array constant
  {1,2,3} or {"a","b"}, a dict literal {'a': 1} and an empty {} are left
  alone - they are exactly what the new delimiter exists to permit.
- Already-doubled tokens are left alone, so the sweep is idempotent.
- In a .py file, f-strings are skipped (a brace inside one is already an
  escape); those few are fixed by hand and listed in the report. Every
  other string, comment and docstring is swept.
- The file is rewritten in place. Before writing, the result is checked
  against the original with every brace run collapsed to one: the two
  must be identical, which proves the sweep changed braces and nothing
  else. --check reports what would change without writing.

After sweeping a recipe, run --validate on it: the retired form is fatal
there, so a token this script did not recognize names itself.
"""

import os
import re
import sys


single_token_rgx = re.compile(r'(?<!\{)\{(?!\{)([a-zA-Z_]\w*)(:[^{}\n]*)?\}(?!\})')
fstring_prefix_rgx = re.compile(r'(?i)^[rb]?f[rb]?["\']')
brace_run_rgx = re.compile(r'\{+|\}+')


def double(match) -> str:
    return '{{' + match.group(1) + (match.group(2) or '') + '}}'


def sweep_text(text: str) -> str:
    return single_token_rgx.sub(double, text)


def sweep_python(text: str):
    """Sweep ONLY string and comment tokens; return (text, fstring_lines).

    A line-based sweep is not safe in python: a dict comprehension
    {name: expr for ...} has exactly the shape of a placeholder, and the
    first version of this script doubled 29 of them across the package
    before the tokenize audit caught it (2026-11-02). So the file is
    tokenized and only STRING and COMMENT tokens are rewritten; code is
    untouchable by construction. f-strings are left alone and their
    lines reported: a brace inside one is already an escape, so a
    placeholder spelled there needs four braces, by hand.
    """
    import io
    import tokenize

    edits = []
    fstring_lines = []
    fstring_middle = getattr(tokenize, 'FSTRING_MIDDLE', -1)
    for tok in tokenize.generate_tokens(io.StringIO(text).readline):
        if tok.type == tokenize.COMMENT or tok.type == tokenize.STRING:
            if tok.type == tokenize.COMMENT and re.search(r"""\bf["']""", tok.string):
                # commented-out code holding an f-string: its braces are
                # interpolations, not placeholders - leave the fossil alone
                continue
            if tok.type == tokenize.STRING and fstring_prefix_rgx.match(tok.string):
                if single_token_rgx.search(tok.string):
                    fstring_lines.append(tok.start[0])
                continue
            swept = sweep_text(tok.string)
            if swept != tok.string:
                edits.append((tok.start, tok.end, swept))
        elif tok.type == fstring_middle:
            # python 3.12 tokenizes f-string bodies separately; report, never touch
            if single_token_rgx.search(tok.string):
                fstring_lines.append(tok.start[0])
    if not edits:
        return text, sorted(set(fstring_lines))
    lines = text.split('\n')
    # apply from the end so earlier offsets stay valid
    for (sl, sc), (el, ec), swept in sorted(edits, reverse=True):
        before = lines[sl - 1][:sc]
        after = lines[el - 1][ec:]
        lines[sl - 1:el] = (before + swept + after).split('\n')
    return '\n'.join(lines), sorted(set(fstring_lines))


def collapsed(text: str) -> str:
    return brace_run_rgx.sub(lambda m: m.group(0)[0], text)


def sweep_file(path: str, write: bool) -> int:
    original = open(path, encoding='utf-8').read()
    skipped = []
    if path.endswith('.py'):
        swept, skipped = sweep_python(original)
    else:
        swept = sweep_text(original)
    if collapsed(swept) != collapsed(original):
        print(f"  REFUSED {path}: the sweep would change more than braces")
        return 1
    changed = len(single_token_rgx.findall(original)) - len(single_token_rgx.findall(swept))
    note = f"; f-string lines left for hand editing: {skipped}" if skipped else ''
    if swept == original:
        print(f"  unchanged  {path}{note}")
        return 0
    print(f"  {'swept' if write else 'would sweep'} {changed:>4} token(s)  {path}{note}")
    if write:
        open(path, 'w', encoding='utf-8').write(swept)
    return 0


def main() -> int:
    arguments = [a for a in sys.argv[1:] if not a.startswith('--')]
    write = '--check' not in sys.argv
    if not arguments:
        print(__doc__)
        return 1
    failures = 0
    for path in arguments:
        if not os.path.isfile(path):
            print(f"  missing {path}")
            failures += 1
            continue
        failures += sweep_file(path, write)
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())


# End of file #
