"""
Hyperlink target construction for format_excel column rules.

excel_recipe_processor/processors/_helpers/format_excel_hyperlink_utils.py

A make_hyperlinks column rule declares what the bare cell text IS - a file
path, a web URL, or an email address - and this module turns that text into
the target string stored in the real cell.hyperlink relationship. The
declaration is explicit because a formatter sniffing cell content to guess
between a path and a URL is implicit behavior, and implicit behavior is how
links end up wrong in ways nobody notices until click time.

Kept separate from the column-formats module so the per-kind validation
rules stay small, testable on plain strings, and away from openpyxl.
"""

from urllib.parse import quote


HYPERLINK_KINDS = ('file_paths', 'web_urls', 'email_addresses')

# Excel's own hyperlink presentation: this blue, underlined
DEFAULT_HYPERLINK_COLOR = '0563C1'


class HyperlinkTargetError(ValueError):
    """A cell value cannot become a valid target for its declared kind."""
    pass


def _has_whitespace(text: str) -> bool:
    """True when any character in text is whitespace."""
    for character in text:
        if character.isspace():
            return True
    return False


def build_hyperlink_target(kind: str, cell_text: str) -> str:
    """
    Turn bare cell text into a hyperlink target for the declared kind.

    Values already carrying their scheme (file://, ://, mailto:) pass
    through untouched. Everything else is validated against the declared
    kind and prefixed - loudly refusing values that would make a link
    that is wrong in a quiet way (relative paths, backslash paths,
    whitespace in URLs).

    Args:
        kind:       One of HYPERLINK_KINDS
        cell_text:  The bare cell value, already known to be a
                    non-blank string

    Returns:
        The target string to store on cell.hyperlink

    Raises:
        HyperlinkTargetError: value cannot become a valid target
    """
    if kind not in HYPERLINK_KINDS:
        legal = ', '.join(HYPERLINK_KINDS)
        raise HyperlinkTargetError(
            f"make_hyperlinks must be one of: {legal}. Got: {kind!r}"
        )

    text = cell_text.strip()

    if kind == 'file_paths':
        if text.startswith('file://'):
            return text
        if '\\' in text:
            raise HyperlinkTargetError(
                f"{text!r} contains backslashes; file_paths expects "
                f"absolute POSIX paths like /Users/name/file.xlsx"
            )
        if not text.startswith('/'):
            raise HyperlinkTargetError(
                f"{text!r} is not an absolute path; a file:// link has no "
                f"working directory to anchor a relative path to, so the "
                f"link would be quietly wrong. Supply the absolute path."
            )
        # Percent-encode spaces and specials; keep / as the separator
        return 'file://' + quote(text, safe='/')

    if kind == 'web_urls':
        if '://' in text:
            return text
        if _has_whitespace(text):
            raise HyperlinkTargetError(
                f"{text!r} contains whitespace and cannot be a URL"
            )
        # Scheme-less gets https:// - the same assumption Excel itself
        # makes, and virtually everything is and should be https now
        return 'https://' + text

    # email_addresses
    if text.startswith('mailto:'):
        return text
    if '@' not in text or _has_whitespace(text):
        raise HyperlinkTargetError(
            f"{text!r} is not an email address (needs an @, no spaces)"
        )
    return 'mailto:' + text


# End of file #
