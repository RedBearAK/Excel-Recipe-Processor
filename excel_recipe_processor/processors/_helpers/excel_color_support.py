"""
Color normalization shared by the formatting processors.

excel_recipe_processor/processors/_helpers/excel_color_support.py

One color vocabulary everywhere a recipe names a color: hex with or
without # (FF0000, #F00), CSS names (red, forestgreen), and rgb(255,0,0).
Extracted from format_excel_processor so conditional_format speaks the
identical language; a color that works in a header option works in a
conditional-formatting rule, byte for byte.

Deliberately NOT parent-prefixed like other helper modules: it serves
format_excel and conditional_format equally, and a shared vocabulary
belongs to neither.

(The previous in-processor version wrapped its webcolors use in a
try/except ImportError that could never fire - the package imports
webcolors unguarded at module scope and requirements.txt declares it a
hard dependency. The dead guard is not carried forward.)
"""

import webcolors


def normalize_color(color) -> str:
    """
    Normalize a color to 6-digit uppercase hex (no #).

    Args:
        color: Hex with/without # (3 or 6 digit), CSS name, or rgb(r,g,b)

    Returns:
        Six uppercase hex digits, e.g. 'FF0000'

    Raises:
        ValueError: On None, non-string, empty, or unrecognized formats
    """
    if color is None:
        raise ValueError("Color cannot be None")

    if not isinstance(color, str):
        # Fail loud on ints and everything else. Unquoted hex in YAML is a
        # corruption trap: header_text_color: 000123 parses as the int 123,
        # the leading zeros vanish, and str() coercion would bless a
        # plausible wrong color ('112233'). Quoted strings only.
        raise ValueError(
            f"Color must be a string, got {type(color).__name__}: {color!r}. "
            f"Quote hex colors in YAML, e.g. '000123', so leading zeros survive."
        )

    color_text = color.strip()
    if not color_text:
        raise ValueError("Color cannot be empty")

    # rgb(255, 0, 0)
    if color_text.lower().startswith('rgb(') and color_text.endswith(')'):
        try:
            parts = [int(part.strip()) for part in color_text[4:-1].split(',')]
        except (ValueError, TypeError):
            raise ValueError(f"Invalid RGB format '{color_text}'")
        if len(parts) != 3:
            raise ValueError(f"RGB format must have exactly 3 values: '{color_text}'")
        for value in parts:
            if not 0 <= value <= 255:
                raise ValueError(f"RGB values must be 0-255: '{color_text}'")
        return '%02X%02X%02X' % tuple(parts)

    # CSS color name
    try:
        return webcolors.name_to_hex(color_text.lower())[1:].upper()
    except ValueError:
        pass  # not a CSS name; fall through to hex

    return _normalize_hex(color_text)


def _normalize_hex(color_text: str) -> str:
    """Normalize a 3- or 6-digit hex string, with or without leading #."""
    cleaned = color_text[1:] if color_text.startswith('#') else color_text

    if not cleaned:
        raise ValueError("Color cannot be just '#'")

    hex_digits = set('0123456789ABCDEFabcdef')
    if any(character not in hex_digits for character in cleaned):
        raise ValueError(
            f"Unrecognized color format: '{color_text}'. Supported formats: "
            f"hex (#FF0000), CSS names (red, blue), RGB (rgb(255,0,0))"
        )

    if len(cleaned) == 3:
        cleaned = ''.join(character * 2 for character in cleaned)

    if len(cleaned) != 6:
        raise ValueError(f"Hex color must be 3 or 6 digits, got '{color_text}'")

    return cleaned.upper()

# End of file #
