"""
Workbook theme and custom pivot style support for format_excel.

excel_recipe_processor/processors/_helpers/format_excel_theme_manager.py

Two separate mechanisms, often confused, handled here together because a
recipe usually wants both:

1. THE THEME (xl/theme/theme1.xml) holds six accent colours. Every entry in
   Excel's PivotTable and Table style galleries is defined against those
   accent SLOTS rather than literal colours, so the same gallery style shows
   different colours in two workbooks with different themes. openpyxl bundles
   a theme frozen at the Office 2007 palette, which is why generated files
   look more muted than files Excel wrote.

2. THE DEFAULT PIVOT STYLE is just a NAME in styles.xml (defaultPivotStyle).
   Excel ships 84 built-in gallery styles, and naming one costs nothing -
   no theme, no style definition. Excel's own default is PivotStyleLight16,
   the blue swatch; the gallery runs seven per row with positions 2-7
   mapping to accent1-accent6, so PivotStyleLight19 is the purple swatch in
   that same row. That is all it takes for a user's new pivot to come up
   purple instead of blue.

3. A CUSTOM PIVOT STYLE lives in styles.xml as a tableStyle whose elements
   point at differential formats (dxf). This is what controls a specific
   header fill, header font colour, and bold subtotal / grand total rows -
   none of which a built-in name or the theme can express.

Which built-in name is purple depends on the accents in the file, since
gallery styles resolve through theme slots. Generated files carry
openpyxl's bundled palette, where accent4 is 8064A2 - purple - so
PivotStyleLight19 is stable and predictable. Injecting a theme (1) can
move that; the two features are independent on purpose.

Neither mechanism touches explicit cell formatting. Colours a recipe applies
directly (header fills, fonts) are literal RGB and are unaffected by a theme
change - only theme-referencing gallery styles shift.
"""

import zipfile
import logging

from pathlib import Path

from openpyxl.writer.theme import theme_xml
from openpyxl.styles import Font, PatternFill, Side, Border
from openpyxl.styles.table import TableStyle, TableStyleElement, TableStyleList
from openpyxl.styles.differential import DifferentialStyle

from excel_recipe_processor.processors._helpers.format_excel_theme_modern import (
    MODERN_OFFICE_THEME_XML,
)
from excel_recipe_processor.processors._helpers.format_excel_theme_rgx import (
    accent_slot_rgx,
    theme_part_rgx,
    theme_name_rgx,
    hex_color_rgx,
)


logger = logging.getLogger(__name__)


class ThemeManagerError(Exception):
    """Raised when a theme or pivot style cannot be built or applied."""
    pass


# Built-in accent palettes, accent1 through accent6.
#
# "purple" is the ERP signature: the same structure as the blue palette
# Excel defaults to, rotated to a purple lead so a generated file is
# visibly not carrying Excel's stock theme. "office_modern" reproduces the
# palette current Excel writes; "office_legacy" is the muted 2007 set that
# openpyxl bundles, offered for anyone who wants the old look back.
THEME_PRESETS = {
    'purple': ('7030A0', 'B23FA0', '9E4FD1', '5B2C88', 'C77DD8', '4A2069'),
    'office_modern': ('156082', 'E97132', '196B24', '0F9ED5', 'A02B93', '4EA72E'),
    'office_legacy': ('4F81BD', 'C0504D', '9BBB59', '8064A2', '4BACC6', 'F79646'),
}

DEFAULT_PRESET = 'purple'

# Excel's own default; the blue swatch in the gallery's third row
# (position 2 of that row, which maps to accent1).
EXCEL_DEFAULT_PIVOT_STYLE = 'PivotStyleLight16'

# The ERP signature: the purple swatch in that same row. The gallery runs
# seven per row with positions 2-7 mapping to accent1-accent6, so the name
# depends on WHICH accent is purple - and that moved when the modern theme
# became the base for constructed files: accent4 is 8064A2 (purple) in the
# legacy palette but 0F9ED5 (cyan) in the modern one, where A02B93 at
# accent5 is the purple. Hence Light20, four swatches right of the blue
# default rather than three.
#
# A recipe that injects a DIFFERENT palette via workbook_theme may move
# purple again; such a recipe should name its own default_pivot_style.
ERP_DEFAULT_PIVOT_STYLE = 'PivotStyleLight20'

# Pivot style elements that take the "bold" differential format when
# bold_subtotals / bold_grand_totals are requested. Excel addresses subtotal
# rows by nesting level, so all three levels are covered.
SUBTOTAL_ELEMENTS = ('firstSubtotalRow', 'secondSubtotalRow', 'thirdSubtotalRow')
GRAND_TOTAL_ELEMENTS = ('totalRow', 'lastColumn')


def normalize_hex_color(value, context='colour', color_normalizer=None):
    """
    Return six upper-case hex digits, or raise.

    Accepts "1F4E79" or "#1F4E79" directly. Anything else is handed to the
    caller's normalizer first, so these directives accept exactly the same
    colour NAMES ("white", "red", CSS names) the rest of format_excel does -
    one dialect across the processor, not two.
    """
    if not isinstance(value, str):
        raise ThemeManagerError(f"{context} must be a colour string, got {type(value).__name__}")

    match = hex_color_rgx.match(value.strip())

    if match:
        return match.group(1).upper()

    if color_normalizer is not None:
        try:
            resolved = color_normalizer(value)
        except Exception as error:
            raise ThemeManagerError(f"{context}: {error}")

        resolved_match = hex_color_rgx.match(str(resolved).strip().lstrip('#')[-6:])
        if resolved_match:
            return resolved_match.group(1).upper()

    raise ThemeManagerError(
        f"{context} must be a hex colour like '1F4E79' or a known colour name, got '{value}'"
    )


def extract_theme_from_file(file_path):
    """
    Pull the theme XML out of any OOXML package.

    Works on .xlsx, .xlsm, .xltx, .pptx, .docx and .thmx alike - they all
    carry a theme part, just under different folder names. The first theme
    part found wins, which is the primary theme in every package Excel or
    PowerPoint writes.

    Args:
        file_path: Path to an OOXML file to borrow a theme from

    Returns:
        Theme XML as bytes

    Raises:
        ThemeManagerError: If the file is unreadable or carries no theme
    """
    path = Path(file_path)

    if not path.exists():
        raise ThemeManagerError(f"Theme donor file not found: {file_path}")

    try:
        with zipfile.ZipFile(path) as package:
            theme_parts = [name for name in package.namelist() if theme_part_rgx.match(name)]

            if not theme_parts:
                raise ThemeManagerError(
                    f"No theme part in '{file_path}'. A theme donor must be an OOXML "
                    f"file (.xlsx, .xlsm, .xltx, .pptx, .docx or .thmx)."
                )

            theme_bytes = package.read(sorted(theme_parts)[0])

    except zipfile.BadZipFile:
        raise ThemeManagerError(
            f"Theme donor '{file_path}' is not a valid OOXML package. Legacy .xls "
            f"files carry no theme; re-save as .xlsx first."
        )

    name_match = theme_name_rgx.search(theme_bytes.decode('utf-8', 'ignore'))
    logger.info(
        f"🎨 Theme extracted from '{path.name}'"
        f"{f' (theme: {name_match.group(1)})' if name_match else ''}"
    )

    return theme_bytes


def build_theme_with_accents(base_theme_bytes, accent_colors, color_normalizer=None):
    """
    Return theme XML with its six accent slots replaced.

    Everything else in the donor theme - fonts, effects, background and text
    slots - is preserved, so the result is a valid theme that differs only in
    the colours gallery styles resolve against.

    Args:
        base_theme_bytes: Theme XML to start from
        accent_colors:    Six colours, accent1 through accent6

    Returns:
        Theme XML as bytes
    """
    if len(accent_colors) != 6:
        raise ThemeManagerError(
            f"accent_colors needs exactly 6 colours (accent1-accent6), got {len(accent_colors)}"
        )

    normalized = [
        normalize_hex_color(color, context=f"accent{index + 1}",
                            color_normalizer=color_normalizer)
        for index, color in enumerate(accent_colors)
    ]

    theme_text = base_theme_bytes.decode('utf-8')

    def substitute(match):
        slot_number = int(match.group(2))
        return f"{match.group(1)}{normalized[slot_number - 1]}{match.group(4)}"

    updated, count = accent_slot_rgx.subn(substitute, theme_text)

    if count != 6:
        raise ThemeManagerError(
            f"Expected 6 accent slots in the base theme, substituted {count}. "
            f"The donor theme may be malformed."
        )

    return updated.encode('utf-8')


def resolve_theme(workbook, theme_config, color_normalizer=None):
    """
    Work out the theme XML a workbook should carry, from its config.

    Exactly one source is used, in this precedence: an explicit donor file,
    then explicit accent colours, then a named preset, then the default
    preset. Accent colours and presets are applied over the workbook's
    CURRENT theme, so only the accents change.

    Args:
        workbook:     The openpyxl workbook being formatted
        theme_config: The workbook_theme mapping from the recipe

    Returns:
        Theme XML bytes, or None when the recipe opted out
    """
    # No workbook_theme in the recipe means NO theme change. Injecting a
    # palette recolours every gallery style in the file, which is too large
    # a side effect to happen by default; the purple signature is delivered
    # by the default pivot style name instead.
    if not theme_config:
        return None

    if theme_config.get('apply', True) is False:
        logger.info("🎨 workbook_theme apply: false - leaving the existing theme untouched")
        return None

    from_file = theme_config.get('from_file')
    accent_colors = theme_config.get('accent_colors')
    preset = theme_config.get('preset')

    sources_given = [name for name, value in
                     (('from_file', from_file), ('accent_colors', accent_colors), ('preset', preset))
                     if value]

    if len(sources_given) > 1:
        raise ThemeManagerError(
            f"workbook_theme takes ONE source, got {sources_given}. Use from_file to "
            f"copy a whole theme, accent_colors for explicit values, or preset for a "
            f"built-in palette."
        )

    if from_file:
        return extract_theme_from_file(from_file)

    # Accents are applied over whatever theme the workbook already has, so a
    # file that inherited a good theme keeps its fonts and effects.
    base_theme = _current_theme_bytes(workbook)

    if accent_colors:
        logger.info(f"🎨 Applying {len(accent_colors)} explicit accent colour(s) to the theme")
        return build_theme_with_accents(base_theme, accent_colors, color_normalizer)

    preset_name = preset or DEFAULT_PRESET

    if preset_name not in THEME_PRESETS:
        raise ThemeManagerError(
            f"Unknown theme preset '{preset_name}'. Available: {sorted(THEME_PRESETS)}"
        )

    logger.info(f"🎨 Applying the '{preset_name}' accent palette to the workbook theme")

    return build_theme_with_accents(base_theme, THEME_PRESETS[preset_name], color_normalizer)


def modern_base_theme_bytes():
    """The theme every workbook ERP constructs starts from."""
    return MODERN_OFFICE_THEME_XML.encode('utf-8')


def apply_base_theme(workbook):
    """
    Give a freshly constructed workbook the modern Office theme.

    Called by the writer for files ERP creates, so the modern palette is the
    floor rather than an option. Files ERP did not construct are left alone;
    a recipe can still restyle any workbook explicitly with workbook_theme.
    """
    workbook.loaded_theme = modern_base_theme_bytes()


def _current_theme_bytes(workbook):
    """
    The workbook's own theme XML, as bytes.

    openpyxl exposes a loaded file's theme on loaded_theme; a workbook built
    from scratch carries the library's bundled default, which openpyxl keeps
    as a module-level string in its theme writer.
    """
    theme = getattr(workbook, 'loaded_theme', None)

    if theme:
        return theme if isinstance(theme, bytes) else theme.encode('utf-8')

    return theme_xml.encode('utf-8')


def set_default_pivot_style(workbook, style_name):
    """
    Point the workbook's defaultPivotStyle at a built-in gallery style.

    No style is defined - the name refers to one of Excel's built-ins, and
    Excel resolves its colours through the workbook's theme accents. This is
    the cheap path: one attribute, no dxf records, no tableStyle entry.

    Args:
        workbook:   The openpyxl workbook being formatted
        style_name: A built-in style name, e.g. PivotStyleLight19
    """
    if not isinstance(style_name, str) or not style_name.strip():
        raise ThemeManagerError("default_pivot_style must be a non-empty style name")

    existing = workbook._table_styles

    workbook._table_styles = TableStyleList(
        defaultTableStyle=getattr(existing, 'defaultTableStyle', None) or 'TableStyleMedium9',
        defaultPivotStyle=style_name,
        tableStyle=list(getattr(existing, 'tableStyle', []) or []),
    )

    return style_name


def build_pivot_style(workbook, pivot_config, color_normalizer=None):
    """
    Register a custom PivotTable style in the workbook and return its name.

    The style is written into styles.xml as a tableStyle whose elements point
    at differential formats. Setting it as defaultPivotStyle means a pivot the
    user inserts later is styled without them choosing anything from the
    gallery - which is the point: the gallery entry they WOULD have picked
    resolves through theme accents, while this style carries exact colours.

    Args:
        workbook:     The openpyxl workbook being formatted
        pivot_config: The pivot_style mapping from the recipe

    Returns:
        The registered style name
    """
    style_name = pivot_config.get('name', 'ERP Pivot')

    if not isinstance(style_name, str) or not style_name.strip():
        raise ThemeManagerError("pivot_style name must be a non-empty string")

    elements = []

    header_color = pivot_config.get('header_background_color')
    header_font_color = pivot_config.get('header_font_color')
    header_bold = pivot_config.get('header_bold', True)

    if header_color or header_font_color:
        header_font = Font(
            b=bool(header_bold),
            color=normalize_hex_color(header_font_color, context='header_font_color',
                                      color_normalizer=color_normalizer)
            if header_font_color else None,
        )
        header_fill = (
            PatternFill(
                'solid',
                start_color=normalize_hex_color(header_color, context='header_background_color',
                                                color_normalizer=color_normalizer),
                end_color=normalize_hex_color(header_color, context='header_background_color',
                                              color_normalizer=color_normalizer),
            )
            if header_color else None
        )
        header_dxf_id = workbook._differential_styles.add(
            DifferentialStyle(font=header_font, fill=header_fill)
        )
        elements.append(TableStyleElement(type='headerRow', dxfId=header_dxf_id))

    bold_subtotals = pivot_config.get('bold_subtotals', False)
    bold_grand_totals = pivot_config.get('bold_grand_totals', False)

    if bold_subtotals or bold_grand_totals:
        # Total rows carry top+bottom borders in the HEADER blue
        # (2026-08-26): thin for subtotals, medium ("bold") for the
        # grand total. Border colour follows header_background_color
        # so the bands read as one family; without a header colour the
        # rows stay borderless-bold as before.
        border_hex = (
            normalize_hex_color(header_color, context='header_background_color',
                                color_normalizer=color_normalizer)
            if header_color else None)

        def banded_dxf(weight):
            if border_hex is None:
                return DifferentialStyle(font=Font(b=True))
            edge = Side(style=weight, color=border_hex)
            return DifferentialStyle(font=Font(b=True),
                                     border=Border(top=edge, bottom=edge))

        if bold_subtotals:
            subtotal_dxf_id = workbook._differential_styles.add(banded_dxf('thin'))
            for element_type in SUBTOTAL_ELEMENTS:
                elements.append(TableStyleElement(type=element_type, dxfId=subtotal_dxf_id))

        if bold_grand_totals:
            grand_dxf_id = workbook._differential_styles.add(banded_dxf('medium'))
            elements.append(TableStyleElement(type='totalRow', dxfId=grand_dxf_id))

    if not elements:
        raise ThemeManagerError(
            "pivot_style produced no formatting. Set at least one of "
            "header_background_color, header_font_color, bold_subtotals, bold_grand_totals."
        )

    table_style = TableStyle(
        name=style_name,
        pivot=True,
        table=False,
        count=len(elements),
        tableStyleElement=elements,
    )

    existing = workbook._table_styles
    default_table_style = getattr(existing, 'defaultTableStyle', None) or 'TableStyleMedium9'

    set_as_default = pivot_config.get('set_as_default', True)

    workbook._table_styles = TableStyleList(
        defaultTableStyle=default_table_style,
        defaultPivotStyle=style_name if set_as_default else
            (getattr(existing, 'defaultPivotStyle', None) or 'PivotStyleLight16'),
        tableStyle=[table_style],
    )

    logger.info(
        f"🎨 Pivot style '{style_name}' registered with {len(elements)} element(s)"
        f"{' and set as the workbook default' if set_as_default else ''}"
    )

    return style_name

# End of file #
