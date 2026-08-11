"""
Stage inspection helpers for recipe development.

excel_recipe_processor/core/stage_inspection.py

Supports the development-time CLI options that let a recipe be examined without
editing it. Editing a recipe to observe it changes the thing being observed,
which is why these live outside the recipe file entirely.

Row specifications, used by --dump-stage NAME:SPEC

    20        first 20 rows
    -20       last 20 rows
    100-150   rows 100 to 150 inclusive, 1-based
    20,-20    first 20 and last 20, with a marker row between
    (omitted) every row
"""

import re
import logging

import pandas as pd

from datetime import datetime

from pathlib import Path


logger = logging.getLogger(__name__)


# Matches the four accepted forms. Anchored, so anything else is rejected
# rather than silently misread.
head_spec_rgx = re.compile(r'^(\d+)$')
tail_spec_rgx = re.compile(r'^-(\d+)$')
range_spec_rgx = re.compile(r'^(\d+)-(\d+)$')
both_ends_spec_rgx = re.compile(r'^(\d+),-(\d+)$')

# Separates a stage name from its optional row spec. Split on the LAST colon,
# so a stage name containing one is still usable.
SPEC_SEPARATOR = ':'

# Inserted between the two halves of a both-ends dump so the gap is obvious
# rather than looking like contiguous rows.
ELLIPSIS_MARKER = '...'


class StageInspectionError(Exception):
    """Raised when a dump specification cannot be understood."""
    pass


def parse_dump_argument(argument: str) -> tuple:
    """
    Split a --dump-stage argument into a stage name and a row specification.

    Args:
        argument: Either 'stage_name' or 'stage_name:spec'

    Returns:
        Tuple of (stage_name, spec_or_None)
    """
    text = str(argument).strip()

    if not text:
        raise StageInspectionError("--dump-stage needs a stage name")

    if SPEC_SEPARATOR not in text:
        return text, None

    name, _, spec = text.rpartition(SPEC_SEPARATOR)

    if not name:
        raise StageInspectionError(
            f"--dump-stage '{argument}' has no stage name before the colon"
        )

    return name.strip(), spec.strip() or None


def validate_spec(spec) -> None:
    """
    Check a row specification before the recipe starts running.

    Without this, a typo surfaces at whichever step first produces the stage -
    step 37 of 41, in one case - after everything upstream has already run.

    Args:
        spec: Specification to check, or None

    Raises:
        StageInspectionError: If the spec cannot be understood
    """
    if spec is None:
        return

    text = str(spec).strip()

    for pattern in (both_ends_spec_rgx, range_spec_rgx, tail_spec_rgx, head_spec_rgx):
        if pattern.match(text):
            break
    else:
        raise StageInspectionError(
            f"Cannot understand row spec '{spec}'. Accepted forms: 20 (first 20), "
            f"-20 (last 20), 100-150 (a range), 20,-20 (both ends)"
        )

    match = range_spec_rgx.match(text)
    if match:
        start, end = int(match.group(1)), int(match.group(2))
        if start < 1:
            raise StageInspectionError(f"Row range '{spec}' must start at 1 or more")
        if end < start:
            raise StageInspectionError(f"Row range '{spec}' ends before it starts")


def describe_spec(spec) -> str:
    """Render a row specification as something readable for a log line."""
    if spec is None:
        return 'all rows'

    match = both_ends_spec_rgx.match(spec)
    if match:
        return f"first {match.group(1)} and last {match.group(2)} rows"

    match = range_spec_rgx.match(spec)
    if match:
        return f"rows {match.group(1)} to {match.group(2)}"

    match = tail_spec_rgx.match(spec)
    if match:
        return f"last {match.group(1)} rows"

    match = head_spec_rgx.match(spec)
    if match:
        return f"first {match.group(1)} rows"

    return f"unrecognised spec '{spec}'"


def apply_row_spec(data: pd.DataFrame, spec) -> pd.DataFrame:
    """
    Cut a DataFrame down to the rows a specification asks for.

    Args:
        data: Frame to slice
        spec: One of the accepted forms, or None for everything

    Returns:
        The requested rows

    Raises:
        StageInspectionError: If the spec cannot be parsed
    """
    if spec is None:
        return data

    text = str(spec).strip()

    match = both_ends_spec_rgx.match(text)
    if match:
        head_count = int(match.group(1))
        tail_count = int(match.group(2))

        # Nothing to elide if the two halves would cover the whole frame
        if head_count + tail_count >= len(data):
            return data

        head = data.head(head_count)
        tail = data.tail(tail_count)

        marker = pd.DataFrame(
            [[ELLIPSIS_MARKER] * len(data.columns)], columns=data.columns
        )

        return pd.concat([head, marker, tail], ignore_index=True)

    match = range_spec_rgx.match(text)
    if match:
        start = int(match.group(1))
        end = int(match.group(2))

        if start < 1:
            raise StageInspectionError(f"Row range '{spec}' must start at 1 or more")

        if end < start:
            raise StageInspectionError(f"Row range '{spec}' ends before it starts")

        # 1-based and inclusive, matching how a person counts spreadsheet rows
        return data.iloc[start - 1:end]

    match = tail_spec_rgx.match(text)
    if match:
        return data.tail(int(match.group(1)))

    match = head_spec_rgx.match(text)
    if match:
        return data.head(int(match.group(1)))

    raise StageInspectionError(
        f"Cannot understand row spec '{spec}'. Accepted forms: 20 (first 20), "
        f"-20 (last 20), 100-150 (a range), 20,-20 (both ends)"
    )


def dump_stage_to_file(stage_name: str, data: pd.DataFrame, spec,
                       output_dir: str = '.', save_number: int = 1) -> str:
    """
    Write a stage to CSV for inspection.

    CSV rather than xlsx on purpose: faster to write, and diffable and greppable
    once written. These are throwaway artifacts.

    Args:
        stage_name:  Stage being dumped, used for the filename
        data:        The stage contents
        spec:        Row specification, or None
        output_dir:  Where the file goes

    Returns:
        Path written
    """
    selected = apply_row_spec(data, spec)

    # Stage names are recipe identifiers, so they are already filename-safe,
    # but a stray separator would silently write somewhere unexpected
    safe_name = stage_name.replace('/', '_').replace('\\', '_')

    # A re-used stage dumps on EVERY save. The first dump keeps the plain
    # name; later saves append the save number AND a full timestamp, so
    # repeated dumps can never overwrite one another - not within a run
    # (distinct save numbers) and not across runs (distinct timestamps).
    if save_number <= 1:
        output_path = Path(output_dir) / f"{safe_name}.csv"
    else:
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = Path(output_dir) / f"{safe_name}_save{save_number}_{stamp}.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(output_path, index=False)

    logger.info(
        f"🔎 Dumped '{stage_name}' -> {output_path} "
        f"({len(selected):,} of {len(data):,} rows, {len(data.columns)} columns, "
        f"{describe_spec(spec)})"
    )

    return str(output_path)


# End of file #
