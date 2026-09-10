"""
Default log location for recipe runs (2026-09-04).

excel_recipe_processor/core/default_log_path.py

Every run writes a log file. A recipe that names one under
settings.log_file gets exactly that (the path is a template; external
variables resolve first). A recipe that says nothing gets the platform's
standard per-user log location, named after the recipe and stamped, so
nothing is ever lost to the terminal's scrollback:

    macOS    ~/Library/Logs/excel_recipe_processor/
    Windows  %LOCALAPPDATA%/excel_recipe_processor/logs/
    other    $XDG_STATE_HOME/excel_recipe_processor/logs/
             (~/.local/state/... when XDG_STATE_HOME is unset)

ERP_LOG_DIR overrides the platform choice outright. settings.log_file:
false is the opt-out. The CLI --log-file flag still outranks everything.

Why not beside the recipe or the output: a recipe can have several
inputs and outputs, so no single folder is "the" run location - the
recipe declares one when it wants that pairing (the VMS recipe does).
"""

import os
import sys

from pathlib import Path
from datetime import datetime


ENV_LOG_DIR = 'ERP_LOG_DIR'
APP_DIR_NAME = 'excel_recipe_processor'


def default_log_dir(platform: str = '', environ: dict | None = None) -> Path:
    """
    The per-user log folder for this platform.

    Args:
        platform: sys.platform stand-in for tests; '' means the real one
        environ:  os.environ stand-in for tests; None means the real one

    Returns:
        Folder path; not created here
    """
    platform = platform or sys.platform
    environ = os.environ if environ is None else environ

    override = environ.get(ENV_LOG_DIR, '')
    if override:
        return Path(override).expanduser()

    if platform == 'darwin':
        return Path.home() / 'Library' / 'Logs' / APP_DIR_NAME

    if platform.startswith('win'):
        local_app_data = environ.get('LOCALAPPDATA', '')
        base = Path(local_app_data) if local_app_data else Path.home() / 'AppData' / 'Local'
        return base / APP_DIR_NAME / 'logs'

    state_home = environ.get('XDG_STATE_HOME', '')
    base = Path(state_home).expanduser() if state_home else Path.home() / '.local' / 'state'
    return base / APP_DIR_NAME / 'logs'


def default_log_path(recipe_path, now: datetime | None = None,
                     platform: str = '', environ: dict | None = None) -> Path:
    """<log dir>/<recipe stem>_<YYMMDD>_<HHMMSS>_log.txt"""
    stamp = (now or datetime.now()).strftime('%y%m%d_%H%M%S')
    stem = Path(str(recipe_path)).stem if recipe_path else 'recipe'
    return default_log_dir(platform, environ) / f"{stem}_{stamp}_log.txt"


def resolve_log_file_setting(setting, recipe_path, substitute) -> Path | None:
    """
    Turn settings.log_file into a path to attach, or None to write no file.

    Args:
        setting:     the raw settings.log_file value; absent is passed as None
        recipe_path: the recipe file, for the default name
        substitute:  callable applying variable substitution to a template

    Returns:
        Path to attach, or None when the recipe opted out with false

    Raises:
        ValueError: on a value that is neither bool nor str
    """
    if setting is False:
        return None
    if setting is None or setting is True:
        return default_log_path(recipe_path)
    if isinstance(setting, str):
        if not setting.strip():
            raise ValueError("settings.log_file is an empty string; use false to opt out")
        return Path(substitute(setting)).expanduser()
    raise ValueError(
        f"settings.log_file must be a path template, true, or false; got {type(setting).__name__}"
    )


# End of file #
