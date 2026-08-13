"""
Excel file writer for saving pandas DataFrames to Excel files.

Handles writing DataFrames to Excel with various formatting options and error handling.
"""

import logging
import io
import re
import shutil
from pathlib import Path
from datetime import datetime
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.processors._helpers.format_excel_theme_manager import apply_base_theme
from excel_recipe_processor.writers._helpers.excel_writer_backup_rgx import (
    build_backup_name_rgx,
    legacy_backup_rgx,
)

import pandas as pd

logger = logging.getLogger(__name__)


# How many of the newest backups survive; everything older is deleted. Two
# covers the realistic recovery cases - the last good file and the one
# before it - without letting an auto-generated, out-of-sight artefact
# accumulate copies forever.
DEFAULT_DELETE_BACKUPS_BEYOND = 2


class ExcelWriterError(Exception):
    """Raised when Excel writing operations fail."""
    pass


class ExcelWriter:
    """
    Handles writing pandas DataFrames to Excel files.
    
    Provides a clean interface for saving data with proper
    error handling and basic formatting options.
    """
    
    def __init__(self):
        """Initialize the Excel writer."""
        self.last_output_path = None
    
    def write_file(self, df: pd.DataFrame, output_path, sheet_name: str = 'Sheet1', 
                   index: bool = False, **kwargs) -> None:
        """
        Write a DataFrame to an Excel file.
        
        Args:
            df: pandas DataFrame to write
            output_path: Path where the Excel file should be saved
            sheet_name: Name of the sheet to create
            index: Whether to include DataFrame index in output
            **kwargs: Additional arguments passed to pandas.to_excel()
            
        Raises:
            ExcelWriterError: If file writing fails
        """
        # Guard clauses
        if not isinstance(df, pd.DataFrame):
            raise ExcelWriterError("Data must be a pandas DataFrame")
        
        if df.empty:
            logger.warning("Writing empty DataFrame to Excel file")
        
        if not output_path:
            raise ExcelWriterError("Output path cannot be empty")
        
        if not isinstance(sheet_name, str) or not sheet_name.strip():
            raise ExcelWriterError("Sheet name must be a non-empty string")
        
        output_path = Path(output_path)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add .xlsx extension if no extension provided
        if not output_path.suffix:
            output_path = output_path.with_suffix('.xlsx')
        
        # Validate extension
        valid_extensions = {'.xlsx', '.xls', '.xlsm'}
        if output_path.suffix.lower() not in valid_extensions:
            raise ExcelWriterError(
                f"Invalid file extension: {output_path.suffix}. "
                f"Expected one of: {', '.join(valid_extensions)}"
            )
        
        logger.info(f"Writing DataFrame to Excel: {output_path}")
        
        try:
            # Write the DataFrame through an explicit writer, so the
            # constructed workbook can be given the modern Office theme
            # before it is serialized (see apply_base_theme).
            single_writer = pd.ExcelWriter(output_path, engine='openpyxl')
            try:
                df.to_excel(single_writer, sheet_name=sheet_name, index=index, **kwargs)
            finally:
                apply_base_theme(single_writer.book)
                single_writer.close()
            
            self.last_output_path = output_path
            
            logger.info(
                f"Successfully wrote {len(df)} rows, {len(df.columns)} columns "
                f"to sheet '{sheet_name}' in ")
            logger.info(f"{output_path}")
            
        except PermissionError:
            raise ExcelWriterError(
                f"Permission denied writing to: {output_path}. "
                "File may be open in another application."
            )
        except Exception as e:
            raise ExcelWriterError(f"Error writing Excel file: {e}")
    
    def write_multiple_sheets(self, data_dict: dict, output_path) -> None:
        """
        Write multiple DataFrames to different sheets in one Excel file.
        
        Args:
            data_dict: Dictionary mapping sheet names to DataFrames
            output_path: Path where the Excel file should be saved
            
        Raises:
            ExcelWriterError: If file writing fails
        """
        # Guard clauses
        if not isinstance(data_dict, dict):
            raise ExcelWriterError("Data must be provided as a dictionary")
        
        if not data_dict:
            raise ExcelWriterError("Data dictionary cannot be empty")
        
        if not output_path:
            raise ExcelWriterError("Output path cannot be empty")
        
        output_path = Path(output_path)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add .xlsx extension if no extension provided
        if not output_path.suffix:
            output_path = output_path.with_suffix('.xlsx')
        
        logger.info(f"Writing {len(data_dict)} sheets to Excel: '{output_path}'")
        
        try:
            # THE EXPORT BRIDGE. Under a pipeline session, the workbook this
            # writer populates is handed straight to the session instead of
            # being saved - the very next file operation would only have
            # read the identical bytes back. The writer targets a throwaway
            # buffer so no disk handle ever opens; the live book saves once,
            # at run end (or at an explicit flush_workbooks step).
            # Standalone callers keep the legacy save-to-disk path below.
            bridged = WorkbookSession.is_deferred()
            writer_target = io.BytesIO() if bridged else output_path
            writer = pd.ExcelWriter(writer_target, engine='openpyxl')

            try:
                for sheet_name, df in data_dict.items():
                    # Guard clauses for each sheet
                    if not isinstance(sheet_name, str) or not sheet_name.strip():
                        logger.warning(f"Skipping invalid sheet name: '{sheet_name}'")
                        continue
                    
                    if not isinstance(df, pd.DataFrame):
                        logger.warning(f"Skipping non-DataFrame data for sheet: '{sheet_name}'")
                        continue
                    
                    # Write this sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.debug(f"Wrote sheet '{sheet_name}': {len(df)} rows")
            finally:
                # Every workbook ERP constructs gets the modern Office theme
                # as its base. openpyxl's bundled theme is the 2007 palette,
                # which is why generated files offered washed-out colours in
                # Excel's style galleries. Recipe workbook_theme directives
                # layer on top of this later.
                apply_base_theme(writer.book)

                if bridged:
                    # Normalize to DISK EQUIVALENCE before adoption. pandas
                    # materializes every NaN as a literal '' cell (na_rep
                    # default); serializing to disk silently drops those, so
                    # a reloaded file shows them as empty. The live book
                    # must match, or downstream steps see phantom "data"
                    # a disk round-trip would have erased.
                    for worksheet in writer.book.worksheets:
                        for row in worksheet.iter_rows():
                            for cell in row:
                                if cell.value == '':
                                    cell.value = None

                    # Hand the populated book to the session. close() is
                    # DELIBERATELY not called on this path: closing is what
                    # serializes the workbook, and serializing it into a
                    # throwaway buffer is the exact cost the bridge removes.
                    # The buffer holds no disk handle and is garbage.
                    WorkbookSession.adopt_workbook(output_path, writer.book)
                else:
                    writer.close()

            self.last_output_path = output_path
            logger.info(f"Successfully wrote Excel file with {len(data_dict)} sheets")
            
        except PermissionError:
            raise ExcelWriterError(
                f"Permission denied writing to: {output_path}. "
                "File may be open in another application."
            )
        except Exception as e:
            raise ExcelWriterError(f"Error writing multi-sheet Excel file: {e}")
    
    def append_sheet_to_excel_file(self, df: pd.DataFrame, output_path, sheet_name: str) -> None:
        """
        Append a DataFrame to an existing Excel file as a new sheet.
        
        Args:
            df: pandas DataFrame to append
            output_path: Path to existing Excel file
            sheet_name: Name of the new sheet to create
            
        Raises:
            ExcelWriterError: If append operation fails
        """
        # Guard clauses
        if not isinstance(df, pd.DataFrame):
            raise ExcelWriterError("Data must be a pandas DataFrame")
        
        if not output_path:
            raise ExcelWriterError("Output path cannot be empty")
        
        if not isinstance(sheet_name, str) or not sheet_name.strip():
            raise ExcelWriterError("Sheet name must be a non-empty string")
        
        output_path = Path(output_path)
        
        if not output_path.exists():
            # If file doesn't exist, create it with just this sheet
            logger.info(f"File doesn't exist, creating new file: {output_path}")
            self.write_file(df, output_path, sheet_name)
            return
        
        logger.info(f"Appending sheet '{sheet_name}' to existing file: {output_path}")
        
        try:
            # Read existing file to preserve other sheets
            with pd.ExcelFile(output_path) as existing_file:
                existing_sheets = {}
                for existing_sheet_name in existing_file.sheet_names:
                    existing_sheets[existing_sheet_name] = pd.read_excel(
                        existing_file, sheet_name=existing_sheet_name
                    )
            
            # Add the new sheet
            existing_sheets[sheet_name] = df
            
            # Write all sheets back
            self.write_multiple_sheets(existing_sheets, output_path)
            
            logger.info(f"Successfully appended sheet '{sheet_name}'")
            
        except Exception as e:
            raise ExcelWriterError(f"Error appending to Excel file: {e}")
    
    def create_backup(self, file_path, delete_backups_beyond: int = DEFAULT_DELETE_BACKUPS_BEYOND):
        """
        Create a timestamped backup copy, then trim the oldest beyond the cap.

        Backups are named so the EXTENSION SURVIVES:

            report_erpbkup_260812_144320.xlsx

        The old scheme appended ".backup" AFTER the extension, which changed
        the file type as far as the file manager was concerned and stopped
        the backup opening in its default application.

        Names are written ONCE and never renamed. A rolling scheme would have
        to shift every file on every run, and a crash mid-shift leaves the set
        ambiguous; here a crash can only ever leave one extra file. Because
        YYMMDD_HHMMSS is zero-padded and monotonic, sorting the names
        lexicographically sorts them chronologically, so trimming is a pure
        deletion of everything past the newest N.

        Args:
            file_path:             File to back up
            delete_backups_beyond: How many of the newest backups to KEEP;
                                   every older one is deleted. Counts the
                                   backup being made, so 2 leaves the new
                                   one plus its predecessor. 0 makes none.

        Returns:
            Path to the backup file, or None when the cap is 0

        Raises:
            ExcelWriterError: If the cap is negative or the copy fails
        """
        # Guard clauses
        if not file_path:
            raise ExcelWriterError("File path cannot be empty")

        if not isinstance(delete_backups_beyond, int) or isinstance(delete_backups_beyond, bool):
            raise ExcelWriterError(
                f"delete_backups_beyond must be an integer, got {type(delete_backups_beyond).__name__}"
            )

        if delete_backups_beyond < 0:
            raise ExcelWriterError(
                f"delete_backups_beyond cannot be negative, got {delete_backups_beyond}. "
                f"It is the number of newest backups to keep; older ones are deleted."
            )

        file_path = Path(file_path)

        if not file_path.exists():
            raise ExcelWriterError(f"File not found: {file_path}")

        if delete_backups_beyond == 0:
            logger.debug(f"delete_backups_beyond is 0 (keep none); no backup made of {file_path.name}")
            return None

        stamp = datetime.now().strftime('%y%m%d_%H%M%S')
        backup_path = file_path.with_name(
            f"{file_path.stem}_erpbkup_{stamp}{file_path.suffix}"
        )

        # Same-second collision: a fast rerun, or one recipe exporting the
        # same file twice. The counter suffix still sorts after its sibling.
        counter = 2
        while backup_path.exists():
            backup_path = file_path.with_name(
                f"{file_path.stem}_erpbkup_{stamp}_{counter}{file_path.suffix}"
            )
            counter += 1

        try:
            shutil.copy2(file_path, backup_path)
            logger.info(f"Created backup: {backup_path.name}")

        except Exception as e:
            raise ExcelWriterError(f"Error creating backup: {e}")

        self._trim_backups(file_path, delete_backups_beyond)

        return backup_path

    def _trim_backups(self, file_path, delete_backups_beyond: int) -> int:
        """
        Delete this file's oldest backups beyond the allowed count.

        Only names matching the _erpbkup_ pattern for THIS file's stem and
        extension are considered, so a neighbouring file's backups and any
        hand-named file are untouchable. Legacy ".backup" files from the old
        scheme are recognised only to be reported, never deleted.

        Args:
            file_path:             The source file whose backups to trim
            delete_backups_beyond: How many of the newest to keep

        Returns:
            Number of backups deleted
        """
        file_path = Path(file_path)
        folder = file_path.parent

        pattern = build_backup_name_rgx(
            re.escape(file_path.stem), re.escape(file_path.suffix)
        )

        # Sorting names lexicographically sorts them chronologically, given
        # the zero-padded timestamp - no reliance on modification times,
        # which would not survive the files being copied or moved.
        existing = sorted(
            (entry for entry in folder.iterdir()
             if entry.is_file() and pattern.match(entry.name)),
            key=lambda entry: entry.name,
        )

        surplus = existing[:-delete_backups_beyond] if delete_backups_beyond else existing
        deleted = 0

        for stale in surplus:
            try:
                stale.unlink()
                deleted += 1
            except OSError as error:
                logger.warning(f"⚠️  Could not remove old backup {stale.name}: {error}")

        if deleted:
            logger.info(
                f"🧹 Deleted {deleted} backup(s) beyond the newest "
                f"{delete_backups_beyond}; {len(existing) - deleted} kept"
            )

        legacy = [entry.name for entry in folder.iterdir()
                  if entry.is_file()
                  and entry.name.startswith(file_path.name)
                  and legacy_backup_rgx.search(entry.name)]

        if legacy:
            logger.info(
                f"ℹ️  {len(legacy)} backup(s) from the old '.backup' scheme are still "
                f"present and are NOT trimmed automatically: {sorted(legacy)[:3]}"
                f"{' ...' if len(legacy) > 3 else ''}"
            )

        return deleted
    
    def get_output_info(self) -> dict:
        """
        Get information about the last file written.
        
        Returns:
            Dictionary with output file information
        """
        if self.last_output_path is None:
            return {"message": "No file has been written yet"}
        
        info = {
            "output_path": str(self.last_output_path),
            "file_extension": self.last_output_path.suffix,
        }
        
        if self.last_output_path.exists():
            info["file_size"] = self.last_output_path.stat().st_size
            info["exists"] = True
        else:
            info["exists"] = False
        
        return info
