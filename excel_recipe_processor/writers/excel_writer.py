"""
Excel file writer for saving pandas DataFrames to Excel files.

Handles writing DataFrames to Excel with various formatting options and error handling.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


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
            # Write the DataFrame
            df.to_excel(output_path, sheet_name=sheet_name, index=index, **kwargs)
            
            self.last_output_path = output_path
            
            logger.info(
                f"Successfully wrote {len(df)} rows, {len(df.columns)} columns "
                f"to sheet '{sheet_name}' in {output_path}"
            )
            
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
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
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
    
    def create_backup(self, file_path) -> Path:
        """
        Create a backup copy of an Excel file.
        
        Args:
            file_path: Path to file to backup
            
        Returns:
            Path to the backup file
            
        Raises:
            ExcelWriterError: If backup creation fails
        """
        # Guard clause
        if not file_path:
            raise ExcelWriterError("File path cannot be empty")
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ExcelWriterError(f"File not found: {file_path}")
        
        # Generate backup filename
        backup_path = file_path.with_suffix(f'{file_path.suffix}.backup')
        
        # If backup already exists, add a number
        counter = 1
        while backup_path.exists():
            backup_path = file_path.with_suffix(f'{file_path.suffix}.backup{counter}')
            counter += 1
        
        try:
            # Copy the file
            import shutil
            shutil.copy2(file_path, backup_path)
            
            logger.info(f"Created backup: {backup_path}")
            return backup_path
            
        except Exception as e:
            raise ExcelWriterError(f"Error creating backup: {e}")
    
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
