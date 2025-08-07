"""
Excel file reader for loading data into pandas DataFrames.

Handles reading Excel files with various options and error handling.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class ExcelReaderError(Exception):
    """Raised when Excel reading operations fail."""
    pass


class ExcelReader:
    """
    Handles reading Excel files into pandas DataFrames.
    
    Provides a clean interface for loading Excel data with proper
    error handling and validation.
    """
    
    def __init__(self):
        """Initialize the Excel reader."""
        self.last_file_path = None
        self.last_sheet_names = None
    
    def read_file(self, file_path, sheet_name=0, **kwargs) -> pd.DataFrame:
        """
        Read an Excel file into a pandas DataFrame.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name or index of sheet to read (0 for first sheet)
            **kwargs: Additional arguments passed to pandas.read_excel()
            
        Returns:
            pandas DataFrame containing the Excel data
            
        Raises:
            ExcelReaderError: If file reading fails
        """
        # Guard clauses
        if not file_path:
            raise ExcelReaderError("File path cannot be empty")
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ExcelReaderError(f"Excel file not found: {file_path}")
        
        if not file_path.is_file():
            raise ExcelReaderError(f"Path is not a file: {file_path}")
        
        # Check file extension
        valid_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb'}
        if file_path.suffix.lower() not in valid_extensions:
            raise ExcelReaderError(
                f"Invalid file extension: {file_path.suffix}. "
                f"Expected one of: {', '.join(valid_extensions)}"
            )
        
        # Guard clause for explicit None sheet_name
        if sheet_name is None:
            raise ExcelReaderError(
                "sheet_name cannot be None. Use sheet_name=0 for first sheet, "
                "or a specific sheet name/index, or use read_multiple_sheets() for all sheets."
            )
        
        logger.info(f"Reading Excel file: '{file_path}'")
        
        try:
            # Store file info for debugging
            self.last_file_path = file_path
            
            # Read the Excel file
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            
            # Guard clause: ensure we got a DataFrame
            if not isinstance(df, pd.DataFrame):
                raise ExcelReaderError("Failed to read Excel file into DataFrame")
            
            logger.info(f"Successfully read Excel file: {len(df)} rows, {len(df.columns)} columns")
            
            if df.empty:
                logger.warning("Excel file was read but contains no data")
            
            return df

        except pd.errors.EmptyDataError:
            raise ExcelReaderError(f"Excel file appears to be empty: {file_path}")
        except pd.errors.ParserError as e:
            raise ExcelReaderError(f"Error parsing Excel file: {e}")
        except PermissionError:
            raise ExcelReaderError(f"Permission denied reading file: {file_path}")
        except Exception as e:
            raise ExcelReaderError(f"Unexpected error reading Excel file: {e}")
    
    def get_sheet_names(self, file_path) -> list:
        """
        Get list of sheet names in an Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            List of sheet names
            
        Raises:
            ExcelReaderError: If unable to read sheet names
        """
        # Guard clause
        if not file_path:
            raise ExcelReaderError("File path cannot be empty")
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ExcelReaderError(f"Excel file not found: {file_path}")
        
        try:
            # Use pandas ExcelFile to get sheet names
            with pd.ExcelFile(file_path) as excel_file:
                sheet_names = excel_file.sheet_names
            
            # Guard clause: ensure we got a list
            if not isinstance(sheet_names, list):
                raise ExcelReaderError("Failed to retrieve sheet names")
            
            self.last_sheet_names = sheet_names
            logger.debug(f"Found {len(sheet_names)} sheets: {sheet_names}")
            
            return sheet_names
            
        except Exception as e:
            raise ExcelReaderError(f"Error reading sheet names from {file_path}: {e}")
    
    def read_multiple_sheets(self, file_path, sheet_names=None) -> dict:
        """
        Read multiple sheets from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_names: List of sheet names to read (None for all sheets)
            
        Returns:
            Dictionary mapping sheet names to DataFrames
            
        Raises:
            ExcelReaderError: If reading fails
        """
        # Guard clause
        if not file_path:
            raise ExcelReaderError("File path cannot be empty")
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ExcelReaderError(f"Excel file not found: {file_path}")
        
        # If no sheet names specified, get all sheets
        if sheet_names is None:
            sheet_names = self.get_sheet_names(file_path)
        
        # Guard clause: sheet_names should be a list
        if not isinstance(sheet_names, list):
            raise ExcelReaderError("Sheet names must be provided as a list")
        
        if len(sheet_names) == 0:
            raise ExcelReaderError("No sheet names provided")
        
        logger.info(f"Reading {len(sheet_names)} sheets from: {file_path}")
        
        result = {}
        for sheet_name in sheet_names:
            # Guard clause: each sheet name should be a string
            if not isinstance(sheet_name, str):
                logger.warning(f"Skipping invalid sheet name: {sheet_name}")
                continue
            
            try:
                df = self.read_file(file_path, sheet_name=sheet_name)
                result[sheet_name] = df
                logger.debug(f"Read sheet '{sheet_name}': {len(df)} rows")
            except ExcelReaderError as e:
                logger.error(f"Failed to read sheet '{sheet_name}': {e}")
                # Continue with other sheets rather than failing completely
                continue
        
        if not result:
            raise ExcelReaderError("Failed to read any sheets from the Excel file")
        
        logger.info(f"Successfully read {len(result)} sheets")
        return result
    
    def validate_columns_exist(self, df: pd.DataFrame, required_columns: list) -> None:
        """
        Validate that required columns exist in the DataFrame.
        
        Args:
            df: DataFrame to check
            required_columns: List of required column names
            
        Raises:
            ExcelReaderError: If any required columns are missing
        """
        # Guard clauses
        if not isinstance(df, pd.DataFrame):
            raise ExcelReaderError("Data must be a pandas DataFrame")
        
        if not isinstance(required_columns, list):
            raise ExcelReaderError("Required columns must be provided as a list")
        
        missing_columns = []
        for col in required_columns:
            # Guard clause: column name should be a string
            if not isinstance(col, str):
                raise ExcelReaderError(f"Column name must be a string, got: {type(col)}")
            
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            available_columns = list(df.columns)
            raise ExcelReaderError(
                f"Missing required columns: {missing_columns}. "
                f"Available columns: {available_columns}"
            )
    
    def get_file_info(self) -> dict:
        """
        Get information about the last file read.
        
        Returns:
            Dictionary with file information
        """
        if self.last_file_path is None:
            return {"message": "No file has been read yet"}
        
        info = {
            "file_path": str(self.last_file_path),
            "file_size": self.last_file_path.stat().st_size,
            "file_extension": self.last_file_path.suffix,
        }
        
        if self.last_sheet_names:
            info["sheet_names"] = self.last_sheet_names
            info["sheet_count"] = len(self.last_sheet_names)
        
        return info
