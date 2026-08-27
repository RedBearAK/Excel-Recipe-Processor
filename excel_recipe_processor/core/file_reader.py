"""
Central file reading coordination for Excel Recipe Processor.

excel_recipe_processor/core/file_reader.py

Provides unified interface for reading files in various formats with automatic
format detection, and consistent error handling.
"""

import logging
import pandas as pd
from excel_recipe_processor.core.log_format import q

from pathlib import Path
from importlib.util import find_spec

from excel_recipe_processor.readers.excel_reader import ExcelReader, ExcelReaderError


# calamine is a fast Rust-based Excel READER with prebuilt PyPI wheels.
# Optional: when the wheel is present, Excel imports use it (several times
# faster on large files, values identical - see tests/test_calamine_reader);
# when absent, the openpyxl path serves exactly as before. Detected once at
# import, without importing the package itself.
CALAMINE_AVAILABLE = find_spec('python_calamine') is not None


logger = logging.getLogger(__name__)


class FileReaderError(Exception):
    """Raised when file reading operations fail."""
    pass


class FileReader:
    """
    Central coordinator for reading files in various formats.
    
    Handles format auto-detection, and delegates
    to appropriate specialized readers or pandas for different file types.
    All methods are static for easy use across processors.
    """
    
    # Logical format categories (without dots)
    EXCEL_FORMATS = {'xlsx', 'xls', 'xlsm', 'xlsb'}
    CSV_FORMATS = {'csv'}
    TSV_FORMATS = {'tsv'}  # Single logical format for tab-separated
    
    ALL_FORMATS = EXCEL_FORMATS | CSV_FORMATS | TSV_FORMATS
    
    # pandas' documented default NA strings (the set keep_default_na=True
    # applies). Named here so the verbatim-columns policy can reproduce the
    # default coercion for UNPROTECTED columns after a raw read, instead of
    # importing pandas internals.
    DEFAULT_NA_STRINGS = (
        '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
        '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'None',
        'n/a', 'nan', 'null',
    )

    # Extension to logical format mapping
    EXTENSION_TO_FORMAT = {
        '.xlsx': 'xlsx',
        '.xls': 'xls',
        '.xlsm': 'xlsm',
        '.xlsb': 'xlsb',
        '.csv': 'csv',
        '.tsv': 'tsv',
        '.txt': 'tsv',  # .txt files are processed as TSV
    }
    
    @staticmethod
    def read_file(filename, sheet=1, encoding='utf-8', separator=',', explicit_format=None,
                  verbatim_text_columns=None):
        """
        Read a file with automatic format detection
        
        Args:
            filename: Path to file
            sheet: Sheet name or 1-based index (1 for first sheet) - CONVERTS to 0-based internally
            encoding: Text encoding for CSV/TSV files (default: 'utf-8')
            separator: Column separator for CSV files (default: ',')
            explicit_format: Override format detection ('xlsx', 'csv', 'tsv')
            
        Returns:
            DataFrame with file contents
            
        Raises:
            FileReaderError: If file reading fails
        """

        try:
            # Validate file exists
            FileReader._validate_file_exists(filename)
            
            # Determine logical format
            file_format = FileReader._determine_format(filename, explicit_format)
            
            # (The numeric-string-to-index coercion that lived here shadowed
            # tabs literally named "1". Gone as of the 2026-08-14 doctrine:
            # import_file resolves names, numbers and ?sheet_NNN? tokens to a
            # REAL NAME before this call; the int path below remains for
            # internal callers only.)

            # Convert 1-based sheet index to 0-based for Excel files
            if file_format in FileReader.EXCEL_FORMATS and isinstance(sheet, int):
                if sheet < 1:
                    raise FileReaderError(
                        f"Sheet index must be 1 or greater, got {sheet}. "
                        "Use 1 for first sheet, 2 for second sheet, etc."
                    )
                sheet_for_excel = sheet - 1  # Convert to 0-based for ExcelReader
            else:
                sheet_for_excel = sheet  # Pass sheet names through unchanged
            
            # Delegate to appropriate reader based on logical format
            if file_format in FileReader.EXCEL_FORMATS:
                data = FileReader._read_excel_file(
                    filename, sheet_for_excel,
                    raw_na=bool(verbatim_text_columns))
            elif file_format in FileReader.CSV_FORMATS:
                data = FileReader._read_csv_file(
                    filename, encoding, separator,
                    raw_na=bool(verbatim_text_columns))
            elif file_format in FileReader.TSV_FORMATS:
                data = FileReader._read_tsv_file(
                    filename, encoding,
                    raw_na=bool(verbatim_text_columns))
            else:
                raise FileReaderError(f"Unsupported file format: {q(file_format)}")

            if verbatim_text_columns:
                data = FileReader._apply_na_policy(data, verbatim_text_columns)

            return data
                
        except FileReaderError:
            raise
        except Exception as e:
            raise FileReaderError(f"Unexpected error reading file '{filename}': {e}")
    
    @staticmethod
    def file_exists(filename):
        """
        Check if a file exists
        
        Args:
            filename: Path to file
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            return Path(filename).exists()
        except Exception:
            return False
    
    @staticmethod
    def get_file_info(filename):
        """
        Get information about a file
        
        Args:
            filename: Path to file
            
        Returns:
            Dictionary with file information
        """
        try:
            file_path = Path(filename)
            
            if not file_path.exists():
                return {
                    'original_filename': filename,
                    'final_filename': filename,
                    'exists': False,
                    'error': 'File not found'
                }
            
            return {
                'original_filename': filename,
                'final_filename': filename,
                'exists': True,
                'size_bytes': file_path.stat().st_size,
                'extension': file_path.suffix.lower(),
                'detected_format': FileReader._determine_format(filename, None)
            }
            
        except Exception as e:
            return {
                'original_filename': filename,
                'final_filename': filename,
                'exists': False,
                'error': str(e)
            }
    
    @staticmethod
    def get_excel_sheets(filename):
        """
        Get list of sheet names from an Excel file.
        
        Args:
            filename: Path to Excel file
            
        Returns:
            List of sheet names
            
        Raises:
            FileReaderError: If file is not Excel or cannot be read
        """
        try:
            # Validate file exists
            FileReader._validate_file_exists(filename)
            
            # Check if it's an Excel file
            file_format = FileReader._determine_format(filename, None)
            if file_format not in FileReader.EXCEL_FORMATS:
                raise FileReaderError(f"File '{filename}' is not an Excel file (format: {file_format})")
            
            # Use ExcelReader to get sheet names
            excel_reader = ExcelReader()
            return excel_reader.get_sheet_names(filename)
            
        except ExcelReaderError as e:
            raise FileReaderError(f"Error reading Excel sheets from '{filename}': {e}")
        except FileReaderError:
            raise
        except Exception as e:
            raise FileReaderError(f"Unexpected error getting Excel sheets from '{filename}': {e}")
    
    @staticmethod
    def get_supported_formats():
        """
        Get information about supported file formats.
        
        Returns:
            Dictionary with format information
        """
        return {
            'excel_formats': list(FileReader.EXCEL_FORMATS),
            'csv_formats': list(FileReader.CSV_FORMATS),
            'tsv_formats': list(FileReader.TSV_FORMATS),
            'all_formats': list(FileReader.ALL_FORMATS),
            'supported_extensions': list(FileReader.EXTENSION_TO_FORMAT.keys()),
            'extension_mapping': dict(FileReader.EXTENSION_TO_FORMAT),
            'format_descriptions': {
                'xlsx': 'Excel 2007+ format (recommended)',
                'xls': 'Legacy Excel format',
                'xlsm': 'Excel with macros',
                'xlsb': 'Excel binary format',
                'csv': 'Comma-separated values',
                'tsv': 'Tab-separated values (.tsv and .txt files)'
            }
        }
    
    # =============================================================================
    # PRIVATE HELPER METHODS
    # =============================================================================
    
    @staticmethod
    def _validate_file_exists(filename):
        """Validate that a file exists."""
        file_path = Path(filename)
        
        if not file_path.exists():
            raise FileReaderError(f"File not found: {filename}")
        
        if not file_path.is_file():
            raise FileReaderError(f"Path is not a file: {filename}")
    
    @staticmethod
    def _determine_format(filename, explicit_format: str):
        """
        Determine logical format from extension or explicit override.
        
        Returns logical format without dots: 'xlsx', 'csv', 'tsv', etc.
        """
        if explicit_format:
            explicit_lower = explicit_format.lower()
            if explicit_lower in FileReader.ALL_FORMATS:
                return explicit_lower
            else:
                raise FileReaderError(f"Unsupported explicit format: {explicit_format}")
        
        # Auto-detect from file extension using mapping
        file_path = Path(filename)
        extension = file_path.suffix.lower()
        
        if extension in FileReader.EXTENSION_TO_FORMAT:
            logical_format = FileReader.EXTENSION_TO_FORMAT[extension]
            logger.debug(f"Extension {extension} → logical format {logical_format}")
            return logical_format
        else:
            # Unknown extension - default to Excel with warning
            logger.warning(f"Unknown file extension '{extension}' for '{filename}', assuming Excel format")
            return 'xlsx'
    
    @staticmethod
    def _apply_na_policy(data, verbatim_text_columns):
        """
        Restore default NA behavior everywhere EXCEPT the designated columns.

        A raw read (keep_default_na=False) preserves strings like "N/A" that
        pandas would otherwise coerce to missing - but it preserves them in
        EVERY column and turns genuinely empty cells into '' instead of NaN.
        This helper puts the world back except where told not to:

        - unprotected columns: the documented default NA strings and '' both
          become NaN, reproducing a normal read
        - protected (verbatim) columns: only '' becomes NaN, so a blank cell
          stays a blank cell while "N/A" stays the two characters someone
          typed
        - dtypes re-inferred afterward, so a numeric column that carried
          blanks returns to float instead of staying object

        Args:
            data:                  Frame from a raw (keep_default_na=False) read
            verbatim_text_columns: Columns whose text must survive verbatim

        Returns:
            Frame with default NA semantics restored outside the designated columns
        """
        import numpy as np

        missing = [c for c in verbatim_text_columns if c not in data.columns]
        if missing:
            logger.warning(
                f"⚠️  verbatim_text_columns not found (check spelling): {missing}. "
                f"Available: {list(data.columns)}"
            )

        default_na = set(FileReader.DEFAULT_NA_STRINGS)

        for column in data.columns:
            if data[column].dtype != object and str(data[column].dtype) != 'str':
                continue
            if column in verbatim_text_columns:
                data[column] = data[column].replace('', np.nan)
            else:
                data[column] = data[column].map(
                    lambda v: np.nan if (isinstance(v, str) and (v == '' or v in default_na)) else v
                )

        return data.infer_objects()

    @staticmethod
    def _read_excel_file(filename, sheet, raw_na=False):
        """Read Excel file using ExcelReader."""
        try:
            excel_reader = ExcelReader()
            
            # Validate sheet exists if it's a string
            if isinstance(sheet, str):
                available_sheets = excel_reader.get_sheet_names(filename)
                if sheet not in available_sheets:
                    raise FileReaderError(
                        f"Sheet '{sheet}' not found in '{filename}'. "
                        f"Available sheets: {available_sheets}"
                    )
            
            # Read the file
            # Engine choice: calamine when available, openpyxl otherwise.
            # Both were proven value-for-value equal across dtypes,
            # datetimes, blanks, the raw-NA path and formula cells; the
            # fallback keeps machines without the wheel working unchanged.
            engine_kwargs = {'engine': 'calamine'} if CALAMINE_AVAILABLE else {}

            if raw_na:
                data = excel_reader.read_file(filename, sheet_name=sheet,
                                              keep_default_na=False, **engine_kwargs)
            else:
                data = excel_reader.read_file(filename, sheet_name=sheet, **engine_kwargs)
            
            logger.debug(f"Read Excel file '{filename}', sheet: {sheet}, shape: {data.shape}")
            return data
            
        except ExcelReaderError as e:
            raise FileReaderError(f"Excel reading error for '{filename}': {e}")
    
    @staticmethod
    def _read_csv_file(filename, encoding, separator, raw_na=False):
        """Read CSV file with robust options."""
        try:
            data = pd.read_csv(
                filename,
                encoding=encoding,
                sep=separator,
                # Robust CSV reading options
                skipinitialspace=True,
                na_values=[] if raw_na else ['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'None'],
                keep_default_na=not raw_na,
                dtype=str,  # Read as strings initially to avoid data loss
                low_memory=False
            )
            
            # Convert numeric columns that can be converted
            data = FileReader._attempt_numeric_conversion(data)
            
            logger.debug(f"Read CSV file '{filename}', shape: {data.shape}")
            return data
            
        except Exception as e:
            raise FileReaderError(f"CSV reading error for '{filename}': {e}")
    
    @staticmethod
    def _read_tsv_file(filename, encoding, raw_na=False):
        """Read TSV file with robust options."""
        try:
            data = pd.read_csv(
                filename,
                encoding=encoding,
                sep='\t',
                # Robust TSV reading options
                skipinitialspace=True,
                na_values=[] if raw_na else ['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'None'],
                keep_default_na=not raw_na,
                dtype=str,  # Read as strings initially to avoid data loss
                low_memory=False
            )
            
            # Convert numeric columns that can be converted
            data = FileReader._attempt_numeric_conversion(data)
            
            logger.debug(f"Read TSV file '{filename}', shape: {data.shape}")
            return data
            
        except Exception as e:
            raise FileReaderError(f"TSV reading error for '{filename}': {e}")
    
    @staticmethod
    def _attempt_numeric_conversion(data):
        """Attempt to convert string columns to numeric where possible."""
        for column in data.columns:
            # Use the new pandas approach instead of errors='ignore'
            try:
                converted = pd.to_numeric(data[column])
                # Only use the conversion if it actually changed the data type
                if converted.dtype != data[column].dtype:
                    data[column] = converted
            except (ValueError, TypeError):
                # Keep as string if conversion fails
                pass
        
        return data
