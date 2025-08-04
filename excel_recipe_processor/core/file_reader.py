"""
Central file reading coordination for Excel Recipe Processor.

Provides unified interface for reading files in various formats with automatic
format detection, and consistent error handling.
"""

import pandas as pd
import logging

from pathlib import Path

from excel_recipe_processor.readers.excel_reader import ExcelReader, ExcelReaderError


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
    def read_file(filename, sheet=1, encoding='utf-8', separator=',', explicit_format=None):
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
                return FileReader._read_excel_file(filename, sheet_for_excel)
            elif file_format in FileReader.CSV_FORMATS:
                return FileReader._read_csv_file(filename, encoding, separator)
            elif file_format in FileReader.TSV_FORMATS:
                return FileReader._read_tsv_file(filename, encoding)
            else:
                raise FileReaderError(f"Unsupported file format: {file_format}")
                
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
    def _read_excel_file(filename, sheet):
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
            data = excel_reader.read_file(filename, sheet_name=sheet)
            
            logger.debug(f"Read Excel file '{filename}', sheet: {sheet}, shape: {data.shape}")
            return data
            
        except ExcelReaderError as e:
            raise FileReaderError(f"Excel reading error for '{filename}': {e}")
    
    @staticmethod
    def _read_csv_file(filename, encoding, separator):
        """Read CSV file with robust options."""
        try:
            data = pd.read_csv(
                filename,
                encoding=encoding,
                sep=separator,
                # Robust CSV reading options
                skipinitialspace=True,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'None'],
                keep_default_na=True,
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
    def _read_tsv_file(filename, encoding):
        """Read TSV file with robust options."""
        try:
            data = pd.read_csv(
                filename,
                encoding=encoding,
                sep='\t',
                # Robust TSV reading options
                skipinitialspace=True,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'None'],
                keep_default_na=True,
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
