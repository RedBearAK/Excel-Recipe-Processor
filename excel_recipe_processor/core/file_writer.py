"""
Central file writing coordination for Excel Recipe Processor.

Provides unified interface for writing files in various formats with automatic
format detection, and consistent error handling.
"""

import pandas as pd
import logging

from pathlib import Path

from excel_recipe_processor.writers.excel_writer import ExcelWriter, ExcelWriterError

logger = logging.getLogger(__name__)


class FileWriterError(Exception):
    """Raised when file writing operations fail."""
    pass


class FileWriter:
    """
    Central coordinator for writing files in various formats.
    
    Handles format auto-detection, and delegates
    to appropriate specialized writers or pandas for different file types.
    All methods are static for easy use across processors.
    """
    
    # Logical format categories (without dots) - matches FileReader
    EXCEL_FORMATS = {'xlsx', 'xls', 'xlsm'}
    CSV_FORMATS = {'csv'}
    TSV_FORMATS = {'tsv'}
    
    ALL_FORMATS = EXCEL_FORMATS | CSV_FORMATS | TSV_FORMATS
    
    # Extension to logical format mapping - matches FileReader
    EXTENSION_TO_FORMAT = {
        '.xlsx': 'xlsx',
        '.xls': 'xls', 
        '.xlsm': 'xlsm',
        '.csv': 'csv',
        '.tsv': 'tsv',
        '.txt': 'tsv',  # .txt files are written as TSV
    }
    
    @staticmethod
    def write_file(data, filename, sheet_name='Data', index=False, 
                    create_backup=False, explicit_format=None,
                    encoding='utf-8', separator=','):
        """
        Write a DataFrame to file with automatic format detection
        
        Args:
            data: DataFrame to write
            filename: Output file path
            sheet_name: Sheet name for Excel files (default: 'Data')
            index: Whether to include DataFrame index (default: False)
            create_backup: Create backup if file exists (default: False)
            explicit_format: Override format detection ('xlsx', 'csv', 'tsv')
            encoding: Text encoding for CSV/TSV files (default: 'utf-8')
            separator: Column separator for CSV files (default: ',')
            
        Returns:
            Filename
            
        Raises:
            FileWriterError: If file writing fails
        """
        try:
            # Validate input data
            FileWriter._validate_dataframe(data)
            
            # Ensure output directory exists
            FileWriter._ensure_directory_exists(filename)
            
            # Create backup if requested
            if create_backup:
                FileWriter._create_backup_if_exists(filename)
            
            # Determine file format
            file_format = FileWriter._determine_format(filename, explicit_format)
            
            # Delegate to appropriate writer based on logical format
            if file_format in FileWriter.EXCEL_FORMATS:
                FileWriter._write_excel_file(data, filename, sheet_name, index)
            elif file_format in FileWriter.CSV_FORMATS:
                FileWriter._write_csv_file(data, filename, index, encoding, separator)
            elif file_format in FileWriter.TSV_FORMATS:
                FileWriter._write_tsv_file(data, filename, index, encoding)
            else:
                raise FileWriterError(f"Unsupported file format: {file_format}")
            
            logger.info(f"Wrote {len(data)} rows to '{filename}' ({file_format} format)")
            return filename
            
        except FileWriterError:
            raise
        except Exception as e:
            raise FileWriterError(f"Unexpected error writing file '{filename}': {e}")
    
    @staticmethod
    def write_multi_sheet_excel(sheets_data, filename, create_backup=False, active_sheet=None):
        """
        Write multiple DataFrames to different sheets in one Excel file.
        
        Args:
            sheets_data: Dictionary mapping sheet names to DataFrames
            filename: Output Excel file path
            create_backup: Create backup if file exists (default: False)
            active_sheet: Sheet to set as active (default: first sheet)
            
        Returns:
            Filename
            
        Raises:
            FileWriterError: If file writing fails
        """
        try:
            # Validate input
            if not isinstance(sheets_data, dict) or not sheets_data:
                raise FileWriterError("sheets_data must be a non-empty dictionary")
            
            for sheet_name, df in sheets_data.items():
                if not isinstance(sheet_name, str) or not sheet_name.strip():
                    raise FileWriterError(f"Sheet name must be a non-empty string, got: {type(sheet_name)}")
                FileWriter._validate_dataframe(df)
            
            # Force Excel format
            file_path = Path(filename)
            extension = file_path.suffix.lower()
            if extension not in FileWriter.EXTENSION_TO_FORMAT or FileWriter.EXTENSION_TO_FORMAT[extension] not in FileWriter.EXCEL_FORMATS:
                filename = str(file_path.with_suffix('.xlsx'))
                logger.debug(f"Changed extension to .xlsx for multi-sheet file: {filename}")
            
            # Ensure output directory exists
            FileWriter._ensure_directory_exists(filename)
            
            # Create backup if requested
            if create_backup:
                FileWriter._create_backup_if_exists(filename)
            
            # Use ExcelWriter for multi-sheet writing
            excel_writer = ExcelWriter()
            excel_writer.write_multiple_sheets(sheets_data, filename)
            
            # Set active sheet if specified (requires openpyxl)
            if active_sheet and active_sheet in sheets_data:
                FileWriter._set_active_sheet(filename, active_sheet)
            
            total_rows = sum(len(df) for df in sheets_data.values())
            logger.info(f"Wrote {total_rows} total rows across {len(sheets_data)} sheets to '{filename}'")
            return filename
            
        except ExcelWriterError as e:
            raise FileWriterError(f"Excel writing error: {e}")
        except FileWriterError:
            raise
        except Exception as e:
            raise FileWriterError(f"Unexpected error writing multi-sheet Excel file '{filename}': {e}")
    
    @staticmethod
    def create_backup(filename):
        """
        Create a backup copy of an existing file.
        
        Args:
            filename: File to backup
            
        Returns:
            Path to backup file
            
        Raises:
            FileWriterError: If backup creation fails
        """
        try:
            # Use ExcelWriter's backup functionality for all file types
            excel_writer = ExcelWriter()
            backup_path = excel_writer.create_backup(filename)
            
            logger.info(f"Created backup: {backup_path}")
            return str(backup_path)
            
        except ExcelWriterError as e:
            raise FileWriterError(f"Backup creation error: {e}")
        except Exception as e:
            raise FileWriterError(f"Unexpected error creating backup for '{filename}': {e}")
    
    @staticmethod
    def file_writable(filename):
        """
        Check if a file location is writable
        
        Args:
            filename: Path to file
            
        Returns:
            True if file location is writable, False otherwise
        """
        try:
            file_path = Path(filename)
            
            # Check if directory exists or can be created
            try:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                return True
            except PermissionError:
                return False
                
        except Exception:
            return False
    
    @staticmethod
    def get_output_info(filename):
        """
        Get information about an output file path
        
        Args:
            filename: Output file path
            
        Returns:
            Dictionary with output file information
        """
        try:
            file_path = Path(filename)
            
            info = {
                'original_filename': filename,
                'final_filename': filename,
                'directory': str(file_path.parent),
                'extension': file_path.suffix.lower(),
                'detected_format': FileWriter._determine_format(filename, None),
                'directory_exists': file_path.parent.exists(),
                'file_exists': file_path.exists()
            }
            
            if file_path.exists():
                info['size_bytes'] = file_path.stat().st_size
            
            return info
            
        except Exception as e:
            return {
                'original_filename': filename,
                'final_filename': filename,
                'error': str(e)
            }
    
    @staticmethod
    def get_supported_formats():
        """
        Get information about supported output file formats.
        
        Returns:
            Dictionary with format information
        """
        return {
            'excel_formats': list(FileWriter.EXCEL_FORMATS),
            'csv_formats': list(FileWriter.CSV_FORMATS),
            'tsv_formats': list(FileWriter.TSV_FORMATS),
            'all_formats': list(FileWriter.ALL_FORMATS),
            'supported_extensions': list(FileWriter.EXTENSION_TO_FORMAT.keys()),
            'extension_mapping': dict(FileWriter.EXTENSION_TO_FORMAT),
            'format_descriptions': {
                'xlsx': 'Excel 2007+ format (recommended)',
                'xls': 'Legacy Excel format',
                'xlsm': 'Excel with macros',
                'csv': 'Comma-separated values',
                'tsv': 'Tab-separated values (.tsv and .txt files)'
            },
            'features': {
                'excel': ['multi_sheet', 'active_sheet_control', 'rich_formatting_support'],
                'csv': ['custom_separators', 'encoding_options', 'universal_compatibility'],
                'tsv': ['tab_separated', 'encoding_options', 'simple_format']
            }
        }
    
    # =============================================================================
    # PRIVATE HELPER METHODS
    # =============================================================================
    
    @staticmethod
    def _validate_dataframe(data):
        """Validate that data is a proper DataFrame."""
        if not isinstance(data, pd.DataFrame):
            raise FileWriterError(f"Data must be a pandas DataFrame, got: {type(data)}")
        
        # Empty DataFrames are allowed - just warn
        if data.empty:
            logger.warning("Writing empty DataFrame")
    
    @staticmethod
    def _ensure_directory_exists(filename):
        """Ensure the output directory exists."""
        file_path = Path(filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _create_backup_if_exists(filename):
        """Create backup of existing file if it exists."""
        file_path = Path(filename)
        if file_path.exists():
            FileWriter.create_backup(filename)
    
    @staticmethod
    def _determine_format(filename, explicit_format: str):
        """
        Determine logical format from extension or explicit override.
        
        Returns logical format without dots: 'xlsx', 'csv', 'tsv', etc.
        """
        if explicit_format:
            explicit_lower = explicit_format.lower()
            if explicit_lower in FileWriter.ALL_FORMATS:
                return explicit_lower
            else:
                raise FileWriterError(f"Unsupported explicit format: {explicit_format}")
        
        # Auto-detect from file extension using mapping
        file_path = Path(filename)
        extension = file_path.suffix.lower()
        
        if extension in FileWriter.EXTENSION_TO_FORMAT:
            logical_format = FileWriter.EXTENSION_TO_FORMAT[extension]
            logger.debug(f"Extension {extension} → logical format {logical_format}")
            return logical_format
        else:
            # Unknown extension - default to Excel with warning
            logger.warning(f"Unknown file extension '{extension}' for '{filename}', defaulting to Excel format")
            return 'xlsx'
    
    @staticmethod
    def _write_excel_file(data, filename, sheet_name, index):
        """Write DataFrame to Excel file using ExcelWriter."""
        try:
            excel_writer = ExcelWriter()
            excel_writer.write_file(data, filename, sheet_name=sheet_name, index=index)
            
        except ExcelWriterError as e:
            raise FileWriterError(f"Excel writing error for '{filename}': {e}")
    
    @staticmethod
    def _write_csv_file(data: pd.DataFrame, filename, index, encoding, separator):
        """Write DataFrame to CSV file."""
        try:
            data.to_csv(
                filename,
                index=index,
                encoding=encoding,
                sep=separator,
                # Good CSV writing practices
                lineterminator='\n',  # Consistent line endings
                float_format='%.6g'   # Avoid excessive decimal places
            )
            
        except Exception as e:
            raise FileWriterError(f"CSV writing error for '{filename}': {e}")
    
    @staticmethod
    def _write_tsv_file(data: pd.DataFrame, filename, index, encoding):
        """Write DataFrame to TSV file."""
        try:
            data.to_csv(
                filename,
                index=index,
                encoding=encoding,
                sep='\t',
                # Good TSV writing practices
                lineterminator='\n',  # Consistent line endings
                float_format='%.6g'   # Avoid excessive decimal places
            )
            
        except Exception as e:
            raise FileWriterError(f"TSV writing error for '{filename}': {e}")
    
    @staticmethod
    def _set_active_sheet(filename, sheet_name):
        """Set active sheet in Excel file (requires openpyxl)."""
        try:
            import openpyxl
            
            workbook = openpyxl.load_workbook(filename)
            if sheet_name in workbook.sheetnames:
                workbook.active = workbook[sheet_name]
                workbook.save(filename)
                workbook.close()
                logger.debug(f"Set active sheet to '{sheet_name}' in '{filename}'")
            else:
                logger.warning(f"Sheet '{sheet_name}' not found in '{filename}', cannot set as active")
                
        except ImportError:
            logger.warning("openpyxl not available - cannot set active sheet")
        except Exception as e:
            logger.warning(f"Failed to set active sheet '{sheet_name}' in '{filename}': {e}")
