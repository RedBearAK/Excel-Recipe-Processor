"""
Direct Excel reader using openpyxl to avoid pandas auto-conversion.

excel_recipe_processor/readers/openpyxl_excel_reader.py

Reads Excel files directly using openpyxl to preserve original 
text formatting without pandas auto-conversion of dates, numbers, or 
creation of "Unnamed: X" columns. Supports both header-only reading
and full data analysis for column structure detection.
"""

import logging

from pathlib import Path
from typing import List, Optional

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False


logger = logging.getLogger(__name__)


class OpenpyxlExcelReaderError(Exception):
    """Raised when openpyxl Excel reading operations fail."""
    pass


class OpenpyxlExcelReader:
    """
    Direct Excel reader using openpyxl to avoid pandas conversions.
    
    Reads Excel files exactly as they appear, preserving original
    text format for dates and other values that pandas would auto-convert.
    Supports both header-only reading and full column data analysis.
    """
    
    @staticmethod
    def check_availability():
        """Check if openpyxl is available."""
        if not OPENPYXL_AVAILABLE:
            raise OpenpyxlExcelReaderError(
                "openpyxl is required for direct Excel reading but not available. "
                "Please install it with: pip install openpyxl"
            )
    
    @staticmethod
    def read_headers(file_path: str, sheet_name=None, header_row: int = 1) -> List[str]:
        """
        Read column headers from Excel file using openpyxl.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or None for active sheet
            header_row: Row number containing headers (1-based, default: 1)
            
        Returns:
            List of header names as they appear in Excel (strings)
            
        Raises:
            OpenpyxlExcelReaderError: If reading fails
        """
        OpenpyxlExcelReader.check_availability()
        
        file_path = Path(file_path)
        
        # Validate file
        if not file_path.exists():
            raise OpenpyxlExcelReaderError(f"Excel file not found: {file_path}")
        
        if not file_path.suffix.lower() in {'.xlsx', '.xls', '.xlsm', '.xlsb'}:
            raise OpenpyxlExcelReaderError(f"Not an Excel file: {file_path}")
        
        try:
            # Load workbook
            workbook = openpyxl.load_workbook(file_path, read_only=True)
            
            # Get worksheet - support both names and numeric indices
            if sheet_name is None:
                worksheet = workbook.active
                sheet_name = worksheet.title
            elif isinstance(sheet_name, int):
                # Numeric sheet reference (1-based like Excel UI)
                if sheet_name < 1 or sheet_name > len(workbook.sheetnames):
                    available_sheets = workbook.sheetnames
                    workbook.close()
                    raise OpenpyxlExcelReaderError(
                        f"Sheet index {sheet_name} out of range. Available sheets (1-{len(workbook.sheetnames)}): {available_sheets}"
                    )
                # Convert to 0-based index for Python
                worksheet = workbook.worksheets[sheet_name - 1]
                sheet_name = worksheet.title
            else:
                # Sheet name as string
                if sheet_name not in workbook.sheetnames:
                    available_sheets = workbook.sheetnames
                    workbook.close()
                    raise OpenpyxlExcelReaderError(
                        f"Sheet '{sheet_name}' not found. Available sheets: {available_sheets}"
                    )
                worksheet = workbook[sheet_name]
            
            # Read header row
            headers = []
            max_col = worksheet.max_column or 1
            
            for col in range(1, max_col + 1):
                cell = worksheet.cell(row=header_row, column=col)
                
                # Get raw cell value and convert to string
                if cell.value is None:
                    headers.append("")
                else:
                    # Convert to string preserving original format
                    # This keeps dates as they appear in Excel
                    headers.append(str(cell.value))
            
            workbook.close()
            
            logger.debug(f"Read {len(headers)} headers from {file_path}[{sheet_name}] row {header_row}")
            return headers
            
        except Exception as e:
            if hasattr(locals(), 'workbook'):
                workbook.close()
            
            if isinstance(e, OpenpyxlExcelReaderError):
                raise
            else:
                raise OpenpyxlExcelReaderError(f"Failed to read Excel headers: {e}")
    
    @staticmethod
    def read_headers_with_data_check(file_path: str, sheet_name=None, header_row: int = 1, 
                                   max_rows: int = 100000) -> dict:
        """
        Read headers and check which columns contain actual data.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or None for active sheet
            header_row: Row number containing headers (1-based, default: 1)
            max_rows: Maximum number of rows to scan (default: 100,000)
            
        Returns:
            Dictionary with headers and column analysis
        """
        OpenpyxlExcelReader.check_availability()
        
        file_path = Path(file_path)
        
        # Validate file
        if not file_path.exists():
            raise OpenpyxlExcelReaderError(f"Excel file not found: {file_path}")
        
        try:
            # Load workbook
            workbook = openpyxl.load_workbook(file_path, read_only=True)
            
            # Get worksheet - support both names and numeric indices
            if sheet_name is None:
                worksheet = workbook.active
                sheet_name = worksheet.title
            elif isinstance(sheet_name, int):
                # Numeric sheet reference (1-based like Excel UI)
                if sheet_name < 1 or sheet_name > len(workbook.sheetnames):
                    available_sheets = workbook.sheetnames
                    workbook.close()
                    raise OpenpyxlExcelReaderError(
                        f"Sheet index {sheet_name} out of range. Available sheets (1-{len(workbook.sheetnames)}): {available_sheets}"
                    )
                # Convert to 0-based index for Python
                worksheet = workbook.worksheets[sheet_name - 1]
                sheet_name = worksheet.title
            else:
                # Sheet name as string
                if sheet_name not in workbook.sheetnames:
                    available_sheets = workbook.sheetnames
                    workbook.close()
                    raise OpenpyxlExcelReaderError(
                        f"Sheet '{sheet_name}' not found. Available sheets: {available_sheets}"
                    )
                worksheet = workbook[sheet_name]
            
            # Read headers and analyze columns
            headers = []
            column_has_data = []
            max_col = worksheet.max_column or 1
            max_row = worksheet.max_row or header_row
            
            # Limit scanning to reasonable number of rows
            data_end_row = min(max_row, header_row + max_rows)
            
            for col in range(1, max_col + 1):
                # Get header
                header_cell = worksheet.cell(row=header_row, column=col)
                header_value = str(header_cell.value) if header_cell.value is not None else ""
                headers.append(header_value)
                
                # Check if column has any data in entire range
                has_data = False
                for row in range(header_row + 1, data_end_row + 1):
                    cell = worksheet.cell(row=row, column=col)
                    if cell.value is not None and str(cell.value).strip():
                        has_data = True
                        break
                
                column_has_data.append(has_data)
            
            workbook.close()
            
            # Analyze results
            empty_columns = [i for i, has_data in enumerate(column_has_data) if not has_data]
            scanned_data_rows = data_end_row - header_row
            
            logger.debug(f"Analyzed {len(headers)} columns from {file_path}[{sheet_name}], "
                        f"scanned {scanned_data_rows} data rows, "
                        f"found {len(empty_columns)} empty columns")
            
            return {
                'headers': headers,
                'column_has_data': column_has_data,
                'empty_column_indices': empty_columns,
                'scanned_data_rows': scanned_data_rows,
                'total_columns': len(headers)
            }
            
        except Exception as e:
            if hasattr(locals(), 'workbook'):
                workbook.close()
            
            if isinstance(e, OpenpyxlExcelReaderError):
                raise
            else:
                raise OpenpyxlExcelReaderError(f"Failed to read Excel with data check: {e}")
    
    @staticmethod
    def get_sheet_info(file_path: str) -> dict:
        """
        Get information about sheets in an Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            Dictionary with sheet information
            
        Raises:
            OpenpyxlExcelReaderError: If reading fails
        """
        OpenpyxlExcelReader.check_availability()
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise OpenpyxlExcelReaderError(f"Excel file not found: {file_path}")
        
        try:
            workbook = openpyxl.load_workbook(file_path, read_only=True)
            
            info = {
                'file_path': str(file_path),
                'sheet_names': workbook.sheetnames,
                'active_sheet': workbook.active.title,
                'sheet_count': len(workbook.sheetnames)
            }
            
            workbook.close()
            return info
            
        except Exception as e:
            if hasattr(locals(), 'workbook'):
                workbook.close()
            raise OpenpyxlExcelReaderError(f"Failed to get sheet info: {e}")
    
    @staticmethod
    def preview_headers(file_path: str, sheet_name=None, header_row: int = 1, 
                       max_preview: int = 10) -> dict:
        """
        Preview headers and some sample data from Excel file.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or None for active sheet
            header_row: Row number containing headers (1-based)
            max_preview: Maximum number of headers to show in preview
            
        Returns:
            Dictionary with preview information
        """
        OpenpyxlExcelReader.check_availability()
        
        try:
            headers = OpenpyxlExcelReader.read_headers(file_path, sheet_name, header_row)
            
            preview_headers = headers[:max_preview]
            if len(headers) > max_preview:
                preview_headers.append(f"... and {len(headers) - max_preview} more")
            
            return {
                'total_columns': len(headers),
                'header_row': header_row,
                'preview_headers': preview_headers,
                'full_headers': headers,
                'empty_headers_count': sum(1 for h in headers if not h.strip()),
                'sample_issues': OpenpyxlExcelReader._detect_header_issues(headers)
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'total_columns': 0,
                'preview_headers': [],
                'full_headers': []
            }
    
    @staticmethod
    def _detect_header_issues(headers: List[str]) -> List[str]:
        """
        Detect potential issues with headers.
        
        Args:
            headers: List of header strings
            
        Returns:
            List of issue descriptions
        """
        issues = []
        
        # Check for trailing empty headers
        trailing_empty = 0
        for header in reversed(headers):
            if not header.strip():
                trailing_empty += 1
            else:
                break
        
        if trailing_empty > 0:
            issues.append(f"{trailing_empty} trailing empty columns")
        
        # Check for duplicate headers
        non_empty_headers = [h for h in headers if h.strip()]
        if len(non_empty_headers) != len(set(non_empty_headers)):
            issues.append("duplicate header names detected")
        
        # Check for headers that look like dates
        date_like_headers = [h for h in headers if '/' in h and any(c.isdigit() for c in h)]
        if date_like_headers:
            issues.append(f"{len(date_like_headers)} headers look like dates")
        
        return issues


# Demonstration and testing functions
def demonstrate_openpyxl_vs_pandas():
    """
    Demonstrate the difference between openpyxl and pandas reading.
    """
    print("OpenpyxlExcelReader vs pandas.read_excel() comparison:")
    print("=" * 60)
    
    # Example of what each approach would return:
    print("Excel file with headers: Product_ID | 8/4/2025 | [empty] | Status")
    print()
    print("pandas.read_excel() result:")
    print("  ['Product_ID', '2025-08-04 00:00:00', 'Unnamed: 2', 'Status']")
    print()
    print("OpenpyxlExcelReader result:")
    print("  ['Product_ID', '8/4/2025', '', 'Status']")
    print()
    print("Benefits of openpyxl approach:")
    print("  ✓ Preserves original date format")
    print("  ✓ No 'Unnamed: X' auto-generation")
    print("  ✓ Empty columns stay empty") 
    print("  ✓ Exact text as it appears in Excel")
    print()
    print("Enhanced data checking:")
    print("  ✓ Detects columns with headers but no data")
    print("  ✓ Prevents 'ghost' columns in output")
    print("  ✓ Smart trimming based on both headers and content")


def demonstrate_data_checking():
    """
    Demonstrate the data checking functionality.
    """
    print("Data Checking Example:")
    print("=" * 30)
    print("Excel file structure:")
    print("  Headers: ['Product_ID', '8/4/2025', '', 'Status', 'Notes']")
    print("  Row 2:   ['P001',       '100',      '',  'Active', '']")
    print("  Row 3:   ['P002',       '200',      '',  'Active', '']")
    print("  Row 4:   ['P003',       '150',      '',  'Closed', '']")
    print()
    print("Analysis results:")
    print("  Column 1 (Product_ID): Has header ✓, Has data ✓ → Keep")
    print("  Column 2 (8/4/2025):   Has header ✓, Has data ✓ → Keep") 
    print("  Column 3 (''):         No header ✗,  No data ✗  → Drop")
    print("  Column 4 (Status):     Has header ✓, Has data ✓ → Keep")
    print("  Column 5 (Notes):      Has header ✓, No data ✗  → Drop (ghost column)")
    print()
    print("Final trimmed headers: ['Product_ID', '8/4/2025', '', 'Status']")


if __name__ == "__main__":
    demonstrate_openpyxl_vs_pandas()
    print()
    demonstrate_data_checking()


# End of file #
