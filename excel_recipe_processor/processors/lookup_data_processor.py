"""
Lookup data step processor for Excel automation recipes.

Handles looking up values from reference data sources, similar to VLOOKUP/INDEX-MATCH in Excel.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class LookupDataProcessor(BaseStepProcessor):
    """
    Processor for looking up values from reference data sources.
    
    Supports various lookup operations including:
    - VLOOKUP-style left joins
    - INDEX-MATCH operations
    - Multi-column lookups
    - Fuzzy matching
    - Default value handling for non-matches
    """
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the lookup operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            DataFrame with lookup values added
            
        Raises:
            StepProcessorError: If lookup fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Lookup data step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['lookup_source', 'lookup_key', 'source_key', 'lookup_columns'])
        
        lookup_source = self.get_config_value('lookup_source')
        lookup_key = self.get_config_value('lookup_key')
        source_key = self.get_config_value('source_key')
        lookup_columns = self.get_config_value('lookup_columns')
        join_type = self.get_config_value('join_type', 'left')
        handle_duplicates = self.get_config_value('handle_duplicates', 'first')
        case_sensitive = self.get_config_value('case_sensitive', True)
        default_values = self.get_config_value('default_values', {})
        add_prefix = self.get_config_value('add_prefix', '')
        add_suffix = self.get_config_value('add_suffix', '')
        
        # Validate configuration
        self._validate_lookup_config(data, lookup_source, lookup_key, source_key, lookup_columns)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Load or prepare lookup data
            lookup_data = self._prepare_lookup_data(lookup_source)
            
            # Perform the lookup operation
            result_data = self._perform_lookup(
                result_data, lookup_data, lookup_key, source_key, lookup_columns,
                join_type, handle_duplicates, case_sensitive, default_values,
                add_prefix, add_suffix
            )
            
            # Count successful lookups
            lookup_count = len(result_data)
            added_columns = len(lookup_columns)
            
            result_info = f"looked up {added_columns} columns for {lookup_count} rows"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error performing lookup in step '{self.step_name}': {e}")
    
    def _validate_lookup_config(self, df: pd.DataFrame, lookup_source, lookup_key: str,
                               source_key: str, lookup_columns) -> None:
        """
        Validate lookup configuration parameters.
        
        Args:
            df: Input DataFrame
            lookup_source: Lookup data source
            lookup_key: Key column in lookup data
            source_key: Key column in source data
            lookup_columns: Columns to lookup
        """
        # Validate source key exists in main data
        if not isinstance(source_key, str) or not source_key.strip():
            raise StepProcessorError("'source_key' must be a non-empty string")
        
        if source_key not in df.columns:
            available_columns = list(df.columns)
            raise StepProcessorError(
                f"Source key column '{source_key}' not found. "
                f"Available columns: {available_columns}"
            )
        
        # Validate lookup key
        if not isinstance(lookup_key, str) or not lookup_key.strip():
            raise StepProcessorError("'lookup_key' must be a non-empty string")
        
        # Validate lookup columns
        if isinstance(lookup_columns, str):
            lookup_columns = [lookup_columns]
        
        if not isinstance(lookup_columns, list):
            raise StepProcessorError("'lookup_columns' must be a string or list of strings")
        
        if len(lookup_columns) == 0:
            raise StepProcessorError("'lookup_columns' list cannot be empty")
        
        for col in lookup_columns:
            if not isinstance(col, str) or not col.strip():
                raise StepProcessorError(f"Lookup column must be a non-empty string, got: {col}")
    
    def _prepare_lookup_data(self, lookup_source) -> pd.DataFrame:
        """
        Prepare lookup data from various source types.
        
        Args:
            lookup_source: Source of lookup data (DataFrame, dict, file path, etc.)
            
        Returns:
            DataFrame containing lookup data
        """
        if isinstance(lookup_source, pd.DataFrame):
            # Direct DataFrame
            return lookup_source.copy()
        
        elif isinstance(lookup_source, dict):
            # Dictionary data
            if 'data' in lookup_source:
                # Dictionary with data key
                data = lookup_source['data']
                if isinstance(data, dict):
                    # Convert dict to DataFrame
                    return pd.DataFrame(data)
                elif isinstance(data, list):
                    # List of records
                    return pd.DataFrame(data)
                else:
                    raise StepProcessorError(f"Invalid data format in lookup_source: {type(data)}")
            else:
                # Treat whole dict as data
                return pd.DataFrame(lookup_source)
        
        elif isinstance(lookup_source, str):
            # File path
            try:
                from excel_recipe_processor.readers.excel_reader import ExcelReader
                reader = ExcelReader()
                
                if lookup_source.endswith(('.xlsx', '.xls', '.xlsm')):
                    # Excel file
                    sheet_name = self.get_config_value('lookup_sheet', 0)
                    return reader.read_file(lookup_source, sheet_name=sheet_name)
                else:
                    # Assume CSV
                    return pd.read_csv(lookup_source)
                    
            except Exception as e:
                raise StepProcessorError(f"Error reading lookup file '{lookup_source}': {e}")
        
        elif isinstance(lookup_source, list):
            # List of records
            return pd.DataFrame(lookup_source)
        
        else:
            raise StepProcessorError(
                f"Unsupported lookup_source type: {type(lookup_source)}. "
                "Use DataFrame, dict, file path, or list of records."
            )
    
    def _perform_lookup(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                       lookup_key: str, source_key: str, lookup_columns: list,
                       join_type: str, handle_duplicates: str, case_sensitive: bool,
                       default_values: dict, add_prefix: str, add_suffix: str) -> pd.DataFrame:
        """
        Perform the actual lookup operation.
        
        Args:
            main_data: Main DataFrame to add lookup values to
            lookup_data: Lookup DataFrame containing reference data
            lookup_key: Key column in lookup data
            source_key: Key column in main data
            lookup_columns: Columns to lookup from lookup data
            join_type: Type of join ('left', 'inner', 'outer')
            handle_duplicates: How to handle duplicate keys ('first', 'last', 'error')
            case_sensitive: Whether key matching should be case sensitive
            default_values: Default values for non-matches
            add_prefix: Prefix to add to lookup column names
            add_suffix: Suffix to add to lookup column names
            
        Returns:
            DataFrame with lookup values added
        """
        # Validate lookup key exists in lookup data
        if lookup_key not in lookup_data.columns:
            available_columns = list(lookup_data.columns)
            raise StepProcessorError(
                f"Lookup key column '{lookup_key}' not found in lookup data. "
                f"Available columns: {available_columns}"
            )
        
        # Validate lookup columns exist in lookup data
        missing_columns = []
        for col in lookup_columns:
            if col not in lookup_data.columns:
                missing_columns.append(col)
        
        if missing_columns:
            available_columns = list(lookup_data.columns)
            raise StepProcessorError(
                f"Lookup columns not found in lookup data: {missing_columns}. "
                f"Available columns: {available_columns}"
            )
        
        # Prepare data for joining
        join_data = main_data.copy()
        lookup_subset = lookup_data.copy()
        
        # Handle case sensitivity
        if not case_sensitive:
            # Create temporary columns with lowercase values for joining
            join_key_temp = f"{source_key}_temp_join"
            lookup_key_temp = f"{lookup_key}_temp_join"
            
            join_data[join_key_temp] = join_data[source_key].astype(str).str.lower()
            lookup_subset[lookup_key_temp] = lookup_subset[lookup_key].astype(str).str.lower()
            
            join_on_main = join_key_temp
            join_on_lookup = lookup_key_temp
        else:
            join_on_main = source_key
            join_on_lookup = lookup_key
        
        # Handle duplicates in lookup data
        if handle_duplicates == 'first':
            lookup_subset = lookup_subset.drop_duplicates(subset=[join_on_lookup], keep='first')
        elif handle_duplicates == 'last':
            lookup_subset = lookup_subset.drop_duplicates(subset=[join_on_lookup], keep='last')
        elif handle_duplicates == 'error':
            duplicate_count = lookup_subset[join_on_lookup].duplicated().sum()
            if duplicate_count > 0:
                duplicates = lookup_subset[lookup_subset[join_on_lookup].duplicated(keep=False)][join_on_lookup].unique()
                raise StepProcessorError(
                    f"Duplicate keys found in lookup data: {list(duplicates)[:5]}... "
                    f"({duplicate_count} total duplicates)"
                )
        else:
            raise StepProcessorError(
                f"Unknown handle_duplicates option: '{handle_duplicates}'. "
                "Use 'first', 'last', or 'error'."
            )
        
        # Prepare columns to join (include the join key plus lookup columns)
        join_columns = [join_on_lookup] + lookup_columns
        lookup_subset = lookup_subset[join_columns]
        
        # Rename lookup columns with prefix/suffix
        column_mapping = {}
        for col in lookup_columns:
            new_name = f"{add_prefix}{col}{add_suffix}"
            column_mapping[col] = new_name
        
        if column_mapping:
            lookup_subset = lookup_subset.rename(columns=column_mapping)
            final_lookup_columns = list(column_mapping.values())
        else:
            final_lookup_columns = lookup_columns
        
        # Perform the join
        try:
            if join_type == 'left':
                result = join_data.merge(
                    lookup_subset,
                    left_on=join_on_main,
                    right_on=join_on_lookup,
                    how='left',
                    suffixes=('', '_lookup_dup')
                )
            elif join_type == 'inner':
                result = join_data.merge(
                    lookup_subset,
                    left_on=join_on_main,
                    right_on=join_on_lookup,
                    how='inner',
                    suffixes=('', '_lookup_dup')
                )
            elif join_type == 'outer':
                result = join_data.merge(
                    lookup_subset,
                    left_on=join_on_main,
                    right_on=join_on_lookup,
                    how='outer',
                    suffixes=('', '_lookup_dup')
                )
            else:
                raise StepProcessorError(
                    f"Unknown join_type: '{join_type}'. Use 'left', 'inner', or 'outer'."
                )
            
            # Remove temporary join columns if created
            if not case_sensitive:
                if join_key_temp in result.columns:
                    result = result.drop(columns=[join_key_temp])
                if lookup_key_temp in result.columns:
                    result = result.drop(columns=[lookup_key_temp])
            
            # Apply default values for non-matches
            if default_values:
                for col, default_val in default_values.items():
                    # Find the actual column name (might have prefix/suffix)
                    target_col = None
                    if col in final_lookup_columns:
                        target_col = col
                    else:
                        # Try to find with prefix/suffix
                        for lookup_col in final_lookup_columns:
                            if lookup_col.replace(add_prefix, '').replace(add_suffix, '') == col:
                                target_col = lookup_col
                                break
                    
                    if target_col and target_col in result.columns:
                        result[target_col] = result[target_col].fillna(default_val)
            
            # Log lookup statistics
            matched_count = 0
            for col in final_lookup_columns:
                if col in result.columns:
                    matched_count = result[col].notna().sum()
                    break
            
            total_count = len(result)
            unmatched_count = total_count - matched_count
            
            logger.debug(f"Lookup results: {matched_count} matched, {unmatched_count} unmatched")
            
            return result
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error performing join operation: {e}")
    
    def create_vlookup_style(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                           lookup_key: str, source_key: str, return_column: str,
                           default_value=None) -> pd.DataFrame:
        """
        Create a simple VLOOKUP-style lookup operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup table DataFrame
            lookup_key: Key column in lookup table
            source_key: Key column in main data
            return_column: Column to return from lookup table
            default_value: Value to use when no match found
            
        Returns:
            DataFrame with lookup column added
        """
        try:
            # Perform simple left join
            result = main_data.merge(
                lookup_data[[lookup_key, return_column]],
                left_on=source_key,
                right_on=lookup_key,
                how='left'
            )
            
            # Remove duplicate lookup key column
            if lookup_key in result.columns and lookup_key != source_key:
                result = result.drop(columns=[lookup_key])
            
            # Apply default value
            if default_value is not None:
                result[return_column] = result[return_column].fillna(default_value)
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error in VLOOKUP-style operation: {e}")
    
    def create_index_match_style(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                               lookup_key: str, source_key: str, return_columns: list,
                               exact_match: bool = True) -> pd.DataFrame:
        """
        Create an INDEX-MATCH style lookup operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup table DataFrame
            lookup_key: Key column in lookup table
            source_key: Key column in main data
            return_columns: List of columns to return from lookup table
            exact_match: Whether to require exact matches
            
        Returns:
            DataFrame with lookup columns added
        """
        try:
            if exact_match:
                # Standard exact match join
                result = main_data.merge(
                    lookup_data[[lookup_key] + return_columns],
                    left_on=source_key,
                    right_on=lookup_key,
                    how='left'
                )
            else:
                # Approximate match (find closest)
                # This is more complex and would require additional logic
                raise StepProcessorError("Approximate match not yet implemented")
            
            # Remove duplicate lookup key column
            if lookup_key in result.columns and lookup_key != source_key:
                result = result.drop(columns=[lookup_key])
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error in INDEX-MATCH style operation: {e}")
    
    def create_multi_column_lookup(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                                 lookup_keys: list, source_keys: list, return_columns: list) -> pd.DataFrame:
        """
        Create a multi-column lookup operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup table DataFrame
            lookup_keys: List of key columns in lookup table
            source_keys: List of key columns in main data
            return_columns: List of columns to return from lookup table
            
        Returns:
            DataFrame with lookup columns added
        """
        if len(lookup_keys) != len(source_keys):
            raise StepProcessorError("Number of lookup keys must match number of source keys")
        
        try:
            # Perform multi-column join
            result = main_data.merge(
                lookup_data[lookup_keys + return_columns],
                left_on=source_keys,
                right_on=lookup_keys,
                how='left'
            )
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error in multi-column lookup: {e}")
    
    def analyze_lookup_potential(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                               main_key: str, lookup_key: str) -> dict:
        """
        Analyze the potential for a successful lookup operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup DataFrame
            main_key: Key column in main data
            lookup_key: Key column in lookup data
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            'main_data_rows': len(main_data),
            'lookup_data_rows': len(lookup_data),
            'main_key_unique': main_data[main_key].nunique(),
            'lookup_key_unique': lookup_data[lookup_key].nunique(),
            'main_key_nulls': main_data[main_key].isnull().sum(),
            'lookup_key_nulls': lookup_data[lookup_key].isnull().sum(),
        }
        
        # Check for potential matches
        main_values = set(main_data[main_key].dropna().astype(str))
        lookup_values = set(lookup_data[lookup_key].dropna().astype(str))
        
        matches = main_values.intersection(lookup_values)
        analysis['potential_matches'] = len(matches)
        analysis['match_rate'] = len(matches) / len(main_values) if main_values else 0
        
        # Check for duplicates
        analysis['lookup_duplicates'] = lookup_data[lookup_key].duplicated().sum()
        
        # Recommendations
        recommendations = []
        if analysis['match_rate'] < 0.5:
            recommendations.append("Low match rate - check key formats and data quality")
        if analysis['lookup_duplicates'] > 0:
            recommendations.append("Duplicate keys in lookup data - consider deduplication")
        if analysis['main_key_nulls'] > 0:
            recommendations.append("Null values in main key - these will not match")
        
        analysis['recommendations'] = recommendations
        
        return analysis
    
    def get_supported_join_types(self) -> list:
        """
        Get list of supported join types.
        
        Returns:
            List of supported join type strings
        """
        return ['left', 'inner', 'outer']
    
    def get_supported_duplicate_handling(self) -> list:
        """
        Get list of supported duplicate handling options.
        
        Returns:
            List of supported duplicate handling strings
        """
        return ['first', 'last', 'error']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'XLOOKUP-equivalent data enrichment with flexible lookup operations',
            'lookup_features': [
                'vlookup_style', 'xlookup_equivalent', 'multi_column_lookup',
                'case_insensitive_matching', 'default_values', 'duplicate_handling',
                'multiple_data_sources', 'column_prefix_suffix'
            ],
            'join_types': self.get_supported_join_types(),
            'duplicate_handling': self.get_supported_duplicate_handling(),
            'data_sources': ['dataframe', 'dictionary', 'excel_file', 'csv_file', 'list_of_records'],
            'helper_methods': [
                'create_vlookup_style', 'create_index_match_style', 
                'create_multi_column_lookup', 'analyze_lookup_potential'
            ],
            'examples': {
                'product_lookup': "Enrich orders with product details",
                'customer_lookup': "Add customer information to transactions",
                'multi_lookup': "Chain multiple lookups for comprehensive enrichment"
            }
        }
