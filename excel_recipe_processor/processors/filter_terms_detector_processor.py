"""
Filter terms detector processor for Excel automation recipes.

excel_recipe_processor/processors/filter_terms_detector_processor.py

Analyzes differences between raw and filtered datasets to identify potential
filter terms using n-gram analysis and statistical comparison techniques.
"""

import pandas as pd
import logging

from typing import Any, Optional

from sklearn.feature_extraction.text import CountVectorizer

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError
from excel_recipe_processor.core.stage_manager import StageManager


logger = logging.getLogger(__name__)


class FilterTermsDetectorProcessor(BaseStepProcessor):
    """
    Processor for detecting potential filter terms by comparing raw vs filtered datasets.
    
    Uses n-gram analysis to identify words and phrases that appear frequently in
    raw data but are absent or rare in filtered data, suggesting they were used
    as filter criteria during manual data processing.
    """
    
    @classmethod
    def get_minimal_config(cls):
        """
        Get the minimal configuration required to instantiate this processor.
        
        Returns:
            Dictionary with minimal configuration fields
        """
        return {
            'raw_stage': 'stg_raw_data_imported',
            'filtered_stage': 'stg_filtered_data_final',
            'text_columns': ['notes', 'description']
        }

    def __init__(self, step_config: dict):
        """Initialize the filter terms detector processor."""
        super().__init__(step_config)
        
        # Validate required fields - text_columns is conditional
        required_fields = ['raw_stage', 'filtered_stage']
        self.validate_required_fields(required_fields)
        
        # Get configuration values
        self.raw_stage = self.get_config_value('raw_stage')
        self.filtered_stage = self.get_config_value('filtered_stage')
        self.text_columns = self._normalize_text_columns(self.get_config_value('text_columns', []))
        
        # Optional configuration with defaults
        self.ngram_range = self._normalize_ngram_range(self.get_config_value('ngram_range', (1, 4)))
        self.min_frequency = self.get_config_value('min_frequency', 2)
        self.max_features = self.get_config_value('max_features', 10000)
        self.score_threshold = self.get_config_value('score_threshold', 0.1)
        self.categorical_columns = self.get_config_value('categorical_columns', [])
        self.custom_stop_words = self.get_config_value('custom_stop_words', [])
        self.exclude_columns = self.get_config_value('exclude_columns', [])
        
        # NEW: Read auto_detect_columns configuration
        self.auto_detect_columns = self.get_config_value('auto_detect_columns', False)
        
        # Determine if auto-discovery should be enabled
        if self.auto_detect_columns or (not self.text_columns and not self.categorical_columns):
            self.auto_discover_columns = True
            logger.info("Auto-detection enabled - will discover text and categorical columns")
        else:
            self.auto_discover_columns = False
        
        # Validate configuration
        self._validate_config()

    def _auto_detect_column_types(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame, common_columns: list) -> tuple[list, list]:
        """
        Auto-detect text and categorical columns based on data content and types.
        
        Args:
            raw_data: Raw dataset for analysis
            filtered_data: Filtered dataset for analysis
            common_columns: List of string column names that exist in both datasets
            
        Returns:
            Tuple of (detected_text_columns, detected_categorical_columns)
        """
        logger.info("Starting auto-detection of column types...")
        
        # Filter out excluded columns
        available_columns = [col for col in common_columns if col not in self.exclude_columns]
        logger.info(f"Available columns after exclusions: {len(available_columns)} of {len(common_columns)}")
        
        detected_text_columns = []
        detected_categorical_columns = []
        
        for column in available_columns:
            try:
                # Access column data using string column name
                # (The actual DataFrame might have datetime column names that we converted to strings)
                column_data = None
                
                # Try to find the actual column in the raw data
                for actual_col in raw_data.columns:
                    if str(actual_col) == column:
                        column_data = raw_data[actual_col].dropna()
                        break
                
                if column_data is None or len(column_data) == 0:
                    logger.debug(f"Column '{column}': not found or empty - skipping")
                    continue
                
                # Determine data type
                dtype = column_data.dtype
                unique_count = column_data.nunique()
                total_count = len(column_data)
                
                # Calculate uniqueness ratio
                uniqueness_ratio = unique_count / total_count if total_count > 0 else 0
                
                # Check if column contains primarily text content
                is_text_like = self._is_text_column(column_data, column)
                is_categorical_like = self._is_categorical_column(column_data, column, uniqueness_ratio)
                
                if is_text_like:
                    detected_text_columns.append(column)
                    logger.info(f"Column '{column}': detected as TEXT (dtype={dtype}, unique_ratio={uniqueness_ratio:.3f})")
                elif is_categorical_like:
                    detected_categorical_columns.append(column)
                    logger.info(f"Column '{column}': detected as CATEGORICAL (dtype={dtype}, unique={unique_count}, ratio={uniqueness_ratio:.3f})")
                else:
                    logger.debug(f"Column '{column}': not suitable for analysis (dtype={dtype}, unique_ratio={uniqueness_ratio:.3f})")
                    
            except Exception as e:
                logger.warning(f"Error analyzing column '{column}' for auto-detection: {e}")
                continue
        
        logger.info(f"Auto-detection completed: {len(detected_text_columns)} text columns, {len(detected_categorical_columns)} categorical columns")
        return detected_text_columns, detected_categorical_columns

    def _is_text_column(self, column_data: pd.Series, column_name: str) -> bool:
        """
        Determine if a column should be treated as a text column for n-gram analysis.
        
        Args:
            column_data: Column data to analyze
            column_name: Name of the column
            
        Returns:
            True if column should be analyzed as text
        """
        # Check for obvious text column names
        text_indicators = ['note', 'comment', 'description', 'name', 'title', 'component', 
                        'contract', 'customer', 'destination', 'origin', 'product']
        
        column_lower = column_name.lower()
        has_text_indicator = any(indicator in column_lower for indicator in text_indicators)
        
        # Analyze content
        if column_data.dtype == 'object':
            # Sample some values to check text characteristics
            sample_values = column_data.head(100).astype(str)
            
            # Calculate average length
            avg_length = sample_values.str.len().mean()
            
            # Check for multi-word content
            has_spaces = sample_values.str.contains(' ').any()
            
            # Text columns typically have longer strings and/or spaces
            is_text_content = avg_length > 10 or has_spaces
            
            return has_text_indicator or is_text_content
        
        return False

    def _is_categorical_column(self, column_data: pd.Series, column_name: str, uniqueness_ratio: float) -> bool:
        """
        Determine if a column should be treated as categorical for filter detection.
        Enhanced to avoid problematic mixed-type columns.
        
        Args:
            column_data: Column data to analyze
            column_name: Name of the column
            uniqueness_ratio: Ratio of unique values to total values
            
        Returns:
            True if column should be analyzed as categorical
        """
        # Check for obvious categorical column names
        categorical_indicators = ['status', 'type', 'category', 'species', 'carrier', 'workflow', 
                                'uom', 'term', 'region', 'priority', 'grade']
        
        column_lower = column_name.lower()
        has_categorical_indicator = any(indicator in column_lower for indicator in categorical_indicators)
        
        # Categorical columns should have relatively few unique values
        is_low_cardinality = uniqueness_ratio < 0.1  # Less than 10% unique values
        
        # For small datasets, use absolute thresholds
        unique_count = column_data.nunique()
        is_small_vocabulary = unique_count <= 50
        
        # Must be object type or string-like to be meaningful for filtering
        is_suitable_type = column_data.dtype == 'object'
        
        # ENHANCED: Check for problematic mixed types
        if is_suitable_type:
            # Sample some values to check for datetime contamination
            sample_values = column_data.dropna().head(20)
            has_datetime_objects = any(hasattr(val, 'strftime') for val in sample_values)
            has_string_objects = any(isinstance(val, str) for val in sample_values)
            
            # If we have both datetime and string objects, this might be problematic
            # Only include if it's clearly categorical by name or very low cardinality
            if has_datetime_objects and has_string_objects:
                if not has_categorical_indicator and not (unique_count <= 20):
                    logger.debug(f"Skipping mixed datetime/string column '{column_name}' for categorical analysis")
                    return False
        
        return is_suitable_type and (has_categorical_indicator or is_low_cardinality or is_small_vocabulary)

    def _normalize_text_columns(self, text_columns) -> list:
        """
        Normalize text_columns to always be a list.
        
        Args:
            text_columns: Single column name or list of column names
            
        Returns:
            List of column names
        """
        if isinstance(text_columns, str):
            return [text_columns]
        elif isinstance(text_columns, list):
            return text_columns
        else:
            raise StepProcessorError(f"text_columns must be string or list, got {type(text_columns)}")
    
    def _normalize_ngram_range(self, ngram_range) -> tuple:
        """
        Normalize ngram_range to always be a tuple (scikit-learn requirement).
        
        Args:
            ngram_range: Tuple or list with [min_n, max_n]
            
        Returns:
            Tuple of (min_n, max_n)
        """
        if isinstance(ngram_range, tuple):
            return ngram_range
        elif isinstance(ngram_range, list) and len(ngram_range) == 2:
            return tuple(ngram_range)
        else:
            raise StepProcessorError(f"ngram_range must be tuple or list of 2 integers, got {type(ngram_range)}")
    
    def _validate_config(self) -> None:
        """Validate processor configuration."""
        # Validate ngram_range
        if not isinstance(self.ngram_range, (tuple, list)) or len(self.ngram_range) != 2:
            raise StepProcessorError("ngram_range must be a tuple/list of 2 integers")
        
        min_n, max_n = self.ngram_range
        if not isinstance(min_n, int) or not isinstance(max_n, int):
            raise StepProcessorError("ngram_range values must be integers")
        
        if min_n < 1 or max_n < min_n or max_n > 6:
            raise StepProcessorError("ngram_range must be (1,n) where n >= 1 and n <= 6")
        
        # Validate other numeric parameters
        if not isinstance(self.min_frequency, int) or self.min_frequency < 1:
            raise StepProcessorError("min_frequency must be a positive integer")
        
        if not isinstance(self.max_features, int) or self.max_features < 100:
            raise StepProcessorError("max_features must be an integer >= 100")
        
        if not isinstance(self.score_threshold, (int, float)) or self.score_threshold < 0:
            raise StepProcessorError("score_threshold must be a non-negative number")

    def execute(self, source_stage_data: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Execute filter terms detection analysis."""
        try:
            self.log_step_start()
            
            # Load datasets
            raw_data = StageManager.load_stage(self.raw_stage)
            filtered_data = StageManager.load_stage(self.filtered_stage)
            
            logger.info(f"Loaded raw data: {len(raw_data)} rows, {len(raw_data.columns)} columns")
            logger.info(f"Loaded filtered data: {len(filtered_data)} rows, {len(filtered_data.columns)} columns")
            
            # Find common columns - SAFELY CONVERT ALL COLUMN NAMES TO STRINGS
            raw_columns = set(str(col) for col in raw_data.columns)
            filtered_columns = set(str(col) for col in filtered_data.columns)
            common_columns = list(raw_columns.intersection(filtered_columns))
            
            # Now we can safely sort string column names
            logger.info(f"Raw data columns: {sorted(raw_columns)}")
            logger.info(f"Filtered data columns: {sorted(filtered_columns)}")
            logger.info(f"Common columns ({len(common_columns)}): {sorted(common_columns)}")
            logger.info(f"Raw-only columns: {sorted(raw_columns - filtered_columns)}")
            logger.info(f"Filtered-only columns: {sorted(filtered_columns - raw_columns)}")
            
            # Determine which columns to analyze
            if self.auto_discover_columns:
                # Auto-detect column types - pass original DataFrames but use string column names
                detected_text_columns, detected_categorical_columns = self._auto_detect_column_types(
                    raw_data, filtered_data, common_columns)
                
                # Use detected columns, but also include any explicitly specified columns
                valid_text_columns = list(set(self.text_columns + detected_text_columns))
                valid_categorical_columns = list(set(self.categorical_columns + detected_categorical_columns))
            else:
                # Use only explicitly specified columns - ensure they're strings for comparison
                valid_text_columns = [col for col in self.text_columns if str(col) in common_columns]
                valid_categorical_columns = [col for col in self.categorical_columns if str(col) in common_columns]
            
            logger.info(f"Configured text_columns: {self.text_columns}")
            logger.info(f"Configured categorical_columns: {self.categorical_columns}")
            logger.info(f"Valid text columns (exist in both datasets): {valid_text_columns}")
            logger.info(f"Valid categorical columns (exist in both datasets): {valid_categorical_columns}")
            
            # Update instance variables for analysis methods
            self.text_columns = valid_text_columns
            self.categorical_columns = valid_categorical_columns
            
            # Validate that we have columns to analyze
            if not valid_text_columns and not valid_categorical_columns:
                logger.warning("No valid columns found for analysis - will return empty results")
                logger.warning("Suggestion: Check if your text_columns and categorical_columns match the common columns listed above")
                
            # Perform the analysis
            analysis_results = self._analyze_filter_patterns(raw_data, filtered_data)
            
            # DEBUG: Show analysis results summary
            logger.info(f"Analysis results keys: {list(analysis_results.keys())}")
            for key, value in analysis_results.items():
                if isinstance(value, dict):
                    logger.info(f"Analysis results['{key}']: {len(value)} items")
                else:
                    logger.info(f"Analysis results['{key}']: {value}")
            
            # Convert results to DataFrame format
            results_df = self._create_results_dataframe(analysis_results)
            
            self.log_step_complete(f"detected {len(results_df)} potential filter terms")
            return results_df
            
        except Exception as e:
            self.log_step_error(e)
            raise StepProcessorError(f"Filter terms detection failed: {e}")

    def _analyze_filter_patterns(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame):
            """
            Analyze filter patterns between raw and filtered datasets with comprehensive debugging.
            
            Args:
                raw_data: Original unfiltered dataset
                filtered_data: Processed/filtered dataset
                
            Returns:
                Dictionary containing analysis results
            """
            logger.info("DEBUG: Starting _analyze_filter_patterns method")
            
            # Guard clauses for input validation
            if not isinstance(raw_data, pd.DataFrame):
                raise StepProcessorError(f"raw_data must be DataFrame, got {type(raw_data)}")
            if not isinstance(filtered_data, pd.DataFrame):
                raise StepProcessorError(f"filtered_data must be DataFrame, got {type(filtered_data)}")
            
            logger.info("DEBUG: Input validation passed")
            
            try:
                raw_rows = len(raw_data)
                logger.info(f"DEBUG: Got raw_rows = {raw_rows}")
            except Exception as e:
                logger.error(f"DEBUG: Error getting raw_rows: {e}")
                raise
            
            try:
                filtered_rows = len(filtered_data)
                logger.info(f"DEBUG: Got filtered_rows = {filtered_rows}")
            except Exception as e:
                logger.error(f"DEBUG: Error getting filtered_rows: {e}")
                raise
            
            try:
                rows_removed = raw_rows - filtered_rows
                logger.info(f"DEBUG: Calculated rows_removed = {rows_removed}")
            except Exception as e:
                logger.error(f"DEBUG: Error calculating rows_removed: {e}")
                raise
            
            logger.info("DEBUG: Basic calculations completed")
            
            try:
                results = {
                    'categorical_analysis': {},
                    'text_analysis': {},
                    'summary': {
                        'raw_rows': raw_rows,
                        'filtered_rows': filtered_rows,
                        'rows_removed': rows_removed,
                        'removal_percentage': (rows_removed / raw_rows * 100) if raw_rows > 0 else 0
                    }
                }
                logger.info("DEBUG: Results dictionary created")
            except Exception as e:
                logger.error(f"DEBUG: Error creating results dictionary: {e}")
                raise
            
            # Analyze categorical columns (simple value comparisons)
            if self.categorical_columns:
                logger.info(f"DEBUG: Starting categorical analysis for {len(self.categorical_columns)} columns")
                try:
                    results['categorical_analysis'] = self._analyze_categorical_columns(raw_data, filtered_data)
                    logger.info("DEBUG: Categorical analysis completed")
                except Exception as e:
                    logger.error(f"DEBUG: Error in categorical analysis: {e}")
                    raise
            else:
                logger.info("DEBUG: No categorical columns specified, skipping categorical analysis")
            
            # Analyze text columns using n-gram analysis  
            if self.text_columns:
                logger.info(f"DEBUG: Starting text analysis for {len(self.text_columns)} columns")
                try:
                    results['text_analysis'] = self._analyze_text_columns(raw_data, filtered_data)
                    logger.info("DEBUG: Text analysis completed")
                except Exception as e:
                    logger.error(f"DEBUG: Error in text analysis: {e}")
                    raise
            else:
                logger.info("DEBUG: No text columns specified, skipping text analysis")
            
            logger.info("DEBUG: _analyze_filter_patterns completed successfully")
            return results

    def _analyze_categorical_columns(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame):
        """
        Analyze categorical columns with enhanced vocabulary-aware scoring.
        Fixed to handle mixed datetime/string data types safely.
        
        Args:
            raw_data: Original dataset
            filtered_data: Filtered dataset
            
        Returns:
            Dictionary with categorical analysis results including enhanced scoring
        """
        # Guard clauses for input validation
        if not isinstance(raw_data, pd.DataFrame):
            raise StepProcessorError(f"raw_data must be DataFrame, got {type(raw_data)}")
        if not isinstance(filtered_data, pd.DataFrame):
            raise StepProcessorError(f"filtered_data must be DataFrame, got {type(filtered_data)}")
        
        logger.info("Starting categorical analysis with enhanced scoring")
        
        categorical_results = {}
        raw_columns = raw_data.columns
        filtered_columns = filtered_data.columns
        
        for column in self.categorical_columns:
            if column not in raw_columns:
                logger.warning(f"Categorical column '{column}' not found in raw data")
                continue
            
            if column not in filtered_columns:
                logger.warning(f"Categorical column '{column}' not found in filtered data")
                continue
            
            try:
                logger.debug(f"Analyzing categorical column: '{column}'")
                
                # Get column data
                raw_column_data = raw_data[column]
                filtered_column_data = filtered_data[column]
                
                # Safely convert all values to strings, handling ALL datetime-like objects
                raw_values = set()
                for val in raw_column_data.dropna():
                    str_val = self._safe_value_to_string(val)
                    if str_val is not None:
                        raw_values.add(str_val)
                
                filtered_values = set()
                for val in filtered_column_data.dropna():
                    str_val = self._safe_value_to_string(val)
                    if str_val is not None:
                        filtered_values.add(str_val)
                
                # Calculate vocabulary metrics for enhanced scoring
                total_vocabulary_size = len(raw_values)
                removed_values = raw_values - filtered_values
                retained_values = raw_values & filtered_values
                
                if removed_values:
                    # Enhanced scoring based on vocabulary complexity
                    # Smaller vocabularies get higher confidence scores
                    vocabulary_boost = max(1.0, 50.0 / total_vocabulary_size)  # Small vocab = big boost
                    removal_ratio = len(removed_values) / total_vocabulary_size
                    
                    # Base confidence starts high for categorical (they're usually intentional)
                    base_confidence = 85.0
                    
                    # Enhanced confidence calculation
                    enhanced_confidence = base_confidence + (vocabulary_boost * 20.0) + (removal_ratio * 30.0)
                    
                    # Safe sorting with comprehensive error handling
                    try:
                        # All values should be strings now, but double-check
                        sorted_removed = self._safe_sort_values(list(removed_values))
                        sorted_retained = self._safe_sort_values(list(retained_values))
                    except Exception as sort_error:
                        logger.warning(f"Could not sort values for column '{column}': {sort_error}")
                        # Fallback: convert to list without sorting
                        sorted_removed = list(removed_values)
                        sorted_retained = list(retained_values)
                    
                    categorical_results[column] = {
                        'removed_values': sorted_removed,
                        'retained_values': sorted_retained,
                        'removal_count': len(removed_values),
                        'total_vocabulary_size': total_vocabulary_size,
                        'vocabulary_boost': round(vocabulary_boost, 2),
                        'removal_ratio': round(removal_ratio, 3),
                        'enhanced_confidence': round(enhanced_confidence, 1),
                        'filter_confidence': 'high'  # Legacy field
                    }
                    
                    logger.info(f"Column '{column}': {len(removed_values)} values removed from vocabulary of {total_vocabulary_size} (confidence: {enhanced_confidence:.1f})")
                else:
                    logger.debug(f"Column '{column}': no values removed")
                    
            except Exception as column_error:
                logger.error(f"Error analyzing categorical column '{column}': {column_error}")
                # Continue with other columns instead of failing completely
                continue
        
        logger.info(f"Enhanced categorical analysis completed: {len(categorical_results)} columns with results")
        return categorical_results

    def _safe_value_to_string(self, val) -> str:
        """
        Safely convert any value to a string, handling all datetime types and edge cases.
        
        Args:
            val: Value to convert (any type)
            
        Returns:
            String representation of the value, or None if conversion fails
        """
        try:
            # Handle pandas NaT and NaN
            if pd.isna(val):
                return None
            
            # Handle datetime objects (including pandas Timestamp)
            if hasattr(val, 'strftime'):
                if hasattr(val, 'hour'):  # datetime with time
                    return val.strftime('%Y-%m-%d %H:%M:%S')
                else:  # date only
                    return val.strftime('%Y-%m-%d')
            
            # Handle pandas Timestamp specifically (redundant but safe)
            if hasattr(val, 'isoformat'):
                try:
                    return val.isoformat()
                except:
                    pass
            
            # Handle numeric types that might be confused as datetime
            if isinstance(val, (int, float)) and not pd.isna(val):
                return str(val)
            
            # Convert everything else to string
            str_val = str(val).strip()
            
            # Filter out empty strings and obvious invalid values
            if str_val and str_val.lower() not in ['nan', 'none', 'null', '']:
                return str_val
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not convert value to string: {val} ({type(val)}) - {e}")
            return None

    def _safe_sort_values(self, values: list) -> list:
        """
        Safely sort a list of values, handling mixed types gracefully.
        
        Args:
            values: List of values to sort
            
        Returns:
            Sorted list of values (all converted to strings)
        """
        try:
            # Ensure all values are strings
            string_values = []
            for val in values:
                if isinstance(val, str):
                    string_values.append(val)
                else:
                    # Convert non-string values using our safe converter
                    str_val = self._safe_value_to_string(val)
                    if str_val is not None:
                        string_values.append(str_val)
            
            # Sort the string values
            return sorted(string_values)
            
        except Exception as e:
            logger.warning(f"Failed to sort values safely: {e}")
            # Ultimate fallback: return unsorted list of string representations
            return [str(val) for val in values]

    def _analyze_text_columns(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame):
        """
        Analyze text columns using n-gram analysis to detect filter patterns.
        Enhanced with better error handling for sparse matrix operations.
        
        Args:
            raw_data: Original dataset
            filtered_data: Filtered dataset
            
        Returns:
            Dictionary with text analysis results
        """
        # Guard clauses for input validation
        if not isinstance(raw_data, pd.DataFrame):
            raise StepProcessorError(f"raw_data must be DataFrame, got {type(raw_data)}")
        if not isinstance(filtered_data, pd.DataFrame):
            raise StepProcessorError(f"filtered_data must be DataFrame, got {type(filtered_data)}")
        
        text_results = {}
        raw_columns = raw_data.columns
        filtered_columns = filtered_data.columns
        
        for column in self.text_columns:
            if column not in raw_columns:
                logger.warning(f"Text column '{column}' not found in raw data")
                continue
            
            if column not in filtered_columns:
                logger.warning(f"Text column '{column}' not found in filtered data")
                continue
            
            logger.info(f"Analyzing text column: {column}")
            
            try:
                # Prepare text data for vectorization
                raw_column_data = raw_data[column]
                filtered_column_data = filtered_data[column]
                
                raw_text = self._prepare_text_data(raw_column_data)
                filtered_text = self._prepare_text_data(filtered_column_data)
                
                if not raw_text or not filtered_text:
                    logger.warning(f"Insufficient text data in column '{column}' for analysis")
                    continue
                
                # Perform n-gram analysis with improved error handling
                column_results = self._perform_ngram_analysis(raw_text, filtered_text, column)
                
                if column_results:
                    text_results[column] = column_results
                    logger.info(f"Column '{column}': analysis successful")
                else:
                    logger.info(f"Column '{column}': no results from analysis")
                    
            except Exception as e:
                logger.warning(f"Text analysis failed for column '{column}': {e}")
                # Continue with other columns instead of stopping completely
                continue
        
        logger.info(f"Text analysis completed: {len(text_results)} columns processed")
        return text_results

    def _prepare_text_data(self, text_series: pd.Series) -> list:
            """
            Prepare text data for vectorization analysis, safely handling mixed data types.
            
            Args:
                text_series: Pandas Series containing text data (may include datetime objects)
                
            Returns:
                List of cleaned text strings
            """
            # Guard clause for input validation
            if not isinstance(text_series, pd.Series):
                raise StepProcessorError(f"text_series must be pandas Series, got {type(text_series)}")
            
            cleaned_texts = []
            
            for val in text_series:
                try:
                    # Skip null/NaN values
                    if pd.isna(val):
                        continue
                    
                    # Handle datetime objects specifically
                    if hasattr(val, 'strftime'):  # datetime-like object
                        if hasattr(val, 'hour'):  # datetime with time
                            text_val = val.strftime('%Y-%m-%d %H:%M:%S')
                        else:  # date only
                            text_val = val.strftime('%Y-%m-%d')
                    else:
                        # Convert everything else to string
                        text_val = str(val)
                    
                    # Basic cleaning and validation
                    text_val = text_val.lower().strip()
                    
                    # Filter out very short text (less useful for n-gram analysis)
                    if len(text_val) > 2:
                        cleaned_texts.append(text_val)
                        
                except Exception as convert_error:
                    logger.warning(f"Could not convert text value: {val} ({type(val)}) - {convert_error}")
                    continue
            
            logger.debug(f"Prepared {len(cleaned_texts)} text documents from {len(text_series)} input values")
            return cleaned_texts

    def _perform_ngram_analysis(self, raw_text: list, filtered_text: list, column_name: str):
        """
        Perform n-gram analysis with enhanced vocabulary-aware and rarity-based scoring.
        Fixed to handle sparse matrix operations properly.
        
        Args:
            raw_text: List of raw text documents
            filtered_text: List of filtered text documents
            column_name: Name of column being analyzed
            
        Returns:
            Dictionary with analysis results including enhanced scoring
        """
        try:
            logger.info(f"Starting enhanced n-gram analysis for column '{column_name}'")
            
            # Validate inputs
            if not raw_text or not filtered_text:
                logger.warning(f"Insufficient text data for n-gram analysis in column '{column_name}'")
                return {}
            
            # Prepare stop words
            stop_words = 'english'
            if self.custom_stop_words:
                if isinstance(self.custom_stop_words, list):
                    stop_words = list(CountVectorizer(stop_words='english').get_stop_words()) + self.custom_stop_words
                else:
                    logger.warning(f"custom_stop_words should be a list, got {type(self.custom_stop_words)}")
            
            # Adjust max_df based on dataset size to prevent over-filtering
            dataset_size = len(raw_text)
            if dataset_size < 100:
                adjusted_max_df = 0.9
            elif dataset_size < 1000:
                adjusted_max_df = 0.8
            else:
                adjusted_max_df = 0.7
            
            # Create vectorizer
            vectorizer = CountVectorizer(
                ngram_range=self.ngram_range,
                min_df=self.min_frequency,
                max_df=adjusted_max_df,
                max_features=self.max_features,
                stop_words=stop_words,
                lowercase=True,
                token_pattern=r'\b\w+\b'  # Word boundaries only
            )
            
            # Fit on raw data to get vocabulary
            raw_matrix = vectorizer.fit_transform(raw_text)
            raw_feature_names = vectorizer.get_feature_names_out()
            raw_frequencies = raw_matrix.sum(axis=0).A1  # Convert to 1D array
            
            # Transform filtered data using same vocabulary
            filtered_matrix = vectorizer.transform(filtered_text)
            filtered_frequencies = filtered_matrix.sum(axis=0).A1
            
            # Calculate scoring factors
            total_raw_documents = len(raw_text)
            total_vocabulary_size = len(raw_feature_names)
            vocabulary_boost = max(1.0, 1000.0 / max(total_vocabulary_size, 1))
            score_threshold = max(self.score_threshold, 0.001)
            
            filter_candidates = []
            
            # Analyze each n-gram
            for i, term in enumerate(raw_feature_names):
                raw_freq = raw_frequencies[i]
                filtered_freq = filtered_frequencies[i]
                
                # Calculate disappearance ratio and absolute reduction
                if raw_freq > 0:
                    disappearance_ratio = max(0.0, (raw_freq - filtered_freq) / raw_freq)
                    absolute_reduction = raw_freq - filtered_freq
                else:
                    continue  # Skip terms with zero frequency
                
                # Skip terms that weren't actually reduced
                if disappearance_ratio < 0.1:  # Must lose at least 10% to be considered
                    continue
                
                # 1. Vocabulary boost - simpler columns (fewer unique terms) get higher scores
                # 2. Term rarity factor - rarer terms get higher scores  
                # Count how many documents contain this term - FIXED VERSION
                try:
                    # Convert sparse matrix column to dense array and count non-zero entries
                    term_column = raw_matrix[:, i].toarray().flatten()
                    term_doc_frequency = (term_column > 0).sum()
                except Exception as matrix_error:
                    logger.debug(f"Matrix operation fallback for term '{term}': {matrix_error}")
                    # Fallback: estimate based on frequency
                    term_doc_frequency = min(int(raw_freq), total_raw_documents)
                
                rarity_boost = max(1.0, total_raw_documents / max(term_doc_frequency, 1))
                
                # 3. Frequency importance - higher frequency terms get modest boost
                frequency_boost = 1.0 + (absolute_reduction / 100.0)
                
                # 4. Combined enhanced score
                enhanced_score = (
                    disappearance_ratio *          # How much was removed (0.0-1.0)
                    vocabulary_boost *             # Simpler columns score higher  
                    rarity_boost *                 # Rarer terms score higher
                    frequency_boost                # Higher frequency gets modest boost
                )
                
                if enhanced_score >= score_threshold:
                    term_words = term.split()
                    filter_candidates.append({
                        'term': term,
                        'raw_frequency': int(raw_freq),
                        'filtered_frequency': int(filtered_freq),
                        'disappearance_ratio': round(disappearance_ratio, 3),
                        'absolute_reduction': int(absolute_reduction),
                        'vocabulary_boost': round(vocabulary_boost, 2),
                        'rarity_boost': round(rarity_boost, 2), 
                        'frequency_boost': round(frequency_boost, 2),
                        'enhanced_score': round(enhanced_score, 3),
                        'filter_score': round(enhanced_score, 3),  # For backward compatibility
                        'ngram_length': len(term_words),
                        'term_doc_frequency': int(term_doc_frequency),
                        'column_vocabulary_size': total_vocabulary_size
                    })
            
            # Sort by enhanced score descending
            filter_candidates.sort(key=lambda x: x['enhanced_score'], reverse=True)
            
            logger.info(f"Enhanced n-gram analysis completed for column '{column_name}': {len(filter_candidates)} candidates found")
            
            return {
                'column_name': column_name,
                'raw_documents': len(raw_text),
                'filtered_documents': len(filtered_text),
                'total_ngrams_analyzed': len(raw_feature_names),
                'column_vocabulary_size': total_vocabulary_size,
                'filter_candidates': filter_candidates[:50],  # Top 50 candidates
                'analysis_parameters': {
                    'ngram_range': self.ngram_range,
                    'min_frequency': self.min_frequency,
                    'score_threshold': self.score_threshold,
                    'enhanced_scoring': True
                }
            }
            
        except Exception as e:
            logger.error(f"Enhanced n-gram analysis failed for column '{column_name}': {e}")
            return {}

    def _create_results_dataframe(self, analysis_results):
        """
        Convert analysis results to DataFrame with enhanced column type weighting.
        
        Args:
            analysis_results: Dictionary containing all analysis results
            
        Returns:
            DataFrame with filter term candidates and enhanced metadata
        """
        # Guard clause for input validation
        if not isinstance(analysis_results, dict):
            raise StepProcessorError(f"analysis_results must be dict, got {type(analysis_results)}")
        
        all_candidates = []
        
        # Process categorical analysis results with enhanced scoring
        categorical_analysis = analysis_results.get('categorical_analysis', {})
        if not isinstance(categorical_analysis, dict):
            categorical_analysis = {}
        
        for column, column_results in categorical_analysis.items():
            if not isinstance(column_results, dict):
                continue
            
            removed_values = column_results.get('removed_values', [])
            if not isinstance(removed_values, list):
                continue
            
            # Enhanced categorical scoring
            total_vocab_size = column_results.get('total_vocabulary_size', 10)
            enhanced_confidence = column_results.get('enhanced_confidence', 95.0)
            
            # Apply categorical type weighting (3x boost for categorical vs text)
            categorical_type_weight = 3.0
            final_categorical_score = enhanced_confidence * categorical_type_weight
            
            for removed_value in removed_values:
                # Calculate actual word count for categorical values
                actual_word_count = len(str(removed_value).split())
                
                all_candidates.append({
                    'Column_Name': column,
                    'Filter_Term': removed_value,
                    'Term_Type': 'categorical_value',
                    'Ngram_Length': actual_word_count,  # Actual word count, not always 1
                    'Raw_Frequency': 0,  # Would need separate counting
                    'Filtered_Frequency': 0,
                    'Disappearance_Ratio': 1.0,
                    'Confidence_Score': round(final_categorical_score, 1),
                    'Analysis_Method': 'categorical_comparison',
                    'Column_Vocabulary_Size': total_vocab_size,
                    'Vocabulary_Boost': column_results.get('vocabulary_boost', 1.0),
                    'Column_Type_Weight': categorical_type_weight
                })
        
        # Process text analysis results with enhanced scoring
        text_analysis = analysis_results.get('text_analysis', {})
        if not isinstance(text_analysis, dict):
            text_analysis = {}
        
        for column, column_results in text_analysis.items():
            if not isinstance(column_results, dict):
                continue
            
            filter_candidates = column_results.get('filter_candidates', [])
            if not isinstance(filter_candidates, list):
                continue
            
            # Text type weighting (1x - baseline)
            text_type_weight = 1.0
            column_vocab_size = column_results.get('column_vocabulary_size', 1000)
            
            for candidate in filter_candidates:
                if not isinstance(candidate, dict):
                    continue
                
                # Apply text type weighting to enhanced score
                base_score = candidate.get('enhanced_score', candidate.get('filter_score', 0))
                final_text_score = base_score * text_type_weight
                
                all_candidates.append({
                    'Column_Name': column,
                    'Filter_Term': candidate['term'],
                    'Term_Type': 'text_ngram',
                    'Ngram_Length': candidate['ngram_length'],
                    'Raw_Frequency': candidate['raw_frequency'],
                    'Filtered_Frequency': candidate['filtered_frequency'],
                    'Disappearance_Ratio': candidate['disappearance_ratio'],
                    'Confidence_Score': round(final_text_score, 1),  # Changed from Confidence_Percentage
                    'Analysis_Method': 'ngram_analysis',
                    'Column_Vocabulary_Size': column_vocab_size,
                    'Vocabulary_Boost': candidate.get('vocabulary_boost', 1.0),
                    'Rarity_Boost': candidate.get('rarity_boost', 1.0),
                    'Frequency_Boost': candidate.get('frequency_boost', 1.0),
                    'Column_Type_Weight': text_type_weight,
                    'Term_Doc_Frequency': candidate.get('term_doc_frequency', 0)
                })
        
        if not all_candidates:
            # Return empty DataFrame with enhanced structure
            return pd.DataFrame(columns=[
                'Column_Name', 'Filter_Term', 'Term_Type', 'Ngram_Length',
                'Raw_Frequency', 'Filtered_Frequency', 'Disappearance_Ratio',
                'Confidence_Score', 'Analysis_Method', 'Column_Vocabulary_Size',
                'Vocabulary_Boost', 'Rarity_Boost', 'Frequency_Boost', 'Column_Type_Weight',
                'Term_Doc_Frequency'
            ])
        
        # Create DataFrame and sort by enhanced confidence score
        results_df = pd.DataFrame(all_candidates)
        results_df = results_df.sort_values('Confidence_Score', ascending=False)
        
        # Add summary metadata
        summary = analysis_results.get('summary', {})
        if not isinstance(summary, dict):
            summary = {}
        
        results_df['Raw_Total_Rows'] = summary.get('raw_rows', 0)
        results_df['Filtered_Total_Rows'] = summary.get('filtered_rows', 0)
        results_df['Rows_Removed'] = summary.get('rows_removed', 0)
        results_df['Removal_Percentage'] = round(summary.get('removal_percentage', 0), 2)
        
        return results_df.reset_index(drop=True)

    @classmethod
    def get_capabilities(cls):
        """
        Get comprehensive information about processor capabilities.
        
        Returns:
            Dictionary describing processor capabilities
        """
        return {
            'description': 'Detect potential filter terms by comparing raw vs filtered datasets using n-gram analysis',
            'category': 'Data Analysis',
            'stage_to_stage': True,
            'analysis_features': [
                'N-gram analysis (1-6 word phrases)',
                'Categorical value comparison', 
                'Statistical frequency analysis',
                'Configurable scoring thresholds',
                'Custom stop word filtering'
            ],
            'output_structure': [
                'Ranked filter term candidates',
                'Confidence scores and frequency data',
                'Analysis method metadata',
                'Summary statistics'
            ],
            'column_types_supported': {
                'text_columns': 'Long text fields (notes, descriptions)',
                'categorical_columns': 'Discrete value fields (status, category)'
            },
            'parameters': {
                'ngram_range': 'Tuple defining n-gram size range (default: (1,4))',
                'min_frequency': 'Minimum occurrences to consider term (default: 2)',
                'max_features': 'Maximum n-grams to analyze (default: 10000)',
                'score_threshold': 'Minimum score for inclusion (default: 0.1)',
                'custom_stop_words': 'Additional words to ignore'
            },
            'use_cases': [
                'Reverse-engineer manual filtering rules',
                'Automate report generation workflows',
                'Identify data quality patterns',
                'Document implicit business logic',
                'Create reusable filter configurations'
            ],
            'configuration_options': {
                'required': ['raw_stage', 'filtered_stage', 'text_columns'],
                'optional': ['categorical_columns', 'ngram_range', 'min_frequency', 'max_features', 'score_threshold', 'custom_stop_words']
            }
        }
    
    def get_usage_examples(self):
        """Get complete usage examples for the filter_terms_detector processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('filter_terms_detector')


# End of file #
