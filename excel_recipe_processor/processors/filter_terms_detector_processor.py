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
        
        # Validate required fields
        required_fields = ['raw_stage', 'filtered_stage', 'text_columns']
        self.validate_required_fields(required_fields)
        
        # Get configuration values
        self.raw_stage = self.get_config_value('raw_stage')
        self.filtered_stage = self.get_config_value('filtered_stage')
        self.text_columns = self._normalize_text_columns(self.get_config_value('text_columns'))
        
        # Optional configuration with defaults
        self.ngram_range = self._normalize_ngram_range(self.get_config_value('ngram_range', (1, 4)))
        self.min_frequency = self.get_config_value('min_frequency', 2)
        self.max_features = self.get_config_value('max_features', 10000)
        self.score_threshold = self.get_config_value('score_threshold', 0.1)
        self.categorical_columns = self.get_config_value('categorical_columns', [])
        self.custom_stop_words = self.get_config_value('custom_stop_words', [])
        
        # Validate configuration
        self._validate_config()
    
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
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the filter terms detection analysis.
        
        Args:
            data: Input data (not used, loads from configured stages)
            
        Returns:
            DataFrame with detected filter terms and analysis results
            
        Raises:
            StepProcessorError: If analysis fails
        """
        self.log_step_start()
        
        try:
            # Load the two datasets for comparison
            raw_data = StageManager.load_stage(self.raw_stage)
            filtered_data = StageManager.load_stage(self.filtered_stage)
            
            logger.info(f"Loaded raw data: {len(raw_data)} rows, {len(raw_data.columns)} columns")
            logger.info(f"Loaded filtered data: {len(filtered_data)} rows, {len(filtered_data.columns)} columns")
            
            # Perform the analysis
            analysis_results = self._analyze_filter_patterns(raw_data, filtered_data)
            
            # Convert results to DataFrame format
            results_df = self._create_results_dataframe(analysis_results)
            
            self.log_step_complete(f"detected {len(results_df)} potential filter terms")
            return results_df
            
        except Exception as e:
            self.log_step_error(e)
            raise StepProcessorError(f"Filter terms detection failed: {e}")
    
    def _analyze_filter_patterns(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame):
        """
        Analyze filter patterns between raw and filtered datasets.
        
        Args:
            raw_data: Original unfiltered dataset
            filtered_data: Processed/filtered dataset
            
        Returns:
            Dictionary containing analysis results
        """
        # Guard clauses for input validation
        if not isinstance(raw_data, pd.DataFrame):
            raise StepProcessorError(f"raw_data must be DataFrame, got {type(raw_data)}")
        if not isinstance(filtered_data, pd.DataFrame):
            raise StepProcessorError(f"filtered_data must be DataFrame, got {type(filtered_data)}")
        
        raw_rows = len(raw_data)
        filtered_rows = len(filtered_data)
        rows_removed = raw_rows - filtered_rows
        
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
        
        # Analyze categorical columns (simple value comparisons)
        if self.categorical_columns:
            results['categorical_analysis'] = self._analyze_categorical_columns(raw_data, filtered_data)
        
        # Analyze text columns using n-gram analysis  
        if self.text_columns:
            results['text_analysis'] = self._analyze_text_columns(raw_data, filtered_data)
        
        return results
    
    def _analyze_categorical_columns(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame):
        """
        Analyze categorical columns for simple filter patterns.
        
        Args:
            raw_data: Original dataset
            filtered_data: Filtered dataset
            
        Returns:
            Dictionary with categorical analysis results
        """
        # Guard clauses for input validation
        if not isinstance(raw_data, pd.DataFrame):
            raise StepProcessorError(f"raw_data must be DataFrame, got {type(raw_data)}")
        if not isinstance(filtered_data, pd.DataFrame):
            raise StepProcessorError(f"filtered_data must be DataFrame, got {type(filtered_data)}")
        
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
            
            # Get unique values in each dataset
            raw_column_data = raw_data[column]
            filtered_column_data = filtered_data[column]
            
            raw_values = set(raw_column_data.dropna().astype(str))
            filtered_values = set(filtered_column_data.dropna().astype(str))
            
            # Find values that were removed
            removed_values = raw_values - filtered_values
            retained_values = raw_values & filtered_values
            
            if removed_values:
                categorical_results[column] = {
                    'removed_values': sorted(list(removed_values)),
                    'retained_values': sorted(list(retained_values)),
                    'removal_count': len(removed_values),
                    'filter_confidence': 'high'  # Categorical removals are usually intentional
                }
        
        return categorical_results
    
    def _analyze_text_columns(self, raw_data: pd.DataFrame, filtered_data: pd.DataFrame):
        """
        Analyze text columns using n-gram analysis to detect filter patterns.
        
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
            
            # Prepare text data for vectorization
            raw_column_data = raw_data[column]
            filtered_column_data = filtered_data[column]
            
            raw_text = self._prepare_text_data(raw_column_data)
            filtered_text = self._prepare_text_data(filtered_column_data)
            
            if not raw_text or not filtered_text:
                logger.warning(f"Insufficient text data in column '{column}' for analysis")
                continue
            
            # Perform n-gram analysis
            column_results = self._perform_ngram_analysis(raw_text, filtered_text, column)
            
            if column_results:
                text_results[column] = column_results
        
        return text_results
    
    def _prepare_text_data(self, text_series: pd.Series) -> list:
        """
        Prepare text data for vectorization analysis.
        
        Args:
            text_series: Pandas Series containing text data
            
        Returns:
            List of cleaned text strings
        """
        # Guard clause for input validation
        if not isinstance(text_series, pd.Series):
            raise StepProcessorError(f"text_series must be pandas Series, got {type(text_series)}")
        
        # Convert to string and handle nulls
        text_data = text_series.fillna('').astype(str)
        
        # Filter out empty strings and very short text
        text_data = text_data[text_data.str.len() > 2]
        
        # Basic cleaning
        text_data = text_data.str.lower().str.strip()
        
        return text_data.tolist()
    
    def _perform_ngram_analysis(self, raw_text: list, filtered_text: list, column_name: str):
        """
        Perform n-gram analysis to identify filter terms.
        
        Args:
            raw_text: Text data from raw dataset
            filtered_text: Text data from filtered dataset  
            column_name: Name of the column being analyzed
            
        Returns:
            Dictionary with analysis results for this column
        """
        # Guard clauses for input validation
        if not isinstance(raw_text, list):
            raise StepProcessorError(f"raw_text must be list, got {type(raw_text)}")
        if not isinstance(filtered_text, list):
            raise StepProcessorError(f"filtered_text must be list, got {type(filtered_text)}")
        if not isinstance(column_name, str):
            raise StepProcessorError(f"column_name must be string, got {type(column_name)}")
        
        try:
            # Create custom stop words list
            custom_stop_words = self.custom_stop_words
            if not isinstance(custom_stop_words, list):
                custom_stop_words = []
            
            stop_words = list(set(['english'] + custom_stop_words)) if custom_stop_words else 'english'
            
            # Set up vectorizer with n-gram analysis
            # Adjust parameters for small datasets to avoid min_df/max_df conflicts
            raw_doc_count = len(raw_text)
            adjusted_min_df = min(self.min_frequency, max(1, raw_doc_count // 4))
            adjusted_max_df = max(0.95, (raw_doc_count - 1) / raw_doc_count) if raw_doc_count > 2 else 1.0
            
            vectorizer = CountVectorizer(
                ngram_range=self.ngram_range,
                min_df=adjusted_min_df,
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
            
            # Calculate filter term scores
            filter_candidates = []
            min_freq = self.min_frequency
            score_threshold = self.score_threshold
            
            for i, term in enumerate(raw_feature_names):
                raw_freq = raw_frequencies[i]
                filtered_freq = filtered_frequencies[i]
                
                # Calculate disappearance score (higher = more likely a filter term)
                if raw_freq >= min_freq:
                    disappearance_ratio = (raw_freq - filtered_freq) / raw_freq
                    absolute_reduction = raw_freq - filtered_freq
                    
                    # Combined score: both relative and absolute importance
                    filter_score = disappearance_ratio * (1 + (absolute_reduction / 10))
                    
                    if filter_score >= score_threshold:
                        term_words = term.split()
                        filter_candidates.append({
                            'term': term,
                            'raw_frequency': int(raw_freq),
                            'filtered_frequency': int(filtered_freq),
                            'disappearance_ratio': round(disappearance_ratio, 3),
                            'absolute_reduction': int(absolute_reduction),
                            'filter_score': round(filter_score, 3),
                            'ngram_length': len(term_words)
                        })
            
            # Sort by filter score descending
            filter_candidates.sort(key=lambda x: x['filter_score'], reverse=True)
            
            return {
                'column_name': column_name,
                'raw_documents': len(raw_text),
                'filtered_documents': len(filtered_text),
                'total_ngrams_analyzed': len(raw_feature_names),
                'filter_candidates': filter_candidates[:50],  # Top 50 candidates
                'analysis_parameters': {
                    'ngram_range': self.ngram_range,
                    'min_frequency': self.min_frequency,
                    'score_threshold': self.score_threshold
                }
            }
            
        except Exception as e:
            logger.error(f"N-gram analysis failed for column '{column_name}': {e}")
            return {}
    
    def _create_results_dataframe(self, analysis_results):
        """
        Convert analysis results to a structured DataFrame.
        
        Args:
            analysis_results: Dictionary containing all analysis results
            
        Returns:
            DataFrame with filter term candidates and metadata
        """
        # Guard clause for input validation
        if not isinstance(analysis_results, dict):
            raise StepProcessorError(f"analysis_results must be dict, got {type(analysis_results)}")
        
        all_candidates = []
        
        # Process categorical analysis results
        categorical_analysis = analysis_results.get('categorical_analysis', {})
        if not isinstance(categorical_analysis, dict):
            categorical_analysis = {}
        
        for column, column_results in categorical_analysis.items():
            if not isinstance(column_results, dict):
                continue
            
            removed_values = column_results.get('removed_values', [])
            if not isinstance(removed_values, list):
                continue
            
            for removed_value in removed_values:
                all_candidates.append({
                    'Column_Name': column,
                    'Filter_Term': removed_value,
                    'Term_Type': 'categorical_value',
                    'Ngram_Length': 1,
                    'Raw_Frequency': 0,  # Would need separate counting
                    'Filtered_Frequency': 0,
                    'Disappearance_Ratio': 1.0,
                    'Confidence_Percentage': 95.0,  # High confidence for categorical
                    'Analysis_Method': 'categorical_comparison'
                })
        
        # Process text analysis results
        text_analysis = analysis_results.get('text_analysis', {})
        if not isinstance(text_analysis, dict):
            text_analysis = {}
        
        for column, column_results in text_analysis.items():
            if not isinstance(column_results, dict):
                continue
            
            filter_candidates = column_results.get('filter_candidates', [])
            if not isinstance(filter_candidates, list):
                continue
            
            for candidate in filter_candidates:
                if not isinstance(candidate, dict):
                    continue
                
                all_candidates.append({
                    'Column_Name': column,
                    'Filter_Term': candidate['term'],
                    'Term_Type': 'text_ngram',
                    'Ngram_Length': candidate['ngram_length'],
                    'Raw_Frequency': candidate['raw_frequency'],
                    'Filtered_Frequency': candidate['filtered_frequency'],
                    'Disappearance_Ratio': candidate['disappearance_ratio'],
                    'Confidence_Percentage': round(candidate['filter_score'] * 100, 1),  # Convert to percentage
                    'Analysis_Method': 'ngram_analysis'
                })
        
        if not all_candidates:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=[
                'Column_Name',
                'Filter_Term', 
                'Term_Type',
                'Ngram_Length',
                'Raw_Frequency',
                'Filtered_Frequency',
                'Disappearance_Ratio',
                'Confidence_Percentage',
                'Analysis_Method'
            ])
        
        # Create DataFrame and sort by confidence score
        results_df = pd.DataFrame(all_candidates)
        results_df = results_df.sort_values('Confidence_Percentage', ascending=False)
        
        # Add summary metadata as additional columns
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
