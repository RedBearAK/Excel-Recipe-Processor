"""
Stage management for Excel Recipe Processor.

Provides the StageManager class for saving, loading, and managing
intermediate data stages during recipe processing.
"""

import pandas as pd
import logging

from datetime import datetime


logger = logging.getLogger(__name__)


class StageError(Exception):
    """Raised when stage operations fail."""
    pass


class StageManager:
    """
    Static utility class for managing data stages.
    
    DO NOT INSTANTIATE - Use StageManager.method_name() directly.
    
    Uses class variables to maintain shared state across all usage.
    All public methods are class methods for clean global access.
    """
    
    # Shared state across all usage
    _current_stages: dict   = {}                # dict[str, pd.DataFrame]
    _stage_metadata: dict   = {}                # dict[str, dict]  
    _stage_usage: dict      = {}                # dict[str, int]
    _max_stages: int        = 25                # Configurable limit
    
    def __new__(cls):
        raise TypeError(f"{cls.__name__} is a static utility class. "
                        f"Use {cls.__name__}.method_name() directly.")
    
    # =============================================================================
    # PUBLIC API - What processors call
    # =============================================================================
    
    @classmethod
    def save_stage(cls, stage_name: str, data: pd.DataFrame, overwrite: bool = False,
                    description: str = '', step_name: str = '') -> None:
        """
        Save a DataFrame as a named stage.
        
        Args:
            stage_name: Name for the stage
            data: DataFrame to save
            overwrite: Whether to overwrite existing stage
            description: Optional description
            step_name: Name of step creating this stage
            
        Raises:
            StageError: If stage saving fails
        """
        # Validate stage name
        cls._validate_stage_name(stage_name)
        
        # Check if stage already exists
        if stage_name in cls._current_stages and not overwrite:
            raise StageError(
                f"Stage '{stage_name}' already exists. Use overwrite=true to replace it."
            )
        
        # Check stage limit
        if len(cls._current_stages) >= cls._max_stages and stage_name not in cls._current_stages:
            current_stages = list(cls._current_stages.keys())
            raise StageError(
                f"Maximum number of stages ({cls._max_stages}) reached. "
                f"Current stages: {current_stages}"
            )
        
        # Save the stage
        cls._current_stages[stage_name] = data.copy()
        cls._stage_metadata[stage_name] = {
            'rows': len(data),
            'columns': len(data.columns),
            'column_names': list(data.columns),
            'description': description,
            'step_name': step_name,
            'created_at': datetime.now(),
            'memory_usage_mb': round(data.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        }
        cls._stage_usage[stage_name] = 0  # Reset usage counter
        
        logger.info(
            f"Stage '{stage_name}' saved: {len(data)} rows, {len(data.columns)} columns"
            + (f" - {description}" if description else "")
        )
    
    @classmethod
    def load_stage(cls, stage_name: str) -> pd.DataFrame:
        """
        Load data from a named stage.
        
        Args:
            stage_name: Name of stage to load
            step_name: Name of step loading this stage (for logging)
            
        Returns:
            DataFrame from the stage
            
        Raises:
            StageError: If stage not found
        """
        # Check if stage exists
        if stage_name not in cls._current_stages:
            available_stages = list(cls._current_stages.keys())
            raise StageError(
                f"Stage '{stage_name}' not found. Available stages: {available_stages}"
            )
        
        # Increment usage counter
        cls._stage_usage[stage_name] += 1
        
        # Get stage data
        stage_data = cls._current_stages[stage_name].copy()
        
        logger.info(
            f"Stage '{stage_name}' loaded: {len(stage_data)} rows, {len(stage_data.columns)} columns "
            f"[usage: {cls._stage_usage[stage_name]}]"
        )
        
        return stage_data
    
    @classmethod
    def list_stages(cls) -> dict:
        """Get information about all saved stages."""
        stage_info = {}
        for stage_name in cls._current_stages:
            stage_info[stage_name] = {
                **cls._stage_metadata[stage_name],
                'usage_count': cls._stage_usage.get(stage_name, 0)
            }
        return stage_info
    
    @classmethod
    def get_unused_stages(cls) -> list:
        """Get list of stages that were created but never used."""
        return [name for name, usage in cls._stage_usage.items() if usage == 0]
    
    @classmethod
    def stage_exists(cls, stage_name: str) -> bool:
        """Check if a stage exists."""
        return stage_name in cls._current_stages
    
    @classmethod
    def get_stage_count(cls) -> int:
        """Get the number of currently stored stages."""
        return len(cls._current_stages)
    
    # =============================================================================
    # LIFECYCLE MANAGEMENT - Called by pipeline
    # =============================================================================
    
    @classmethod
    def initialize_stages(cls, max_stages: int = 10) -> None:
        """Initialize stage storage (called by pipeline at start)."""
        cls._max_stages = max_stages
        cls.cleanup_stages()  # Start fresh
        logger.debug(f"Initialized stage storage with max_stages={max_stages}")
    
    @classmethod
    def cleanup_stages(cls) -> None:
        """Clean up all stage storage (called by pipeline at end)."""
        stage_count = len(cls._current_stages)
        memory_freed = sum(meta.get('memory_usage_mb', 0.0) for meta in cls._stage_metadata.values())
        
        cls._current_stages.clear()
        cls._stage_metadata.clear()
        cls._stage_usage.clear()
        
        if stage_count > 0:
            logger.info(f"Cleaned up {stage_count} stages, freed ~{memory_freed:.1f}MB memory")
    
    @classmethod
    def get_stage_summary(cls) -> dict:
        """Get summary of stage manager state."""
        unused_stages = cls.get_unused_stages()
        
        return {
            'total_stages': len(cls._current_stages),
            'unused_stages': len(unused_stages),
            'unused_stage_names': unused_stages,
            'total_memory_mb': sum(meta.get('memory_usage_mb', 0.0) for meta in cls._stage_metadata.values()),
            'stage_names': list(cls._current_stages.keys())
        }
    
    # =============================================================================
    # PRIVATE HELPERS AND UTILITIES
    # =============================================================================
    
    @staticmethod
    def _suggest_alternative_stage_names(problematic_name: str) -> list:
        """
        Suggest human-readable alternative stage names.
        
        Args:
            problematic_name: Name that has conflicts
            
        Returns:
            List of human-readable alternative names
        """
        base = problematic_name.replace('_', ' ').title()
        
        return [
            f"Processed {base}",
            f"Clean {base}", 
            f"{base} Results",
            f"Working {base}",
            f"{base} Dataset"
        ]
    
    @staticmethod
    def get_stage_naming_guidelines() -> dict:
        """
        Get guidelines for stage naming best practices.
        
        Returns:
            Dictionary with naming guidelines and examples
        """
        return {
            'principles': [
                'Use clear, descriptive names that anyone can understand',
                'Include business context when possible',
                'Describe the data state or content, not the processing step',
                'Use proper capitalization and spaces for readability',
                'Keep names under 80 characters for log readability'
            ],
            'excellent_examples': [
                'Cleaned Customer Data',           # Processing state + content
                'Active Orders Only',              # Filter result description  
                'Q1 Sales Summary',               # Business context + content type
                'Customer Support Tickets',        # Business entity description
                'Regional Performance Metrics',    # Analysis result description
                'Validated Product Catalog',       # Quality state + content
                'March Sales with Customer Details' # Comprehensive description
            ],
            'avoid': [
                'clean_data',          # Conflicts with processor, not descriptive
                'temp',               # Not descriptive
                'data',               # Too generic
                'result',             # Not descriptive
                'df',                 # Programmer jargon
                'output'              # System-sounding
            ],
            'naming_patterns': {
                'business_entities': [
                    'Customer Master List', 'Product Catalog 2024', 'Employee Directory',
                    'Vendor Contact Information', 'Regional Sales Data'
                ],
                'processing_states': [
                    'Cleaned Sales Data', 'Filtered Active Records', 'Validated Entries',
                    'Enriched Customer Information', 'Processed Order Details'
                ],
                'analysis_results': [
                    'Monthly Sales Summary', 'Regional Performance Analysis', 
                    'Customer Segmentation Results', 'Quarterly Budget Report'
                ],
                'workflow_stages': [
                    'Initial Data Load', 'After Quality Validation', 'Ready for Analysis',
                    'Backup Before Processing', 'Final Report Dataset'
                ]
            },
            'tips': [
                'Think about how this will appear in log messages',
                'Consider what a business user would call this data',
                'Be specific about what filtering/processing was applied',
                'Include time periods when relevant (Q1, March, 2024)',
                'Use terms your organization already uses for this data'
            ]
        }
    
    @staticmethod
    def _get_registered_processor_types() -> set:
        """
        Get set of all registered processor types to check for collisions.
        
        Returns:
            Set of processor type strings
        """
        try:
            # Import registry to get current processor types
            from excel_recipe_processor.processors.base_processor import registry
            return set(registry.get_registered_types())
        except ImportError:
            # Fallback: known processor types if registry unavailable
            logger.warning("Could not import processor registry, using fallback processor list")
            return {
                'add_calculated_column', 'add_subtotals', 'aggregate_data', 'clean_data',
                'debug_breakpoint', 'fill_data', 'filter_data', 'group_data',
                'lookup_data', 'merge_data', 'pivot_table', 'rename_columns',
                'sort_data', 'split_column', 'save_stage', 'load_stage',
                'export_file', 'import_file', 'format_excel', 'create_stage'
            }
    
    @staticmethod
    def _validate_stage_name(stage_name: str) -> None:
        """
        Validate a stage name for safety and readability.
        
        Args:
            stage_name: Stage name to validate
            
        Raises:
            StageError: If stage name is invalid
        """
        if not isinstance(stage_name, str) or not stage_name.strip():
            raise StageError("Stage name must be a non-empty string")
        
        # Check length (reasonable for display in logs)
        if len(stage_name) > 80:
            raise StageError("Stage name too long (max 80 characters)")
        
        # Only check for file system problematic characters
        problematic_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\t', '\n', '\r']
        found_problematic = [char for char in problematic_chars if char in stage_name]
        if found_problematic:
            raise StageError(f"Stage name contains problematic characters: {found_problematic}")
        
        # Check for system reserved names (but allow proper case variations)
        reserved_names = ['current', 'input', 'output', 'temp', 'tmp', 'data']
        if stage_name.lower().strip() in reserved_names:
            raise StageError(f"Stage name '{stage_name}' is reserved. Please use a more descriptive name.")
        
        # Check for collision with processor names (case-insensitive for safety)
        processor_types = StageManager._get_registered_processor_types()
        if stage_name.lower().replace(' ', '_') in processor_types or stage_name in processor_types:
            suggestions = StageManager._suggest_alternative_stage_names(stage_name)
            raise StageError(
                f"Stage name '{stage_name}' too similar to processor type. "
                f"Please use a more descriptive name to avoid confusion. "
                f"Suggestions: {', '.join(suggestions)}"
            )
