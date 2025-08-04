"""
Stage management for Excel Recipe Processor.

Provides the StageManager class for saving, loading, and managing
intermediate data stages during recipe processing with friendly validation.
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
    _max_stages: int        = 100               # Configurable limit
    _declared_stages: dict  = {}
    _protected_stages       = set()
    
    def __new__(cls):
        raise TypeError(f"{cls.__name__} is a static utility class. "
                        f"Use {cls.__name__}.method_name() directly.")
    
    # =============================================================================
    # PUBLIC API - What processors call
    # =============================================================================

    @classmethod
    def declare_recipe_stages(cls, recipe_config: dict) -> None:
        """Declare all stages from recipe settings."""
        cls._declared_stages.clear()
        cls._protected_stages.clear()
        
        stages_list = recipe_config.get('settings', {}).get('stages', [])
        
        for stage_config in stages_list:
            stage_name = stage_config['stage_name']  # Required field
            cls._declared_stages[stage_name] = stage_config
            
            if stage_config.get('protected', False):
                cls._protected_stages.add(stage_name)
        
        logger.info(f"Declared {len(stages_list)} stages")

    @classmethod
    def validate_recipe_stages(cls, recipe_config: dict) -> dict:
        """
        Validate all stage references in recipe and return helpful warnings/suggestions.
        
        Returns:
            Dictionary with:
            - 'warnings': List of warning messages
            - 'undeclared_stages': Set of stage names that should be declared
            - 'suggested_declarations': YAML text for stage declarations
            - 'protection_issues': List of protection-related warnings
        """
        declared_stages = set(cls._declared_stages.keys())
        warnings = []
        undeclared_stages = set()
        protection_issues = []
        
        for step_index, step in enumerate(recipe_config.get('recipe', [])):
            step_desc = step.get('step_description', f'Step {step_index + 1}')
            processor_type = step.get('processor_type')
            
            # Import processors create stages
            if processor_type == 'import_file':
                save_stage = step.get('save_to_stage')
                if save_stage and save_stage not in declared_stages:
                    warnings.append(f"Step '{step_desc}': stage '{save_stage}' not declared (will be created dynamically)")
                    undeclared_stages.add(save_stage)
                continue
            
            # Export processors consume stages  
            if processor_type == 'export_file':
                source_stage = step.get('source_stage')
                if source_stage and source_stage not in declared_stages:
                    warnings.append(f"Step '{step_desc}': stage '{source_stage}' not declared (existence will be checked at runtime)")
                    undeclared_stages.add(source_stage)
                continue
            
            # Processing steps require both
            source_stage = step.get('source_stage')
            save_stage = step.get('save_to_stage')
            
            if source_stage and source_stage not in declared_stages:
                warnings.append(f"Step '{step_desc}': source stage '{source_stage}' not declared")
                undeclared_stages.add(source_stage)
            
            if save_stage and save_stage not in declared_stages:
                warnings.append(f"Step '{step_desc}': save stage '{save_stage}' not declared")
                undeclared_stages.add(save_stage)
        
        # Generate helpful suggestions
        suggested_declarations = cls._generate_stage_declarations(undeclared_stages)
        
        # Check for potential protection issues
        if undeclared_stages:
            protection_issues.append("💡 Consider declaring stages to enable protection and auto-completion features")
            protection_issues.append("💡 Use 'protected: true' for critical data stages that shouldn't be overwritten")
        
        return {
            'warnings': warnings,
            'undeclared_stages': undeclared_stages,
            'suggested_declarations': suggested_declarations,
            'protection_issues': protection_issues,
            'has_undeclared': len(undeclared_stages) > 0
        }

    @classmethod
    def _generate_stage_declarations(cls, stage_names: set) -> str:
        """Generate YAML for stage declarations."""
        if not stage_names:
            return ""
        
        sorted_stages = sorted(stage_names)
        yaml_lines = ["💡 Suggested stage declarations to add to settings section:", ""]
        yaml_lines.append("stages:")
        
        for stage_name in sorted_stages:
            yaml_lines.append(f'  - stage_name: "{stage_name}"')
            yaml_lines.append(f'    description: "TODO: Add description for {stage_name}"')
            yaml_lines.append('    protected: false')
            yaml_lines.append('')
        
        return "\n".join(yaml_lines)

    @classmethod
    def is_stage_declared(cls, stage_name: str) -> bool:
        """Check if stage was declared in recipe settings."""
        return stage_name in cls._declared_stages

    @classmethod
    def is_stage_protected(cls, stage_name: str) -> bool:
        """Check if stage is protected from overwriting."""
        return stage_name in cls._protected_stages

    @classmethod
    def save_stage(cls, stage_name: str, data: pd.DataFrame, description: str = '',
                    step_name: str = '', overwrite: bool = False,
                    confirm_replacement: bool = False) -> None:
        """
        Save a DataFrame to a named stage with protection checks.
        
        Args:
            stage_name: Name for the stage
            data: DataFrame to save
            description: Optional description
            step_name: Name of step creating this stage
            overwrite: Whether to overwrite existing stage
            confirm_replacement: Explicit confirmation for protected stages
            
        Raises:
            StageError: If stage saving fails due to protection or other issues
        """
        # Validate stage name
        cls._validate_stage_name(stage_name)
        
        # Protection checks for declared protected stages
        if cls._declared_stages and stage_name in cls._protected_stages:
            if stage_name in cls._current_stages:
                # Protected stage already exists - need explicit confirmation
                if not confirm_replacement and not overwrite:
                    raise StageError(
                        f"Protected stage '{stage_name}' cannot be overwritten without explicit confirmation. "
                        f"Use 'overwrite: true' or 'confirm_replacement: true' to override."
                    )
                else:
                    logger.warning(f"⚠️ Overwriting protected stage '{stage_name}' with explicit confirmation")
            else:
                # First creation of protected stage - allowed
                logger.info(f"Creating protected stage '{stage_name}' (first save)")
        
        # Check if stage already exists (for non-protected stages)
        if stage_name in cls._current_stages and not overwrite and stage_name not in cls._protected_stages:
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
            'memory_usage_mb': round(data.memory_usage(deep=True).sum() / (1024 * 1024), 2),
            'declared': stage_name in cls._declared_stages,
            'protected': stage_name in cls._protected_stages
        }
        cls._stage_usage[stage_name] = 0  # Reset usage counter
        
        # Log with appropriate level based on declaration status
        if stage_name in cls._declared_stages:
            logger.info(
                f"Stage '{stage_name}' saved: {len(data)} rows, {len(data.columns)} columns"
                + (f" - {description}" if description else "")
            )
        else:
            logger.info(
                f"Stage '{stage_name}' saved (undeclared): {len(data)} rows, {len(data.columns)} columns"
                + (f" - {description}" if description else "")
            )

    @classmethod
    def load_stage(cls, stage_name: str) -> pd.DataFrame:
        """
        Load data from a named stage.
        
        Args:
            stage_name: Name of stage to load
            
        Returns:
            DataFrame from the stage
            
        Raises:
            StageError: If stage not found with helpful suggestions
        """
        # Check if stage exists
        if stage_name not in cls._current_stages:
            available_stages = list(cls._current_stages.keys())
            
            # Try to suggest similar stage names
            suggestions = cls._suggest_similar_stage_names(stage_name, available_stages)
            
            error_msg = f"Stage '{stage_name}' not found."
            if available_stages:
                error_msg += f" Available stages: {available_stages}"
                if suggestions:
                    error_msg += f"\n💡 Did you mean: {', '.join(suggestions)}?"
            else:
                error_msg += " No stages have been created yet."
                error_msg += "\n💡 Make sure an import_file or processing step created this stage first."
            
            raise StageError(error_msg)
        
        # Increment usage counter
        cls._stage_usage[stage_name] += 1
        
        # Get stage data
        stage_data = cls._current_stages[stage_name].copy()
        
        # Log with declaration status
        if stage_name in cls._declared_stages:
            logger.info(
                f"Stage '{stage_name}' loaded: {len(stage_data)} rows, {len(stage_data.columns)} columns "
                f"[usage: {cls._stage_usage[stage_name]}]"
            )
        else:
            logger.info(
                f"Stage '{stage_name}' loaded (undeclared): {len(stage_data)} rows, {len(stage_data.columns)} columns "
                f"[usage: {cls._stage_usage[stage_name]}]"
            )
        
        return stage_data

    @classmethod
    def _suggest_similar_stage_names(cls, target_name: str, available_names: list[str]) -> list:
        """Suggest similar stage names for typos."""
        if not available_names:
            return []
        
        suggestions = []
        target_lower = target_name.lower()
        
        for name in available_names:
            name_lower = name.lower()
            
            # Simple similarity checks
            if target_lower in name_lower or name_lower in target_lower:
                suggestions.append(name)
            elif abs(len(target_name) - len(name)) <= 2:
                # Similar length - might be a typo
                differences = sum(1 for a, b in zip(target_lower, name_lower) if a != b)
                if differences <= 2:
                    suggestions.append(name)
        
        return suggestions[:3]  # Limit to top 3 suggestions

    @classmethod
    def get_recipe_completion_report(cls) -> dict:
        """Generate comprehensive report after recipe completion."""
        return {
            'stages_declared': len(cls._declared_stages),
            'stages_created': len(cls._current_stages),
            'stages_unused': cls.get_unused_stages(),
            'protected_stages': list(cls._protected_stages),
            'undeclared_stages_created': [
                name for name in cls._current_stages.keys() 
                if name not in cls._declared_stages
            ],
            'total_memory_mb': sum(
                meta.get('memory_usage_mb', 0) 
                for meta in cls._stage_metadata.values()
            ),
            'stage_details': {
                name: {
                    'declared': name in cls._declared_stages,
                    'description': cls._declared_stages.get(name, {}).get('description', 'N/A'),
                    'protected': name in cls._protected_stages,
                    'rows': meta['rows'],
                    'columns': meta['columns'],
                    'memory_mb': meta['memory_usage_mb'],
                    'usage_count': cls._stage_usage.get(name, 0)
                }
                for name, meta in cls._stage_metadata.items()
            }
        }

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
            problematic_name: The problematic stage name
            
        Returns:
            List of suggested alternative names
        """
        # Simple transformations for common issues
        suggestions = []
        
        # Remove special characters
        cleaned = ''.join(c if c.isalnum() or c in ' _-' else '' for c in problematic_name)
        if cleaned != problematic_name:
            suggestions.append(cleaned.strip())
        
        # Replace spaces with underscores
        if ' ' in problematic_name:
            suggestions.append(problematic_name.replace(' ', '_'))
        
        # Title case version
        suggestions.append(problematic_name.title().replace('_', ' '))
        
        return list(set(suggestions))[:3]  # Remove duplicates, limit to 3
    
    @staticmethod
    def _validate_stage_name(stage_name: str) -> None:
        """
        Validate a stage name according to our conventions.
        
        Args:
            stage_name: Stage name to validate
            
        Raises:
            StageError: If stage name is invalid
        """
        if not isinstance(stage_name, str):
            raise StageError("Stage name must be a string")
        
        if not stage_name.strip():
            raise StageError("Stage name cannot be empty")
        
        # Reserved names check
        reserved_names = {'current', 'input', 'output', 'temp', 'temporary'}
        if stage_name.lower() in reserved_names:
            alternatives = StageManager._suggest_alternative_stage_names(stage_name)
            raise StageError(
                f"Stage name '{stage_name}' is reserved. Please use a more descriptive name."
                + (f" Suggestions: {alternatives}" if alternatives else "")
            )
