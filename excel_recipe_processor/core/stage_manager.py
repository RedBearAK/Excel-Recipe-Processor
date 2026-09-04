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

    # Run-level memory accounting (estimation-level, from pandas
    # memory_usage(deep=True) at each save). All stage traffic funnels
    # through save_stage/delete_stage, so a running concurrent total
    # with a high-water mark gives the EXACT peak at that estimation
    # level - which is not the same number as "sum of everything ever
    # created", because stages are freed mid-run by design.
    _mem_current_mb: float  = 0.0
    _mem_peak_mb: float     = 0.0
    _mem_saved_total_mb: float = 0.0
    _mem_freed_total_mb: float = 0.0
    # How many times each stage has been saved this run. Lets --dump-stage
    # dump a stage on EVERY save, not just its first: a re-used stage that
    # only dumped once would be untroubleshootable from the outside.
    _save_counts: dict      = {}                # dict[str, int]
    _stage_metadata: dict   = {}                # dict[str, dict]  
    _stage_usage: dict      = {}                # dict[str, int]
    _auto_free: bool        = True              # settings: auto_free_stages (default on, 2026-09-04)
    _expected_uses: dict    = {}                # dict[str, int] from recipe scan
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

        # AUTO-FREE (2026-08-26, designed with Kris): the recipe already
        # declares every use, so expected consumer counts come from a
        # structural scan - values exactly matching stage names, with
        # producer and prose/formula keys excluded. Runtime decrements
        # on every load; a stage frees the moment its counter reaches
        # zero. Failure geometry is the safety case: an undercount
        # fails LOUD at the next load (stage-not-found with
        # suggestions, never silent wrong data); an overcount only
        # holds memory longer. DEFAULT ON since 2026-09-04 - a recipe that
        # said nothing kept every stage alive to the end (13 stages, 149 MB
        # in the VMS merge) and nobody wanted that; settings:
        # auto_free_stages: false is the opt-out.
        cls._auto_free = bool(
            recipe_config.get('settings', {}).get('auto_free_stages', True))
        cls._expected_uses = {}
        cls._step_consumers = []
        if cls._auto_free:
            cls._step_consumers = cls._scan_step_consumers(recipe_config)
            for consumed in cls._step_consumers:
                for name in consumed:
                    cls._expected_uses[name] = cls._expected_uses.get(name, 0) + 1
            planned = sum(cls._expected_uses.values())
            logger.info(
                f"🍃 Auto-free enabled: {len(cls._expected_uses)} "
                f"stage(s) tracked across {planned} consuming step(s); "
                f"each frees when its last consuming step completes")

    # Keys whose string values are never a stage the step CONSUMES. The
    # scan walks recipe steps only, never settings.stages, so the
    # declaration key stage_name is not here: inside a step it is always
    # a reference - filter_data in_stage / not_in_stage, verify_stage_data
    # rules, aggregate/group/merge sources (the 2026-08-13 standardization:
    # "stage_name is for declarations and rule references, never
    # step-level flow"). Excluding it undercounted every stage referenced
    # only through a rule, which auto-free then freed too early; found
    # 2026-09-04 when the VMS merge halted at its in_stage filter.
    _NON_REFERENCE_KEYS = frozenset((
        'save_to_stage', 'step_description', 'description',
        'pandas_formula', 'excel_formula', 'when_formula', 'name_mgr_comment',
    ))

    @classmethod
    def _scan_step_consumers(cls, recipe_config: dict) -> list:
        """Per-step SETS of consumed stage names, in recipe order.

        A step consuming a stage counts ONCE however many loads it
        performs; the countdown is steps, not loads.
        """
        names = set(cls._declared_stages)
        for step in recipe_config.get('recipe', []) or []:
            saved = step.get('save_to_stage')
            if saved:
                names.add(saved)
        per_step = []

        for step in recipe_config.get('recipe', []) or []:
            consumed = set()

            def walk(node, key=None):
                if isinstance(node, dict):
                    for k, v in node.items():
                        walk(v, k)
                elif isinstance(node, list):
                    for item in node:
                        walk(item, key)
                elif isinstance(node, str):
                    if key in cls._NON_REFERENCE_KEYS:
                        return
                    if node in names:
                        consumed.add(node)

            if step.get('processor_type') != 'free_stages':
                walk({k: v for k, v in step.items() if k != 'save_to_stage'})
            per_step.append(consumed)
        return per_step

    @classmethod
    def auto_free_after_step(cls, step_index: int) -> None:
        """Decrement each stage the completed step consumed; free at zero."""
        if not cls._auto_free or step_index >= len(cls._step_consumers):
            return
        for name in sorted(cls._step_consumers[step_index]):
            remaining = cls._expected_uses.get(name)
            if remaining is None:
                continue
            remaining -= 1
            cls._expected_uses[name] = remaining
            if (remaining <= 0 and name in cls._current_stages
                    and not cls.is_stage_protected(name)):
                freed_mb = cls._stage_metadata.get(name, {}).get(
                    'memory_usage_mb', 0.0)
                cls._mem_current_mb -= freed_mb
                cls._mem_freed_total_mb += freed_mb
                del cls._current_stages[name]
                logger.info(
                    f"♻️  Auto-freed stage '{name}' - last consuming "
                    f"step complete (~{freed_mb:.0f} MB returned; "
                    f"{len(cls._current_stages)} stage(s) in memory)")

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
    def get_stage_save_count(cls, stage_name: str) -> int:
        """How many times this stage has been saved in the current run."""
        return cls._save_counts.get(stage_name, 0)

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
        
        # Check if stage already exists (for non-protected stages).
        # confirm_replacement counts as consent here too: it is the key a
        # recipe step can actually set ('confirm_stage_replacement: true'),
        # and without honoring it, re-using an unprotected stage was
        # impossible through any processor.
        if stage_name in cls._current_stages and not overwrite and not confirm_replacement \
                and stage_name not in cls._protected_stages:
            raise StageError(
                f"Stage '{stage_name}' already exists. Set 'confirm_stage_replacement: true' "
                f"on the step (or overwrite=true programmatically) to replace it."
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
        cls._save_counts[stage_name] = cls._save_counts.get(stage_name, 0) + 1

        # Memory accounting: an overwrite releases the old frame first
        previous = cls._stage_metadata.get(stage_name, {}).get('memory_usage_mb', 0.0)
        if previous:
            cls._mem_current_mb -= previous
            cls._mem_freed_total_mb += previous
        new_mb = round(data.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        cls._mem_current_mb += new_mb
        cls._mem_saved_total_mb += new_mb
        if cls._mem_current_mb > cls._mem_peak_mb:
            cls._mem_peak_mb = cls._mem_current_mb

        cls._stage_metadata[stage_name] = {
            'rows': len(data),
            'columns': len(data.columns),
            'column_names': list(data.columns),
            'description': description,
            'step_name': step_name,
            'created_at': datetime.now(),
            'memory_usage_mb': new_mb,
            'declared': stage_name in cls._declared_stages,
            'protected': stage_name in cls._protected_stages
        }
        cls._stage_usage[stage_name] = 0  # Reset usage counter
        
        # Log with appropriate level based on declaration status
        if stage_name in cls._declared_stages:
            logger.info(
                f"Stage '{stage_name}' saved: {len(data)} rows, {len(data.columns)} columns")
            logger.info(
                (f" - {description}" if description else "")
            )
        else:
            logger.info(
                f"Stage '{stage_name}' saved (undeclared): {len(data)} rows, {len(data.columns)} columns")
            logger.info(
                (f" - {description}" if description else "")
            )

    @classmethod
    def delete_stage(cls, stage_name: str) -> None:
        """
        Delete a stage, freeing its memory.

        Refuses protected stages: protection means "this must survive the
        run", and a memory-trimming step does not outrank that declaration.

        Args:
            stage_name: Stage to delete

        Raises:
            StageError: If the stage does not exist or is protected
        """
        if stage_name not in cls._current_stages:
            similar = cls._suggest_similar_stage_names(stage_name, list(cls._current_stages.keys()))
            hint = f" Did you mean: {similar}?" if similar else ""
            raise StageError(f"Cannot delete stage '{stage_name}': not found.{hint}")

        if cls.is_stage_protected(stage_name):
            raise StageError(
                f"Cannot delete stage '{stage_name}': declared protected"
            )

        freed_mb = cls._stage_metadata.get(stage_name, {}).get('memory_usage_mb', 0.0)
        cls._mem_current_mb -= freed_mb
        cls._mem_freed_total_mb += freed_mb

        del cls._current_stages[stage_name]

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

        # Auto-free happens at STEP COMPLETION, never at load: a
        # consuming step may load its source any number of times
        # (the pipeline peek plus the processor's own load), so
        # per-load countdown undercounts systemically
        
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
            'stages_declared':          list(cls._declared_stages.keys()),
            # Created means SAVED AT LEAST ONCE this run - sourced from the
            # save counter, not from what is still in memory. A recipe that
            # frees its stages mid-run (free_stages) would otherwise report
            # "created: 0" at completion, which reads as a broken run when it
            # is actually a tidy one.
            'stages_created':           list(cls._save_counts.keys()),
            'stages_freed':             [
                name for name in cls._save_counts.keys()
                if name not in cls._current_stages
            ],
            'stages_in_memory':         list(cls._current_stages.keys()),
            'stages_unused':            cls.get_unused_stages(),
            'protected_stages':         list(cls._protected_stages),
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
    def get_memory_stats(cls) -> dict:
        """Run-level stage-memory accounting for the completion summary.

        Estimation-level numbers (pandas deep memory_usage at save time),
        so they describe DataFrame footprint, not process RSS - the OS
        may compress or swap independently. Peak is the true concurrent
        high-water mark, which mid-run freeing keeps well below the
        total-allocated figure by design.
        """
        return {
            'peak_concurrent_mb': round(cls._mem_peak_mb, 1),
            'total_allocated_mb': round(cls._mem_saved_total_mb, 1),
            'total_freed_mb': round(cls._mem_freed_total_mb, 1),
            'still_held_mb': round(max(cls._mem_current_mb, 0.0), 1),
        }

    @classmethod
    def initialize_stages(cls, max_stages: int = 10) -> None:
        """Initialize stage storage (called by pipeline at start)."""
        cls._max_stages = max_stages
        cls.cleanup_stages()  # Start fresh
        cls._mem_current_mb = 0.0
        cls._mem_peak_mb = 0.0
        cls._mem_saved_total_mb = 0.0
        cls._mem_freed_total_mb = 0.0
        logger.debug(f"Initialized stage storage with max_stages={max_stages}")
    
    @classmethod
    def cleanup_stages(cls) -> None:
        """Clean up all stage storage (called by pipeline at end)."""
        stage_count = len(cls._current_stages)
        memory_freed = sum(meta.get('memory_usage_mb', 0.0) for meta in cls._stage_metadata.values())
        
        cls._current_stages.clear()
        cls._save_counts.clear()
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
