"""
Generate column configuration processor for Excel automation recipes.

excel_recipe_processor/processors/generate_column_config_processor.py

Compares column names between two stages and generates YAML configuration files
with raw columns, desired columns, to-create columns, and rename mappings.
Designed to automate the tedious process of maintaining column configurations
when manual reports change their structure.

FUTURE CONSIDERATIONS:
- Could theoretically operate in memory by manipulating recipe variables directly,
  but current architecture doesn't support processors saving variable values
  for later steps to use
- If variable manipulation capability is added, might need to inherit from
  FileOpsBaseProcessor instead of ExportBaseProcessor  
- Potential enhancement to select_columns processor to allow mapping during
  transfer (mapping dict to rename while selecting, avoiding separate rename step)
- Could add more sophisticated fuzzy matching algorithms for rename detection
- Could support multiple similarity thresholds for different confidence levels
"""

import os
import re
import pandas as pd
import logging

from difflib import SequenceMatcher
from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import ExportBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class GenerateColumnConfigProcessor(ExportBaseProcessor):
    """
    Processor that compares column names between two stages and generates 
    YAML configuration files for column management in recipes.
    
    Takes a source stage (original/raw columns) and target stage (desired columns)
    and produces lists of raw columns, desired columns, columns to create,
    and suggested rename mappings.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'source_stage': 'raw_data',
            'template_stage': 'desired_data', 
            'output_file': 'column_config.yaml'
        }
    
    def __init__(self, step_config: dict):
        super().__init__(step_config)
        
        # Validate required configuration beyond what ExportBaseProcessor handles
        self.template_stage = self.get_config_value('template_stage')
        if not self.template_stage:
            raise StepProcessorError(f"Step '{self.step_name}' requires template_stage")
        
        self.output_file = self.get_config_value('output_file')
        if not self.output_file:
            raise StepProcessorError(f"Step '{self.step_name}' requires output_file")
        
        # Optional configuration
        self.include_recipe_section = self.get_config_value('include_recipe_section', False)
        self.similarity_threshold = self.get_config_value('similarity_threshold', 0.8)
    
    def save_data(self, data: pd.DataFrame) -> None:
        """
        Generate column configuration by comparing source and target stages.
        
        Args:
            data: Data from source_stage (contains raw/original columns)
        """
        try:
            # Load target stage for comparison
            target_data = StageManager.load_stage(self.template_stage)
            if target_data is None or target_data.empty:
                raise StepProcessorError(f"Target stage '{self.template_stage}' not found or empty")
            
            # Extract column names from both stages - use the data parameter from ExportBaseProcessor
            raw_columns = list(data.columns)
            desired_columns = list(target_data.columns)
            
            # Generate column analysis
            analysis = self._analyze_columns(raw_columns, desired_columns)
            
            # Write YAML configuration file
            self._write_yaml_config(analysis)
            
            logger.info(f"Generated column configuration in {self.output_file}")
            logger.info(f"Raw columns: {len(analysis['raw'])}, "
                        f"Desired: {len(analysis['desired'])}, "
                        f"To create: {len(analysis['to_create'])}, "
                        f"Renames: {len(analysis['rename_mapping'])}")
            
        except Exception as e:
            raise StepProcessorError(f"Failed to generate column configuration: {e}")
    
    def _analyze_columns(self, raw_columns: list, desired_columns: list) -> dict:
        """
        Analyze column differences and generate rename suggestions with enhanced matching.
        
        Args:
            raw_columns: List of column names from source stage
            desired_columns: List of column names from target stage (preserves order)
            
        Returns:
            Dictionary with analysis results including rename suggestions
        """
        raw_set = set(raw_columns)
        desired_set = set(desired_columns)
        
        # Find columns that exist in both (exact matches)
        exact_matches = raw_set.intersection(desired_set)
        
        # Find columns that need to be created or renamed (in desired but not in raw)
        potential_creates = desired_set - raw_set
        
        # Find potential renames using enhanced bidirectional matching
        rename_mapping, rename_candidates = self._find_enhanced_renames(
            raw_columns, list(potential_creates), exact_matches
        )
        
        # Remaining items are actual creates (not renames)
        actual_creates = potential_creates - set(rename_mapping.values())
        
        # Sort to_create alphabetically for easy scanning (convert to strings first)
        to_create_list = sorted([str(col) for col in actual_creates])
        
        return {
            'raw': [str(col) for col in raw_columns],
            'desired': [str(col) for col in desired_columns],  # Preserves order from target stage
            'to_create': to_create_list,
            'rename_mapping': {str(k): str(v) for k, v in rename_mapping.items()},
            'rename_candidates': rename_candidates  # For potential future use/debugging
        }
    
    def _find_enhanced_renames(self, raw_columns: list, desired_columns: list, exclude_set: set) -> tuple:
        """
        Find potential renames using enhanced bidirectional matching with word overlap.
        
        Args:
            raw_columns: List of raw column names to search
            desired_columns: List of desired column names looking for matches
            exclude_set: Set of columns to exclude (already matched)
            
        Returns:
            Tuple of (rename_mapping dict, candidates dict for debugging)
        """
        rename_mapping = {}
        all_candidates = {}
        used_raw_columns = set(exclude_set)
        
        for desired_col in desired_columns:
            candidates = []
            
            # Find all potential matches for this desired column
            for raw_col in raw_columns:
                if raw_col in used_raw_columns:
                    continue
                    
                # Calculate enhanced similarity score
                score = self._calculate_enhanced_similarity(desired_col, raw_col)
                
                if score >= self.similarity_threshold:
                    candidates.append({
                        'raw_column': raw_col,
                        'score': score,
                        'confidence': self._get_confidence_level(score)
                    })
            
            # Sort candidates by score
            candidates.sort(key=lambda x: x['score'], reverse=True)
            all_candidates[desired_col] = candidates
            
            # For bidirectional confirmation, check if we have a good candidate
            if candidates:
                best_candidate = candidates[0]
                best_raw = best_candidate['raw_column']
                
                # Check bidirectional: is desired_col the best match for best_raw?
                reverse_score = self._find_best_reverse_match(best_raw, desired_columns, used_raw_columns)
                
                if reverse_score and reverse_score['desired_column'] == desired_col:
                    # Bidirectional confirmation - this is likely a real rename
                    rename_mapping[best_raw] = desired_col
                    used_raw_columns.add(best_raw)
        
        return rename_mapping, all_candidates
    
    def _calculate_enhanced_similarity(self, col1: str, col2: str) -> float:
        """
        Calculate enhanced similarity combining character matching and word overlap.
        
        Args:
            col1: First column name
            col2: Second column name
            
        Returns:
            Enhanced similarity score (0.0 to 1.0)
        """
        # Convert to strings to handle datetime columns or other object types
        str1 = str(col1).lower()
        str2 = str(col2).lower()
        
        # Character-level similarity using SequenceMatcher
        char_score = SequenceMatcher(None, str1, str2).ratio()
        
        # Word-level overlap analysis
        words1 = set(re.findall(r'\w+', str1))
        words2 = set(re.findall(r'\w+', str2))
        
        if words1 and words2:
            # Calculate word overlap as Jaccard similarity
            intersection = len(words1.intersection(words2))
            union = len(words1.union(words2))
            word_overlap = intersection / union if union > 0 else 0
        else:
            word_overlap = 0
        
        # Weighted combination: character similarity (60%) + word overlap (40%)
        enhanced_score = (char_score * 0.6) + (word_overlap * 0.4)
        
        return enhanced_score
    
    def _find_best_reverse_match(self, raw_col: str, desired_columns: list, used_columns: set) -> dict:
        """
        Find the best desired column match for a given raw column (reverse direction).
        
        Args:
            raw_col: Raw column to find a match for
            desired_columns: List of desired columns to search
            used_columns: Set of columns to exclude
            
        Returns:
            Dictionary with best match info, or None if no good match
        """
        best_score = 0
        best_match = None
        
        for desired_col in desired_columns:
            if desired_col in used_columns:
                continue
                
            score = self._calculate_enhanced_similarity(raw_col, desired_col)
            
            if score > best_score and score >= self.similarity_threshold:
                best_score = score
                best_match = desired_col
        
        if best_match:
            return {
                'desired_column': best_match,
                'score': best_score,
                'confidence': self._get_confidence_level(best_score)
            }
        
        return None
    
    def _get_confidence_level(self, score: float) -> str:
        """
        Convert similarity score to confidence level description.
        
        Args:
            score: Similarity score (0.0 to 1.0)
            
        Returns:
            Confidence level string
        """
        if score >= 0.95:
            return "very_high"
        elif score >= 0.9:
            return "high"
        elif score >= 0.8:
            return "medium"
        else:
            return "low"
    
    def _write_yaml_config(self, analysis: dict) -> None:
        """
        Write the YAML configuration file with proper formatting.
        
        Args:
            analysis: Dictionary with column analysis results
        """
        # Apply variable substitution to output path if available
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            output_path = self.variable_substitution.substitute(self.output_file)
        else:
            output_path = self.output_file
        
        # Ensure output directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("settings:\n")
            f.write("  variables:\n")
            
            # Write raw columns (max 3 per line, bracket format)
            f.write("    var_columns_raw: [\n")
            self._write_column_list(f, analysis['raw'], max_per_line=3, indent=6)
            f.write("    ]\n\n")
            
            # Write desired columns (max 3 per line, preserves order, bracket format)
            f.write("    var_columns_to_keep: [\n")
            self._write_column_list(f, analysis['desired'], max_per_line=3, indent=6)
            f.write("    ]\n\n")
            
            # Write to_create columns (max 3 per line, alphabetized, bracket format)
            f.write("    var_columns_to_create: [\n")
            self._write_column_list(f, analysis['to_create'], max_per_line=3, indent=6)
            f.write("    ]\n\n")
            
            # Write rename mapping
            f.write("    var_rename_mapping:\n")
            for old_col, new_col in analysis['rename_mapping'].items():
                f.write(f"      \"{old_col}\": \"{new_col}\"\n")
            
            # Optional recipe section
            if self.include_recipe_section:
                f.write("\n")
                self._write_recipe_section(f)
    
    def _write_column_list(self, file_obj, columns: list, max_per_line: int, indent: int) -> None:
        """
        Write a list of columns with specified formatting.
        
        Args:
            file_obj: File object to write to
            columns: List of column names
            max_per_line: Maximum columns per line
            indent: Number of spaces to indent
        """
        indent_str = " " * indent
        
        for i, col in enumerate(columns):
            if i == 0:
                file_obj.write(f"{indent_str}\"{col}\"")
            elif i % max_per_line == 0:
                file_obj.write(",\n")
                file_obj.write(f"{indent_str}\"{col}\"")
            else:
                file_obj.write(f", \"{col}\"")
        
        if columns:  # Only write final newline if there were columns
            file_obj.write("\n")
    
    def _write_recipe_section(self, file_obj) -> None:
        """
        Write optional recipe section with rename_columns step.
        
        Args:
            file_obj: File object to write to
        """
        file_obj.write("# Optional complete recipe section\n")
        file_obj.write("recipe:\n")
        file_obj.write("  - step_description: \"Rename columns from raw to desired format\"\n")
        file_obj.write("    processor_type: \"rename_columns\"\n")
        file_obj.write(f"    source_stage: \"{self.source_stage}\"\n")
        file_obj.write("    rename_type: \"mapping\"\n")
        file_obj.write("    mapping: \"{dict:var_rename_mapping}\"\n")
        file_obj.write("    save_to_stage: \"renamed_data\"\n")
    
    @classmethod
    def get_capabilities(cls) -> dict:
        """
        Get comprehensive information about processor capabilities.
        
        Returns:
            Dictionary describing processor capabilities
        """
        return {
            'description': 'Generate column configuration files by comparing raw and desired column structures',
            'category': 'Configuration Generation',
            'stage_to_file': True,
            'analysis_features': [
                'Bidirectional fuzzy matching for rename detection',
                'Enhanced similarity scoring with word overlap analysis',
                'Conservative matching to avoid false positives',
                'Alphabetized to-create lists for easy scanning',
                'Ready-to-use recipe section generation'
            ],
            'output_structure': [
                'Raw columns list (preserves source order)',
                'Desired columns list (preserves target order)', 
                'To-create columns list (alphabetized)',
                'Rename mapping dictionary for rename_columns processor',
                'Optional complete recipe section'
            ],
            'similarity_features': {
                'character_matching': 'SequenceMatcher for string similarity (60% weight)',
                'word_overlap': 'Jaccard similarity for shared words (40% weight)',
                'bidirectional_confirmation': 'Prevents false matches by requiring mutual best match',
                'confidence_levels': ['very_high (≥0.95)', 'high (≥0.9)', 'medium (≥0.8)', 'low (<0.8)']
            },
            'parameters': {
                'required': ['source_stage', 'template_stage', 'output_file'],
                'optional': ['similarity_threshold', 'include_recipe_section']
            },
            'column_handling': {
                'datetime_columns': 'Automatically converts datetime column names to strings',
                'mixed_types': 'Handles any object type as column name via str() conversion',
                'special_characters': 'Preserves all characters in column names',
                'empty_columns': 'Handles empty or None column names gracefully'
            },
            'output_format': {
                'variable_naming': 'Uses select_columns compatible naming (var_columns_to_keep, etc.)',
                'formatting': 'Bracket format with max 3 columns per line for easy copy-paste',
                'file_type': 'Direct YAML writing (not through FileWriter) for precise formatting'
            },
            'use_cases': [
                'Automate recipe column configuration maintenance',
                'Sync recipes with changing manual report formats',
                'Detect column renames vs new column creation',
                'Generate reusable column mappings for recipe families',
                'Document implicit business logic in column transformations'
            ],
            'workflow_integration': {
                'input': 'Two stages with different column structures',
                'output': 'YAML configuration file with organized column lists',
                'next_steps': 'Copy variables into main recipes for column processing'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the generate_column_config processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('generate_column_config')


# End of file #
