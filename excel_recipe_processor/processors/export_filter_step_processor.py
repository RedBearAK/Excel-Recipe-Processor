"""
Export filter step processor for Excel automation recipes.

excel_recipe_processor/processors/export_filter_step_processor.py

Generates copy-paste ready YAML/JSON recipe steps from reviewed filter terms.
"""

import json
import yaml
import logging

from pathlib import Path

from excel_recipe_processor.core.base_processor import ExportBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class ExportFilterStepProcessor(ExportBaseProcessor):
    """
    Export processor that generates filter_data recipe steps from reviewed filter terms.
    
    Reads accepted filter terms from a stage and generates copy-paste ready YAML
    that can be added to recipes as filter_data processor steps.
    """
    
    @classmethod
    def get_minimal_config(cls):
        return {
            'source_stage': 'filter_terms_reviewed',
            'output_file': 'generated_filter_step.yaml',
            'target_stage': 'stg_data_to_filter'
        }
    
    def __init__(self, step_config: dict):
        super().__init__(step_config)
        
        # Configuration for reading reviewed terms
        self.acceptance_column = self.get_config_value('acceptance_column', 'User_Verified')
        self.acceptance_values = self.get_config_value('acceptance_values', ['KEEP', 'YES', 'TRUE'])
        self.column_name_field = self.get_config_value('column_name_field', 'Column_Name')
        self.filter_term_field = self.get_config_value('filter_term_field', 'Filter_Term')
        self.term_type_field = self.get_config_value('term_type_field', 'Term_Type')
        
        # Configuration for generated filter step
        self.output_file = self.get_config_value('output_file')
        self.target_stage = self.get_config_value('target_stage', 'stg_data_to_filter')
        self.output_stage = self.get_config_value('output_stage', 'stg_data_filtered')
        self.step_description = self.get_config_value('step_description', 'Filter data using detected terms')
        
        # Output format options
        self.output_format = self.get_config_value('output_format', 'yaml').lower()
        self.include_full_recipe = self.get_config_value('include_full_recipe', True)
        
        # Validate required configuration
        if not self.output_file:
            raise StepProcessorError("export_filter_step processor requires 'output_file' parameter")
    
    def save_data(self, data) -> None:
        """Generate and save filter step to file (implements ExportBaseProcessor abstract method)."""
        try:
            # Filter to accepted terms only
            acceptance_mask = data[self.acceptance_column].astype(str).str.upper().isin(
                [val.upper() for val in self.acceptance_values]
            )
            accepted_terms = data[acceptance_mask]
            
            if len(accepted_terms) == 0:
                logger.warning("No accepted terms found - generating empty filter step")
                content = self._generate_empty_step()
            else:
                # Generate the content based on format
                if self.output_format == 'json':
                    content = self._generate_json_config(accepted_terms)
                else:  # Default to YAML
                    content = self._generate_yaml_config(accepted_terms)
            
            # Write to file
            output_path = Path(self.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            filter_count = len(accepted_terms)
            columns_affected = accepted_terms[self.column_name_field].nunique() if filter_count > 0 else 0
            
            logger.info(f"Generated {self.output_format.upper()} filter step with {filter_count} conditions")
            logger.info(f"Affects {columns_affected} columns, saved to: {output_path}")
            
            # Log the content for easy copy-paste
            if len(content) < 2000:  # Don't flood logs with huge files
                logger.info("Generated content:")
                logger.info(content)
            
        except Exception as e:
            raise StepProcessorError(f"Failed to generate filter step file '{self.output_file}': {e}")
    
    def _generate_yaml_config(self, accepted_terms) -> str:
        """Generate complete YAML configuration."""
        if self.include_full_recipe:
            return self._generate_full_yaml_recipe(accepted_terms)
        else:
            return self._generate_yaml_step_only(accepted_terms)
    
    def _generate_full_yaml_recipe(self, accepted_terms) -> str:
        """Generate a complete recipe with settings and filter step using yaml module."""
        filter_conditions = self._build_filter_conditions(accepted_terms)
        filter_count = len(filter_conditions)
        columns_affected = accepted_terms[self.column_name_field].nunique()
        
        # Build recipe structure as dict, then convert to YAML
        recipe_data = {
            'settings': {
                'description': 'Auto-generated filter from detected terms',
                'stages': [
                    {
                        'stage_name': self.target_stage,
                        'description': 'Source data to be filtered',
                        'protected': False
                    },
                    {
                        'stage_name': self.output_stage,
                        'description': 'Filtered data output',
                        'protected': False
                    }
                ]
            },
            'recipe': [
                {
                    'step_description': self.step_description,
                    'processor_type': 'filter_data',
                    'source_stage': self.target_stage,
                    'filters': [
                        {
                            'column': condition['column'],
                            'condition': condition['condition'],
                            'value': condition['value']
                        }
                        for condition in filter_conditions
                    ],
                    'save_to_stage': self.output_stage
                }
            ]
        }
        
        # Generate header comment
        header = f"""# Generated Filter Recipe
# Created from {len(accepted_terms)} accepted filter terms  
# Affects {columns_affected} columns with {filter_count} filter conditions

"""
        
        # Convert to YAML with proper formatting
        yaml_content = yaml.dump(recipe_data, default_flow_style=False, sort_keys=False, indent=2)
        
        return header + yaml_content
    
    def _generate_yaml_step_only(self, accepted_terms) -> str:
        """Generate just the filter step using yaml module."""
        filter_conditions = self._build_filter_conditions(accepted_terms)
        
        # Build step structure as dict
        step_data = {
            'step_description': self.step_description,
            'processor_type': 'filter_data',
            'source_stage': self.target_stage,
            'filters': [
                {
                    'column': condition['column'],
                    'condition': condition['condition'],
                    'value': condition['value']
                }
                for condition in filter_conditions
            ],
            'save_to_stage': self.output_stage
        }
        
        # Generate header comment
        header = f"""# Generated filter step - copy into your recipe
# Based on {len(accepted_terms)} accepted filter terms

"""
        
        # Convert to YAML (as a list item)
        yaml_content = yaml.dump([step_data], default_flow_style=False, sort_keys=False, indent=2)
        
        return header + yaml_content
    
    def _generate_json_config(self, accepted_terms) -> str:
        """Generate JSON configuration."""
        filter_conditions = self._build_filter_conditions(accepted_terms)
        
        if self.include_full_recipe:
            config = {
                "settings": {
                    "description": "Auto-generated filter from detected terms",
                    "stages": [
                        {
                            "stage_name": self.target_stage,
                            "description": "Source data to be filtered",
                            "protected": False
                        },
                        {
                            "stage_name": self.output_stage,
                            "description": "Filtered data output", 
                            "protected": False
                        }
                    ]
                },
                "recipe": [
                    {
                        "step_description": self.step_description,
                        "processor_type": "filter_data",
                        "source_stage": self.target_stage,
                        "filters": [
                            {
                                "column": condition['column'],
                                "condition": condition['condition'],
                                "value": condition['value']
                            }
                            for condition in filter_conditions
                        ],
                        "save_to_stage": self.output_stage
                    }
                ]
            }
        else:
            # Just the step
            config = {
                "step_description": self.step_description,
                "processor_type": "filter_data", 
                "source_stage": self.target_stage,
                "filters": [
                    {
                        "column": condition['column'],
                        "condition": condition['condition'],
                        "value": condition['value']
                    }
                    for condition in filter_conditions
                ],
                "save_to_stage": self.output_stage
            }
        
        return json.dumps(config, indent=2, ensure_ascii=False)
    
    def _build_filter_conditions(self, accepted_terms) -> list:
        """Build list of filter conditions from accepted terms."""
        conditions = []
        
        for _, row in accepted_terms.iterrows():
            column_name = str(row[self.column_name_field])
            filter_term = str(row[self.filter_term_field])
            term_type = str(row[self.term_type_field])
            
            # Choose condition based on term type
            if term_type == 'categorical_value':
                condition_type = "not_equals"  # Exact value exclusion
            else:  # text_ngram
                condition_type = "not_contains"  # Substring exclusion
            
            conditions.append({
                'column': column_name,
                'condition': condition_type,
                'value': filter_term
            })
        
        return conditions
    
    def _generate_empty_step(self) -> str:
        """Generate empty filter step when no terms are accepted."""
        if self.output_format == 'json':
            empty_data = {
                "comment": "No filter terms were accepted for generation",
                "step_description": "No filters generated",
                "processor_type": "filter_data",
                "source_stage": self.target_stage,
                "filters": [],
                "save_to_stage": self.output_stage
            }
            return json.dumps(empty_data, indent=2, ensure_ascii=False)
        else:
            # Use yaml module for consistency
            empty_step = {
                'step_description': 'No filters generated',
                'processor_type': 'filter_data',
                'source_stage': self.target_stage,
                'filters': [],
                'save_to_stage': self.output_stage
            }
            
            header = """# No filter terms were accepted for generation
# Review your filter terms and mark some as KEEP to generate filters

"""
            yaml_content = yaml.dump([empty_step], default_flow_style=False, sort_keys=False, indent=2)
            return header + yaml_content
    
    @classmethod
    def get_capabilities(cls):
        """Get processor capabilities information."""
        return {
            'description': 'Generate copy-paste ready filter_data steps from reviewed filter terms',
            'export_formats': ['yaml', 'json'],
            'export_features': [
                'full_recipe_generation',
                'step_only_generation', 
                'proper_column_quoting',
                'categorical_and_text_filters',
                'copy_paste_ready_output'
            ],
            'stage_integration': [
                'reads_reviewed_filter_terms',
                'no_stage_output_required'
            ],
            'examples': [
                'Generate YAML recipe from Excel-reviewed terms',
                'Create JSON filter step for programmatic use',
                'Export step-only format for insertion into existing recipes'
            ]
        }
    
    def get_usage_examples(self) -> dict:
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('export_filter_step')


# End of file #
