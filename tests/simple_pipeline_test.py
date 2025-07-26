#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import tempfile
from pathlib import Path

from excel_recipe_processor.core.pipeline import ExcelPipeline

def test_refactored_pipeline():
    """Test the refactored pipeline with FileReader/FileWriter integration."""
    
    print("Testing refactored pipeline...")
    
    # Create test data
    test_data = pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C'],
        'Sales': [100, 200, 150],
        'Region': ['North', 'South', 'North']
    })
    
    # Create temporary test files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create input file
        input_file = temp_path / 'test_input.xlsx'
        test_data.to_excel(input_file, index=False)
        
        # Create recipe file
        recipe_content = """
# Simple test recipe
recipe:
  - step_description: "Filter for high sales"
    processor_type: "filter_data"
    filters:
      - column: "Sales"
        condition: "greater_than"
        value: 120

settings:
  variables:
    output_prefix: "filtered"
  output_filename: "{output_prefix}_results_{YY}{MM}.xlsx"
"""
        
        recipe_file = temp_path / 'test_recipe.yaml'
        with open(recipe_file, 'w') as f:
            f.write(recipe_content)
        
        # Test the pipeline
        try:
            pipeline = ExcelPipeline()
            
            # Test step-by-step execution
            print("Testing step-by-step execution...")
            
            # Load recipe
            recipe_data = pipeline.load_recipe(recipe_file)
            print(f"✓ Loaded recipe with {len(recipe_data.get('recipe', []))} steps")
            
            # Check variables
            variables = pipeline.get_available_variables()
            print(f"✓ Available variables: {len(variables)} found")
            if 'output_prefix' in variables:
                print(f"  Custom variable: output_prefix = {variables['output_prefix']}")
            
            # Load input
            input_data = pipeline.load_input_file(input_file)
            print(f"✓ Loaded input data: {len(input_data)} rows")
            
            # Execute recipe
            result = pipeline.execute_recipe()
            print(f"✓ Executed recipe: {len(result)} rows remaining")
            
            # Test variable substitution in output
            output_template = "{output_prefix}_test_{YY}{MM}.xlsx"
            output_path = pipeline.substitute_template(output_template)
            print(f"✓ Template substitution: {output_template} → {output_path}")
            
            # Save result
            final_output = temp_path / output_path
            pipeline.save_result(final_output)
            print(f"✓ Saved result to: {final_output}")
            
            # Verify the file was created
            if final_output.exists():
                print("✓ Output file created successfully")
            else:
                print("✗ Output file not found")
                return False
                
            # Test pipeline summary
            summary = pipeline.get_pipeline_summary()
            print(f"✓ Pipeline summary: {summary['steps_executed']} steps executed")
            
            # Test complete pipeline shortcut
            print("\nTesting complete pipeline shortcut...")
            
            output_file2 = temp_path / 'shortcut_output.xlsx'
            result2 = pipeline.run_complete_pipeline(
                recipe_path=recipe_file,
                input_path=input_file,
                output_path=output_file2
            )
            
            if output_file2.exists() and len(result2) == len(result):
                print("✓ Complete pipeline shortcut successful")
            else:
                print("✗ Complete pipeline shortcut failed")
                return False
            
            print("\n🎉 All pipeline refactoring tests passed!")
            return True
            
        except Exception as e:
            print(f"✗ Pipeline test failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def test_variable_integration():
    """Test enhanced variable integration."""
    
    print("\nTesting variable integration...")
    
    try:
        pipeline = ExcelPipeline()
        
        # Test adding custom variables
        pipeline.add_custom_variable('test_var', 'test_value')
        variables = pipeline.get_available_variables()
        
        if 'test_var' in variables and variables['test_var'] == 'test_value':
            print("✓ Custom variable addition works")
        else:
            print("✗ Custom variable addition failed")
            return False
        
        # Test template validation
        templates = [
            '{year}_{month}_report.xlsx',  # Valid
            '{unknown_var}_report.xlsx',   # Invalid
            '{test_var}_{date}.xlsx'       # Valid
        ]
        
        validation = pipeline.validate_variable_templates(templates)
        
        valid_count = sum(1 for result in validation.values() if result['valid'])
        print(f"✓ Template validation: {valid_count}/{len(templates)} templates valid")
        
        if valid_count == 2:  # Should be 2 valid, 1 invalid
            print("✓ Variable integration works correctly")
            return True
        else:
            print("✗ Variable integration failed")
            return False
            
    except Exception as e:
        print(f"✗ Variable integration test failed: {e}")
        return False

if __name__ == '__main__':
    success = True
    
    success &= test_refactored_pipeline()
    success &= test_variable_integration()
    
    if success:
        print("\n🎉 All refactored pipeline tests passed!")
    else:
        print("\n❌ Some refactored pipeline tests failed!")
    
    print(f"\nResult: {'PASS' if success else 'FAIL'}")
