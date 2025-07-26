"""
Test the complete end-to-end pipeline functionality.
"""

import pandas as pd
from pathlib import Path

from excel_recipe_processor.core.pipeline import ExcelPipeline, PipelineError


def create_test_excel_file():
    """Create a test Excel file with sample data."""
    test_data = pd.DataFrame({
        'Product_Name': [
            'CANNED BEANS', 'FRESH FISH', 'CANNED CORN', 'DRIED FRUIT', 
            'CANNED SOUP', 'FRESH BREAD', 'CANNED TUNA', 'TEST PRODUCT'
        ],
        'Component': ['FLESH', 'FLESH', 'FLESH', 'FLESH', 'FLESH', 'FLESH', 'FLESH', 'FLESH'],
        'Quantity': [100, 50, 75, 25, 200, 30, 80, 5],
        'Price': [10.50, 25.00, 15.75, 8.25, 12.00, 3.50, 18.00, 1.00],
        'Department': ['Grocery', 'Fresh', 'Grocery', 'Snacks', 'Grocery', 'Bakery', 'Grocery', 'Test'],
        'Status': ['Active', 'Active', 'Cancelled', 'Active', 'Active', 'Active', 'Active', 'Test']
    })
    
    test_file = Path('test_input.xlsx')
    test_data.to_excel(test_file, index=False, sheet_name='ProductData')
    
    print(f"✓ Created test Excel file: {test_file} ({len(test_data)} rows)")
    return test_file


def create_test_recipe_file():
    """Create a test recipe file similar to the van report workflow."""
    recipe_content = """
# Test Recipe - simulates part of the van report workflow
recipe:
  - step_description: "Filter for canned products only"
    processor_type: "filter_data"
    filters:
      - column: "Product_Name"
        condition: "contains"
        value: "CANNED"
        
  - step_description: "Remove cancelled and test items"
    processor_type: "filter_data"
    filters:
      - column: "Status"
        condition: "not_equals"
        value: "Cancelled"
      - column: "Status"
        condition: "not_equals"  
        value: "Test"
        
  - step_description: "Filter for products with reasonable quantity"
    processor_type: "filter_data"
    filters:
      - column: "Quantity"
        condition: "greater_than"
        value: 50

settings:
  output_filename: "processed_products.xlsx"
  create_backup: true
"""
    
    recipe_file = Path('test_recipe.yaml')
    with open(recipe_file, 'w') as f:
        f.write(recipe_content)
    
    print(f"✓ Created test recipe file: {recipe_file}")
    return recipe_file


def test_complete_pipeline_workflow():
    """Test the complete pipeline from recipe to result."""
    
    print("Testing complete pipeline workflow...")
    
    # Create test files
    input_file = create_test_excel_file()
    recipe_file = create_test_recipe_file()
    output_file = Path('test_output_pipeline.xlsx')
    
    try:
        # Initialize pipeline
        pipeline = ExcelPipeline()
        
        # Test step-by-step execution
        print("\n--- Step-by-step execution ---")
        
        # Step 1: Load recipe
        recipe_data = pipeline.load_recipe(recipe_file)
        print(f"✓ Loaded recipe with {len(pipeline.recipe_loader.get_steps())} steps")
        
        # Step 2: Load input file
        input_data = pipeline.load_input_file(input_file)
        print(f"✓ Loaded input data: {len(input_data)} rows, {len(input_data.columns)} columns")
        
        # Step 3: Execute recipe
        result_data = pipeline.execute_recipe()
        print(f"✓ Executed recipe: {len(result_data)} rows remaining")
        
        # Step 4: Save result
        pipeline.save_result(output_file)
        print(f"✓ Saved result to: {output_file}")
        
        # Verify the filtering worked correctly
        print(f"\n--- Verification ---")
        print(f"Original data: {len(input_data)} rows")
        print(f"Final result: {len(result_data)} rows")
        
        # Should have filtered to: CANNED products, not cancelled/test, quantity > 50
        # Expected: CANNED BEANS (100), CANNED SOUP (200), CANNED TUNA (80)
        expected_products = {'CANNED BEANS', 'CANNED SOUP', 'CANNED TUNA'}
        actual_products = set(result_data['Product_Name'].tolist())
        
        if actual_products == expected_products:
            print(f"✓ Filtering worked correctly: {actual_products}")
        else:
            print(f"✗ Unexpected filtering result: {actual_products}")
            print(f"Expected: {expected_products}")
            return False
        
        # Test pipeline summary
        summary = pipeline.get_pipeline_summary()
        print(f"✓ Pipeline summary: {summary}")
        
        return True
        
    except Exception as e:
        print(f"✗ Pipeline test failed: {e}")
        return False
    
    finally:
        # Clean up test files
        for file_path in [input_file, recipe_file, output_file]:
            if file_path.exists():
                file_path.unlink()
        print("✓ Cleaned up test files")


def test_complete_pipeline_shortcut():
    """Test the run_complete_pipeline shortcut method."""
    
    print("\n\nTesting complete pipeline shortcut...")
    
    # Create test files
    input_file = create_test_excel_file()
    recipe_file = create_test_recipe_file()
    output_file = Path('test_output_shortcut.xlsx')
    
    try:
        # Initialize pipeline
        pipeline = ExcelPipeline()
        
        # Run complete pipeline in one call
        result = pipeline.run_complete_pipeline(
            recipe_path=recipe_file,
            input_path=input_file,
            output_path=output_file
        )
        
        print(f"✓ Complete pipeline executed: {len(result)} rows in result")
        
        # Verify output file exists
        if output_file.exists():
            print(f"✓ Output file created: {output_file}")
        else:
            print("✗ Output file not created")
            return False
        
        # Verify the data by reading it back
        verification_data = pd.read_excel(output_file)
        if len(verification_data) == len(result):
            print("✓ Saved data matches result data")
        else:
            print("✗ Saved data doesn't match result")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ Complete pipeline shortcut failed: {e}")
        return False
    
    finally:
        # Clean up test files
        for file_path in [input_file, recipe_file, output_file]:
            if file_path.exists():
                file_path.unlink()
        print("✓ Cleaned up shortcut test files")


def test_error_handling():
    """Test error handling in the pipeline."""
    
    print("\n\nTesting pipeline error handling...")
    
    pipeline = ExcelPipeline()
    
    # Test recipe execution without loading recipe
    try:
        pipeline.execute_recipe()
        print("✗ Should have failed without loaded recipe")
    except PipelineError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test recipe execution without loading input data
    try:
        recipe_file = create_test_recipe_file()
        pipeline.load_recipe(recipe_file)
        pipeline.execute_recipe()
        print("✗ Should have failed without input data")
        recipe_file.unlink()  # cleanup
    except PipelineError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test with non-existent recipe file
    try:
        pipeline.load_recipe('nonexistent_recipe.yaml')
        print("✗ Should have failed with non-existent recipe")
    except PipelineError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test with non-existent input file
    try:
        recipe_file = create_test_recipe_file()
        pipeline.load_recipe(recipe_file)
        pipeline.load_input_file('nonexistent_input.xlsx')
        print("✗ Should have failed with non-existent input")
        recipe_file.unlink()  # cleanup
    except PipelineError as e:
        print(f"✓ Caught expected error: {e}")


def test_registry_functionality():
    """Test that the processor registry is working."""
    
    print("\n\nTesting processor registry...")
    
    from excel_recipe_processor.processors.base_processor import registry
    
    # Check that filter_data processor is registered
    registered_types = registry.get_registered_types()
    print(f"✓ Registered processor types: {registered_types}")
    
    if 'filter_data' in registered_types:
        print("✓ filter_data processor is registered")
    else:
        print("✗ filter_data processor not registered")
        return False
    
    # Test creating a processor
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Test filter',
        'filters': []
    }
    
    try:
        processor = registry.create_processor(step_config)
        print(f"✓ Created processor from registry: {processor}")
        return True
    except Exception as e:
        print(f"✗ Failed to create processor: {e}")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_complete_pipeline_workflow()
    success &= test_complete_pipeline_shortcut()
    success &= test_registry_functionality()
    test_error_handling()
    
    if success:
        print("\n🎉 All complete pipeline tests passed!")
        print("The Excel automation system is working end-to-end!")
    else:
        print("\n❌ Some pipeline tests failed!")
