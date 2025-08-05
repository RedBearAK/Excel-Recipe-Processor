"""
Simple integration test for the lookup processor with the overall pipeline.
"""

import pandas as pd

from pathlib import Path


def test_lookup_integration():
    """Test that lookup processor integrates with the pipeline system."""
    
    print("Testing lookup processor integration...")
    
    # Test that the processor is registered
    # Import pipeline first to trigger processor registration
    from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
    from excel_recipe_processor.core.base_processor import registry
    
    registered_types = registry.get_registered_types()
    print(f"✓ Registered processor types: {registered_types}")
    
    if 'lookup_data' in registered_types:
        print("✓ lookup_data processor is registered")
    else:
        print("✗ lookup_data processor not registered")
        return False
    
    # Test creating the processor from registry
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Test lookup',
        'lookup_source': {'Code': ['A001'], 'Category': ['Electronics']},
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category']
    }
    
    try:
        processor = registry.create_processor(step_config)
        print(f"✓ Created processor from registry: {processor}")
    except Exception as e:
        print(f"✗ Failed to create processor: {e}")
        return False
    
    # Test execution
    test_data = pd.DataFrame({
        'Product_Code': ['A001', 'B002'],
        'step_description': ['Widget', 'Gadget']
    })
    
    try:
        result = processor.execute(test_data)
        print(f"✓ Executed lookup: {len(result)} rows, {len(result.columns)} columns")
        
        if 'Category' in result.columns:
            print("✓ Lookup column added successfully")
            return True
        else:
            print("✗ Lookup column not added")
            return False
            
    except Exception as e:
        print(f"✗ Execution failed: {e}")
        return False


def test_with_pipeline():
    """Test lookup processor with the full pipeline."""
    
    print("\nTesting with full pipeline...")
    
    try:
        from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
        
        # Create test data files
        main_data = pd.DataFrame({
            'Product_Code': ['A001', 'B002', 'C003'],
            'Quantity': [100, 50, 75]
        })
        
        lookup_data = pd.DataFrame({
            'Code': ['A001', 'B002', 'C003'],
            'Category': ['Electronics', 'Tools', 'Hardware'],
            'Price': [10.50, 25.00, 15.75]
        })
        
        # Save test files
        input_file = Path('test_main_data.xlsx')
        lookup_file = Path('test_lookup_data.xlsx')
        output_file = Path('test_pipeline_output.xlsx')
        
        main_data.to_excel(input_file, index=False)
        lookup_data.to_excel(lookup_file, index=False)
        
        # Create recipe content
        recipe_content = f"""
recipe:
  - step_description: "Lookup product info"
    processor_type: "lookup_data"
    lookup_source: "{lookup_file}"
    lookup_key: "Code"
    source_key: "Product_Code"
    lookup_columns: ["Category", "Price"]

settings:
  output_filename: "{output_file}"
"""
        
        recipe_file = Path('test_lookup_recipe.yaml')
        with open(recipe_file, 'w') as f:
            f.write(recipe_content)
        
        # Run pipeline
        pipeline = RecipePipeline()
        result = pipeline.run_complete_recipe(
            recipe_path=recipe_file,
            cli_variables=None
        )
        
        print(f"✓ Pipeline completed: {len(result)} rows, {len(result.columns)} columns")
        
        # Check results
        if 'Category' in result.columns and 'Price' in result.columns:
            print("✓ Pipeline with lookup processor worked correctly")
            return True
        else:
            print("✗ Pipeline with lookup processor failed")
            return False
        
    except Exception as e:
        print(f"✗ Pipeline test failed: {e}")
        return False
    
    finally:
        # Clean up test files
        for file_path in [input_file, lookup_file, output_file, recipe_file]:
            if file_path.exists():
                file_path.unlink()
        print("✓ Cleaned up test files")


if __name__ == '__main__':
    success = True
    
    success &= test_lookup_integration()
    success &= test_with_pipeline()
    
    if success:
        print("\n🎉 Lookup processor integration tests passed!")
        print("The lookup processor is ready to use in your Excel automation recipes!")
    else:
        print("\n❌ Some integration tests failed!")
