"""
Test the SaveStageProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.base_processor import StepProcessorError
from excel_recipe_processor.processors.save_stage_processor import SaveStageProcessor


def create_sample_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004'],
        'Customer_Name': ['Alice Corp', 'Bob Industries', 'Charlie LLC', 'Delta Inc'],
        'Region': ['West', 'East', 'North', 'South'],
        'Order_Value': [1000, 2000, 1500, 800]
    })


def test_basic_save_functionality():
    """Test basic stage saving functionality."""
    
    print("Testing basic save functionality...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        step_config = {
            'processor_type': 'save_stage',
            'step_description': 'Save customer data',
            'stage_name': 'Customer Master Data',
            'description': 'Complete customer information for analysis'
        }
        
        processor = SaveStageProcessor(step_config)
        result = processor.execute(test_df)
        
        # Check that input data is unchanged
        if not result.equals(test_df):
            print("✗ Input data was modified")
            return False
        
        # Check that stage was created
        if not StageManager.stage_exists('Customer Master Data'):
            print("✗ Stage was not created")
            return False
        
        # Check saved data integrity
        saved_data = StageManager.load_stage('Customer Master Data')
        if saved_data.equals(test_df):
            print("✓ Basic save functionality worked correctly")
            return True
        else:
            print("✗ Saved data does not match input data")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_overwrite_behavior():
    """Test stage overwrite behavior."""
    
    print("\nTesting overwrite behavior...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df1 = create_sample_data()
        test_df2 = pd.DataFrame({
            'Product_ID': ['P001', 'P002'],
            'Product_Name': ['Widget A', 'Gadget B']
        })
        
        # Save initial stage
        step_config = {
            'processor_type': 'save_stage',
            'stage_name': 'Test Overwrite Stage',
            'description': 'Initial data'
        }
        
        processor = SaveStageProcessor(step_config)
        processor.execute(test_df1)
        
        # Verify initial save
        saved_data = StageManager.load_stage('Test Overwrite Stage')
        if not saved_data.equals(test_df1):
            print("✗ Initial save failed")
            return False
        
        # Try to save again without overwrite (should fail)
        try:
            processor.execute(test_df2)
            print("✗ Should have failed without overwrite=true")
            return False
        except StepProcessorError as e:
            print(f"✓ Correctly prevented overwrite: {e}")
        
        # Now with overwrite=true
        step_config['overwrite'] = True
        step_config['description'] = 'Overwritten data'
        
        processor = SaveStageProcessor(step_config)
        processor.execute(test_df2)
        
        # Verify stage was overwritten
        saved_data = StageManager.load_stage('Test Overwrite Stage')
        if saved_data.equals(test_df2):
            print("✓ Overwrite behavior worked correctly")
            return True
        else:
            print("✗ Overwrite failed")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_metadata_tracking():
    """Test that stage metadata is properly tracked."""
    
    print("\nTesting metadata tracking...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        step_config = {
            'processor_type': 'save_stage',
            'step_description': 'Save for metadata test',
            'stage_name': 'Metadata Test Stage', 
            'description': 'Stage created for testing metadata tracking'
        }
        
        processor = SaveStageProcessor(step_config)
        processor.execute(test_df)
        
        # Check metadata
        stage_info = StageManager.list_stages()
        metadata = stage_info.get('Metadata Test Stage', {})
        
        expected_description = 'Stage created for testing metadata tracking'
        expected_step_name = 'Save for metadata test'
        expected_rows = len(test_df)
        expected_columns = len(test_df.columns)
        
        if (metadata.get('description') == expected_description and
            metadata.get('step_name') == expected_step_name and
            metadata.get('rows') == expected_rows and
            metadata.get('columns') == expected_columns and
            'created_at' in metadata and
            'memory_usage_mb' in metadata):
            print("✓ Metadata tracking worked correctly")
            return True
        else:
            print(f"✗ Metadata tracking failed: {metadata}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_multiple_stages():
    """Test saving multiple stages."""
    
    print("\nTesting multiple stages...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        customer_df = create_sample_data()
        product_df = pd.DataFrame({
            'Product_ID': ['P001', 'P002', 'P003'],
            'Product_Name': ['Widget A', 'Widget B', 'Gadget X'],
            'Category': ['Electronics', 'Electronics', 'Hardware']
        })
        
        # Save first stage
        step_config1 = {
            'processor_type': 'save_stage',
            'stage_name': 'Customer Data',
            'description': 'Customer master data'
        }
        
        processor1 = SaveStageProcessor(step_config1)
        processor1.execute(customer_df)
        
        # Save second stage
        step_config2 = {
            'processor_type': 'save_stage',
            'stage_name': 'Product Catalog',
            'description': 'Product master data'
        }
        
        processor2 = SaveStageProcessor(step_config2)
        processor2.execute(product_df)
        
        # Verify both stages exist
        stage_summary = StageManager.get_stage_summary()
        
        if (stage_summary['total_stages'] == 2 and
            StageManager.stage_exists('Customer Data') and
            StageManager.stage_exists('Product Catalog')):
            print("✓ Multiple stages saved correctly")
            return True
        else:
            print(f"✗ Multiple stages failed: {stage_summary}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_error_handling():
    """Test error handling for various invalid configurations."""
    
    print("\nTesting error handling...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        # Test missing stage_name
        try:
            bad_config = {
                'processor_type': 'save_stage',
                'step_description': 'Missing stage name'
            }
            processor = SaveStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with missing stage_name")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for missing stage_name: {e}")
        
        # Test reserved stage name
        try:
            bad_config = {
                'processor_type': 'save_stage',
                'stage_name': 'input'  # Reserved name
            }
            processor = SaveStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with reserved stage name")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for reserved name: {e}")
        
        # # Test empty DataFrame
        # empty_df = pd.DataFrame()
        # step_config = {
        #     'processor_type': 'save_stage',
        #     'stage_name': 'Empty Stage'
        # }
        
        # # This should work (empty DataFrames are allowed)
        # processor = SaveStageProcessor(step_config)
        # result = processor.execute(empty_df)
        
        # if StageManager.stage_exists('Empty Stage'):
        #     print("✓ Empty DataFrame save handled correctly")
        # else:
        #     print("✗ Empty DataFrame save failed")
        #     return False

        # Test empty DataFrame - should fail with validation error
        empty_df = pd.DataFrame()
        step_config = {
            'processor_type': 'save_stage',
            'stage_name': 'Empty Stage'
        }

        try:
            processor = SaveStageProcessor(step_config)
            processor.execute(empty_df)
            print("✗ Should have failed with empty DataFrame")
            return False
        except StepProcessorError as e:
            if "empty DataFrame" in str(e):
                print("✓ Empty DataFrame properly rejected")
            else:
                print(f"✗ Unexpected error: {e}")
                return False

        print("✓ Error handling worked correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_stage_limit_enforcement():
    """Test that stage limits are enforced."""
    
    print("\nTesting stage limit enforcement...")
    
    # Initialize with very low limit
    StageManager.initialize_stages(max_stages=2)
    
    try:
        test_df = create_sample_data()
        
        # Create first stage
        step_config1 = {
            'processor_type': 'save_stage',
            'stage_name': 'Stage 1'
        }
        processor1 = SaveStageProcessor(step_config1)
        processor1.execute(test_df)
        
        # Create second stage
        step_config2 = {
            'processor_type': 'save_stage',
            'stage_name': 'Stage 2'
        }
        processor2 = SaveStageProcessor(step_config2)
        processor2.execute(test_df)
        
        # Try to create third stage (should fail)
        try:
            step_config3 = {
                'processor_type': 'save_stage',
                'stage_name': 'Stage 3'
            }
            processor3 = SaveStageProcessor(step_config3)
            processor3.execute(test_df)
            print("✗ Should have failed with stage limit exceeded")
            return False
        except StepProcessorError as e:
            if "Maximum number of stages" in str(e):
                print("✓ Stage limit enforcement worked correctly")
                return True
            else:
                print(f"✗ Unexpected error: {e}")
                return False
                
    finally:
        StageManager.cleanup_stages()


def test_data_isolation():
    """Test that saved data is properly isolated (copies)."""
    
    print("\nTesting data isolation...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        # Save stage
        step_config = {
            'processor_type': 'save_stage',
            'stage_name': 'Isolation Test'
        }
        
        processor = SaveStageProcessor(step_config)
        processor.execute(test_df)
        
        # Modify original DataFrame
        test_df.loc[0, 'Customer_Name'] = 'MODIFIED'
        
        # Check that saved data was not affected
        saved_data = StageManager.load_stage('Isolation Test')
        
        if saved_data.loc[0, 'Customer_Name'] != 'MODIFIED':
            print("✓ Data isolation worked correctly")
            return True
        else:
            print("✗ Data isolation failed - original data was not copied")
            return False
            
    finally:
        StageManager.cleanup_stages()


if __name__ == '__main__':
    success = True
    
    success &= test_basic_save_functionality()
    success &= test_overwrite_behavior()
    success &= test_metadata_tracking()
    success &= test_multiple_stages()
    success &= test_error_handling()
    success &= test_stage_limit_enforcement()
    success &= test_data_isolation()
    
    if success:
        print("\n✓ All save stage processor tests passed!")
    else:
        print("\n✗ Some save stage processor tests failed!")
    
    # Show processor info
    processor = SaveStageProcessor({
        'processor_type': 'save_stage',
        'stage_name': 'test'
    })
    print(f"\nProcessor capabilities: {list(processor.get_capabilities().keys())}")
