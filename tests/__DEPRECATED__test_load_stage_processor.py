"""
Test the LoadStageProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.load_stage_processor import LoadStageProcessor


def create_sample_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003'],
        'Product_Code': ['P001', 'P002', 'P003'],
        'Order_Value': [100, 200, 150]
    })


def create_stage_data():
    """Create different DataFrame for stage testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004'],
        'Customer_Name': ['Alice Corp', 'Bob Industries', 'Charlie LLC', 'Delta Inc'],
        'Region': ['West', 'East', 'North', 'South'],
        'Tier': ['Premium', 'Standard', 'Premium', 'Basic']
    })


def test_basic_load_functionality():
    """Test basic stage loading functionality."""
    
    print("Testing basic load functionality...")
    
    StageManager.initialize_stages(max_stages=25)
    
    try:
        test_df = create_sample_data()
        stage_df = create_stage_data()
        
        # First save a stage
        StageManager.save_stage(
            stage_name='Customer Data',
            data=stage_df,
            description='Customer master data'
        )
        
        step_config = {
            'processor_type': 'load_stage',
            'step_description': 'Load customer data',
            'stage_name': 'Customer Data',
            'confirm_replace': True
        }
        
        processor = LoadStageProcessor(step_config)
        result = processor.execute(test_df)
        
        # Check that result matches stage data, not input data
        if result.equals(stage_df):
            print("✓ Basic load functionality worked correctly")
            return True
        else:
            print("✗ Loaded data does not match stage data")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_confirm_replace_safety():
    """Test confirm_replace safety mechanism."""
    
    print("\nTesting confirm_replace safety...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        stage_df = create_stage_data()
        
        # Save a stage
        StageManager.save_stage(
            stage_name='Safety Test Stage',
            data=stage_df,
            description='Test data for safety'
        )
        
        # Try to load without confirm_replace (should fail)
        step_config = {
            'processor_type': 'load_stage',
            'stage_name': 'Safety Test Stage'
            # Missing confirm_replace
        }
        
        try:
            processor = LoadStageProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed without confirm_replace=true")
            return False
        except StepProcessorError as e:
            print(f"✓ Correctly enforced confirm_replace safety: {e}")
        
        # Try with confirm_replace=false (should also fail)
        step_config['confirm_replace'] = False
        
        try:
            processor = LoadStageProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed with confirm_replace=false")
            return False
        except StepProcessorError as e:
            print(f"✓ Correctly rejected confirm_replace=false: {e}")
        
        # Now with confirm_replace=true (should work)
        step_config['confirm_replace'] = True
        
        processor = LoadStageProcessor(step_config)
        result = processor.execute(test_df)
        
        if result.equals(stage_df):
            print("✓ confirm_replace safety mechanism worked correctly")
            return True
        else:
            print("✗ Load failed even with confirm_replace=true")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_usage_tracking():
    """Test that stage usage is properly tracked."""
    
    print("\nTesting usage tracking...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        stage_df = create_stage_data()
        
        # Save a stage
        StageManager.save_stage(
            stage_name='Usage Test Stage',
            data=stage_df,
            description='Test data for usage tracking'
        )
        
        # Check initial usage (should be 0)
        stage_info = StageManager.list_stages()
        initial_usage = stage_info['Usage Test Stage']['usage_count']
        
        if initial_usage != 0:
            print(f"✗ Initial usage should be 0, got {initial_usage}")
            return False
        
        # Load the stage
        step_config = {
            'processor_type': 'load_stage',
            'stage_name': 'Usage Test Stage',
            'confirm_replace': True
        }
        
        processor = LoadStageProcessor(step_config)
        processor.execute(test_df)
        
        # Check usage was incremented
        stage_info = StageManager.list_stages()
        after_usage = stage_info['Usage Test Stage']['usage_count']
        
        if after_usage == 1:
            print("✓ Usage tracking worked correctly")
            return True
        else:
            print(f"✗ Usage should be 1, got {after_usage}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_multiple_loads():
    """Test loading the same stage multiple times."""
    
    print("\nTesting multiple loads...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        stage_df = create_stage_data()
        
        # Save a stage
        StageManager.save_stage(
            stage_name='Multi Load Test',
            data=stage_df,
            description='Test data for multiple loads'
        )
        
        step_config = {
            'processor_type': 'load_stage',
            'stage_name': 'Multi Load Test',
            'confirm_replace': True
        }
        
        processor = LoadStageProcessor(step_config)
        
        # Load three times
        result1 = processor.execute(test_df)
        result2 = processor.execute(test_df)
        result3 = processor.execute(test_df)
        
        # Check usage count
        stage_info = StageManager.list_stages()
        usage_count = stage_info['Multi Load Test']['usage_count']
        
        # All results should be identical
        if (result1.equals(stage_df) and 
            result2.equals(stage_df) and 
            result3.equals(stage_df) and
            usage_count == 3):
            print("✓ Multiple loads worked correctly")
            return True
        else:
            print(f"✗ Multiple loads failed - usage count: {usage_count}")
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
                'processor_type': 'load_stage',
                'step_description': 'Missing stage name',
                'confirm_replace': True
            }
            processor = LoadStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with missing stage_name")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for missing stage_name: {e}")
        
        # Test nonexistent stage
        try:
            bad_config = {
                'processor_type': 'load_stage',
                'stage_name': 'Nonexistent Stage',
                'confirm_replace': True
            }
            processor = LoadStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with nonexistent stage")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for nonexistent stage: {e}")
        
        # Test invalid stage_name type
        try:
            bad_config = {
                'processor_type': 'load_stage',
                'stage_name': 123,  # Should be string
                'confirm_replace': True
            }
            processor = LoadStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with invalid stage_name type")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for invalid stage_name type: {e}")
        
        print("✓ Error handling worked correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_data_isolation():
    """Test that loaded data is properly isolated (copies)."""
    
    print("\nTesting data isolation...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        stage_df = create_stage_data()
        
        # Save a stage
        StageManager.save_stage(
            stage_name='Isolation Test',
            data=stage_df,
            description='Test data for isolation'
        )
        
        # Load the stage
        step_config = {
            'processor_type': 'load_stage',
            'stage_name': 'Isolation Test',
            'confirm_replace': True
        }
        
        processor = LoadStageProcessor(step_config)
        result = processor.execute(test_df)
        
        # Modify the result
        result.loc[0, 'Customer_Name'] = 'MODIFIED'
        
        # Check that stage data was not affected
        stage_data = StageManager.load_stage('Isolation Test')
        
        if stage_data.loc[0, 'Customer_Name'] != 'MODIFIED':
            print("✓ Data isolation worked correctly")
            return True
        else:
            print("✗ Data isolation failed - stage data was modified")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_workflow_scenario():
    """Test a realistic workflow with save and load."""
    
    print("\nTesting workflow scenario...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        # Step 1: Start with orders data
        orders_df = pd.DataFrame({
            'Order_ID': [1001, 1002, 1003],
            'Customer_ID': ['C001', 'C002', 'C003'],
            'Amount': [100, 200, 150]
        })
        
        # Step 2: Save as backup
        StageManager.save_stage(
            stage_name='Original Orders',
            data=orders_df,
            description='Original order data before processing'
        )
        
        # Step 3: Process orders (simulate some transformation)
        processed_df = orders_df.copy()
        processed_df['Amount'] = processed_df['Amount'] * 1.1  # Add 10%
        processed_df['Processed'] = True
        
        # Step 4: Save processed data
        StageManager.save_stage(
            stage_name='Processed Orders',
            data=processed_df,
            description='Orders with 10% markup added'
        )
        
        # Step 5: Later, load original data for comparison
        step_config = {
            'processor_type': 'load_stage',
            'stage_name': 'Original Orders',
            'confirm_replace': True
        }
        
        processor = LoadStageProcessor(step_config)
        loaded_original = processor.execute(processed_df)
        
        # Verify we got back the original data
        if (loaded_original.equals(orders_df) and
            len(loaded_original.columns) == 3 and  # No 'Processed' column
            loaded_original.loc[0, 'Amount'] == 100):  # Original amount, not 110
            print("✓ Workflow scenario worked correctly")
            return True
        else:
            print("✗ Workflow scenario failed")
            return False
            
    finally:
        StageManager.cleanup_stages()


if __name__ == '__main__':
    success = True
    
    success &= test_basic_load_functionality()
    success &= test_confirm_replace_safety()
    success &= test_usage_tracking()
    success &= test_multiple_loads()
    success &= test_error_handling()
    success &= test_data_isolation()
    success &= test_workflow_scenario()
    
    if success:
        print("\n✓ All load stage processor tests passed!")
    else:
        print("\n✗ Some load stage processor tests failed!")
    
    # Show processor info
    processor = LoadStageProcessor({
        'processor_type': 'load_stage',
        'stage_name': 'test',
        'confirm_replace': True
    })
    print(f"\nProcessor capabilities: {list(processor.get_capabilities().keys())}")
