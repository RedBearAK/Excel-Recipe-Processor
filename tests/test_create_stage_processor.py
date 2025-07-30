"""
Test the CreateStageProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.create_stage_processor import CreateStageProcessor


def create_sample_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003'],
        'Product_Code': ['P001', 'P002', 'P003'],
        'Order_Value': [100, 200, 150]
    })


def test_list_format_creation():
    """Test creating stages with list format."""
    
    print("Testing list format creation...")
    
    # Initialize StageManager for testing
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        step_config = {
            'processor_type': 'create_stage',
            'step_description': 'Create approved customer list',
            'stage_name': 'Approved Customers',
            'description': 'List of customers approved for promotions',
            'data': {
                'format': 'list',
                'column': 'Customer_ID',
                'values': ['CUST001', 'CUST002', 'CUST003', 'CUST004']
            }
        }
        
        processor = CreateStageProcessor(step_config)
        result = processor.execute(test_df)
        
        # Check that input data is unchanged
        if not result.equals(test_df):
            print("✗ Input data was modified")
            return False
        
        # Check that stage was created
        if not StageManager.stage_exists('Approved Customers'):
            print("✗ Stage was not created")
            return False
        
        # Check stage data
        stage_data = StageManager.load_stage('Approved Customers')
        expected_values = ['CUST001', 'CUST002', 'CUST003', 'CUST004']
        
        if list(stage_data['Customer_ID']) == expected_values:
            print("✓ List format creation worked correctly")
            return True
        else:
            print(f"✗ Expected {expected_values}, got {list(stage_data['Customer_ID'])}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_table_format_creation():
    """Test creating stages with table format."""
    
    print("\nTesting table format creation...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        step_config = {
            'processor_type': 'create_stage',
            'step_description': 'Create customer tier mapping',
            'stage_name': 'Customer Tiers',
            'description': 'Customer tier assignments',
            'data': {
                'format': 'table',
                'columns': ['Customer_ID', 'Customer_Name', 'Tier'],
                'rows': [
                    ['C001', 'Alice Corp', 'Premium'],
                    ['C002', 'Bob Industries', 'Standard'],
                    ['C003', 'Charlie LLC', 'Premium']
                ]
            }
        }
        
        processor = CreateStageProcessor(step_config)
        result = processor.execute(test_df)
        
        # Check stage was created
        if not StageManager.stage_exists('Customer Tiers'):
            print("✗ Stage was not created")
            return False
        
        # Check stage data structure
        stage_data = StageManager.load_stage('Customer Tiers')
        expected_columns = ['Customer_ID', 'Customer_Name', 'Tier']
        
        if list(stage_data.columns) == expected_columns and len(stage_data) == 3:
            print("✓ Table format creation worked correctly")
            return True
        else:
            print(f"✗ Expected columns {expected_columns}, got {list(stage_data.columns)}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_dictionary_format_creation():
    """Test creating stages with dictionary format."""
    
    print("\nTesting dictionary format creation...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        step_config = {
            'processor_type': 'create_stage',
            'step_description': 'Create region mapping',
            'stage_name': 'Region Mapping',
            'description': 'State to region mappings',
            'data': {
                'format': 'dictionary',
                'key_column': 'State',
                'value_column': 'Region',
                'data': {
                    'CA': 'West',
                    'NY': 'East',
                    'TX': 'South',
                    'WA': 'West'
                }
            }
        }
        
        processor = CreateStageProcessor(step_config)
        result = processor.execute(test_df)
        
        # Check stage was created
        if not StageManager.stage_exists('Region Mapping'):
            print("✗ Stage was not created")
            return False
        
        # Check stage data
        stage_data = StageManager.load_stage('Region Mapping')
        expected_columns = ['State', 'Region']
        
        if (list(stage_data.columns) == expected_columns and 
            len(stage_data) == 4 and 
            'CA' in stage_data['State'].values):
            print("✓ Dictionary format creation worked correctly")
            return True
        else:
            print(f"✗ Dictionary format failed validation")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_size_limits_and_warnings():
    """Test size limit validation and warnings."""
    
    print("\nTesting size limits and warnings...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        # Test list that should trigger error (over 100 items)
        large_list = [f"ITEM{i:03d}" for i in range(101)]
        
        step_config = {
            'processor_type': 'create_stage',
            'step_description': 'Test size limits',
            'stage_name': 'Too Large List',
            'data': {
                'format': 'list',
                'column': 'Item_Code',
                'values': large_list
            }
        }
        
        try:
            processor = CreateStageProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed with size limit error")
            return False
        except StepProcessorError as e:
            if "maximum allowed is 100" in str(e):
                print("✓ Size limit validation worked correctly")
            else:
                print(f"✗ Unexpected error: {e}")
                return False
        
        # Test table that should trigger error (over 200 rows)
        large_table_rows = [[f"ROW{i}", f"VALUE{i}"] for i in range(201)]
        
        step_config = {
            'processor_type': 'create_stage',
            'step_description': 'Test table size limits',
            'stage_name': 'Too Large Table',
            'data': {
                'format': 'table',
                'columns': ['ID', 'Value'],
                'rows': large_table_rows
            }
        }
        
        try:
            processor = CreateStageProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed with table size limit error")
            return False
        except StepProcessorError as e:
            if "maximum allowed is 200" in str(e):
                print("✓ Table size limit validation worked correctly")
                return True
            else:
                print(f"✗ Unexpected error: {e}")
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
                'processor_type': 'create_stage',
                'step_description': 'Missing stage name',
                'data': {
                    'format': 'list',
                    'column': 'Test',
                    'values': ['A', 'B']
                }
            }
            processor = CreateStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with missing stage_name")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for missing stage_name: {e}")
        
        # Test invalid format
        try:
            bad_config = {
                'processor_type': 'create_stage',
                'stage_name': 'Test Stage',
                'data': {
                    'format': 'invalid_format',
                    'column': 'Test',
                    'values': ['A', 'B']
                }
            }
            processor = CreateStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with invalid format")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for invalid format: {e}")
        
        # Test reserved stage name
        try:
            bad_config = {
                'processor_type': 'create_stage',
                'stage_name': 'current',  # Reserved name
                'data': {
                    'format': 'list',
                    'column': 'Test',
                    'values': ['A', 'B']
                }
            }
            processor = CreateStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with reserved stage name")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for reserved name: {e}")
        
        # Test table with mismatched columns/rows
        try:
            bad_config = {
                'processor_type': 'create_stage',
                'stage_name': 'Bad Table',
                'data': {
                    'format': 'table',
                    'columns': ['A', 'B', 'C'],  # 3 columns
                    'rows': [
                        ['X', 'Y']  # Only 2 values
                    ]
                }
            }
            processor = CreateStageProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with mismatched columns/rows")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for mismatched table: {e}")
        
        print("✓ Error handling worked correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_overwrite_behavior():
    """Test stage overwrite behavior."""
    
    print("\nTesting overwrite behavior...")
    
    StageManager.initialize_stages(max_stages=10)
    
    try:
        test_df = create_sample_data()
        
        # Create initial stage
        step_config = {
            'processor_type': 'create_stage',
            'stage_name': 'Test Overwrite',
            'data': {
                'format': 'list',
                'column': 'Items',
                'values': ['A', 'B', 'C']
            }
        }
        
        processor = CreateStageProcessor(step_config)
        processor.execute(test_df)
        
        # Verify initial stage
        stage_data = StageManager.load_stage('Test Overwrite')
        if len(stage_data) != 3:
            print("✗ Initial stage creation failed")
            return False
        
        # Try to create again without overwrite (should fail)
        try:
            processor.execute(test_df)
            print("✗ Should have failed without overwrite=true")
            return False
        except StepProcessorError as e:
            print(f"✓ Correctly prevented overwrite: {e}")
        
        # Now with overwrite=true
        step_config['overwrite'] = True
        step_config['data']['values'] = ['X', 'Y', 'Z', 'W']  # Different data
        
        processor = CreateStageProcessor(step_config)
        processor.execute(test_df)
        
        # Verify stage was overwritten
        stage_data = StageManager.load_stage('Test Overwrite')
        if len(stage_data) == 4 and 'X' in stage_data['Items'].values:
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
            'processor_type': 'create_stage',
            'step_description': 'Test metadata tracking',
            'stage_name': 'Metadata Test Stage',
            'description': 'This is a test stage for metadata',
            'data': {
                'format': 'list',
                'column': 'Test_Items',
                'values': ['Item1', 'Item2', 'Item3']
            }
        }
        
        processor = CreateStageProcessor(step_config)
        processor.execute(test_df)
        
        # Check metadata
        stage_info = StageManager.list_stages()
        metadata = stage_info.get('Metadata Test Stage', {})
        
        expected_description = 'This is a test stage for metadata'
        expected_step_name = 'Test metadata tracking'
        
        if (metadata.get('description') == expected_description and
            metadata.get('step_name') == expected_step_name and
            metadata.get('rows') == 3 and
            metadata.get('columns') == 1):
            print("✓ Metadata tracking worked correctly")
            return True
        else:
            print(f"✗ Metadata tracking failed: {metadata}")
            return False
            
    finally:
        StageManager.cleanup_stages()


if __name__ == '__main__':
    success = True
    
    success &= test_list_format_creation()
    success &= test_table_format_creation()
    success &= test_dictionary_format_creation()
    success &= test_size_limits_and_warnings()
    success &= test_error_handling()
    success &= test_overwrite_behavior()
    success &= test_metadata_tracking()
    
    if success:
        print("\n✓ All create stage processor tests passed!")
    else:
        print("\n✗ Some create stage processor tests failed!")
    
    # Show supported features
    processor = CreateStageProcessor({
        'processor_type': 'create_stage',
        'stage_name': 'test',
        'data': {'format': 'list', 'column': 'test', 'values': []}
    })
    print(f"\nSupported formats: {processor.get_supported_formats()}")
    print(f"Size limits: {processor.get_size_limits()}")
