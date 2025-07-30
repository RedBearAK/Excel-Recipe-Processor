"""
Comprehensive tests for the refactored AggregateDataProcessor with StageManager, 
FileReader, and variable substitution integration.

Tests both existing functionality (regression) and new enhanced capabilities.
"""

import json
import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.aggregate_data_processor import AggregateDataProcessor


def create_sales_test_data():
    """Create sample sales data for testing."""
    return pd.DataFrame({
        'Region': ['North', 'South', 'North', 'West', 'South', 'West', 'North', 'East'],
        'Department': ['Electronics', 'Clothing', 'Electronics', 'Home', 'Clothing', 'Electronics', 'Home', 'Electronics'],
        'Sales_Amount': [1500.0, 800.0, 2200.0, 650.0, 1200.0, 1800.0, 950.0, 1400.0],
        'Order_Count': [15, 8, 22, 6, 12, 18, 9, 14],
        'Quarter': ['Q1', 'Q1', 'Q2', 'Q1', 'Q2', 'Q2', 'Q1', 'Q2'],
        'Year': [2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024]
    })


def create_customer_data():
    """Create sample customer data for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C005', 'C006'],
        'Customer_Type': ['Premium', 'Standard', 'Premium', 'Basic', 'Standard', 'Premium'],
        'Annual_Revenue': [50000, 25000, 75000, 15000, 30000, 60000],
        'Order_Frequency': [12, 6, 18, 4, 8, 15],
        'Territory': ['North', 'South', 'North', 'West', 'South', 'West']
    })


def create_aggregation_config_data():
    """Create sample aggregation configuration data."""
    return pd.DataFrame({
        'config_id': ['sales_summary', 'customer_analysis', 'regional_breakdown'],
        'group_by': ['Region', 'Customer_Type', 'Territory,Quarter'],
        'aggregations': [
            json.dumps([
                {'column': 'Sales_Amount', 'function': 'sum', 'output_name': 'Total_Sales'},
                {'column': 'Order_Count', 'function': 'mean', 'output_name': 'Avg_Orders'}
            ]),
            json.dumps([
                {'column': 'Annual_Revenue', 'function': 'sum', 'output_name': 'Total_Revenue'},
                {'column': 'Order_Frequency', 'function': 'mean', 'output_name': 'Avg_Frequency'}
            ]),
            'Sales_Amount:sum:Regional_Total,Order_Count:count:Total_Orders'
        ],
        'description': ['Sales summary by region', 'Customer analysis by type', 'Regional breakdown by quarter'],
        'active': [True, True, True]
    })


def setup_test_stages():
    """Set up test stages for stage-based aggregation tests."""
    
    StageManager.initialize_stages(max_stages=10)
    
    # Create aggregation config stage
    config_data = create_aggregation_config_data()
    StageManager.save_stage(
        'Aggregation Configs',
        config_data,
        description='Test aggregation configurations'
    )
    
    # Create lookup stage for cross-reference testing
    lookup_data = pd.DataFrame({
        'region_code': ['N', 'S', 'E', 'W'],
        'region_name': ['North', 'South', 'East', 'West'],
        'agg_config': ['sales_summary', 'sales_summary', 'customer_analysis', 'sales_summary'],
        'group_by_col': ['Region', 'Region', 'Customer_Type', 'Region'],
        'agg_spec': [
            'Sales_Amount:sum:Regional_Sales',
            'Sales_Amount:sum:Regional_Sales',
            'Annual_Revenue:mean:Avg_Revenue',
            'Sales_Amount:sum:Regional_Sales'
        ]
    })
    StageManager.save_stage(
        'Region Lookup',
        lookup_data,
        description='Region lookup for aggregation configs'
    )


# =============================================================================
# BASIC FUNCTIONALITY TESTS (Regression)
# =============================================================================

def test_single_column_aggregation():
    """Test basic single column aggregation."""
    
    print("\nTesting single column aggregation...")
    
    test_df = create_sales_test_data()
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Basic aggregation test',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Single column aggregation: {len(test_df)} rows → {len(result)} groups")
    
    # Check expected columns
    expected_columns = ['Region', 'Total_Sales']
    has_expected = all(col in result.columns for col in expected_columns)
    
    if has_expected and len(result) > 0:
        print("✓ Single column aggregation worked correctly")
        return True
    else:
        print("✗ Single column aggregation failed")
        return False


def test_multi_column_aggregation():
    """Test aggregation with multiple grouping columns."""
    
    print("\nTesting multi-column aggregation...")
    
    test_df = create_sales_test_data()
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Aggregate by region and department',
        'group_by': ['Region', 'Department'],
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            },
            {
                'column': 'Order_Count',
                'function': 'mean',
                'new_column_name': 'Avg_Orders'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Multi-column aggregation: {len(test_df)} rows → {len(result)} groups")
    
    # Check expected columns
    expected_cols = ['Region', 'Department', 'Total_Sales', 'Avg_Orders']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ Multi-column aggregation created correctly")
        return True
    else:
        print(f"✗ Missing expected columns: {list(result.columns)}")
        return False


def test_multiple_functions_same_column():
    """Test multiple aggregation functions on the same column."""
    
    print("\nTesting multiple functions on same column...")
    
    test_df = create_sales_test_data()
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Sales statistics by region',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            },
            {
                'column': 'Sales_Amount',
                'function': 'mean',
                'new_column_name': 'Avg_Sales'
            },
            {
                'column': 'Sales_Amount',
                'function': 'count',
                'new_column_name': 'Sales_Count'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Multiple functions: {len(result)} groups")
    
    # Check all expected columns exist
    expected_cols = ['Region', 'Total_Sales', 'Avg_Sales', 'Sales_Count']
    has_all = all(col in result.columns for col in expected_cols)
    
    if has_all:
        print("✓ All aggregation functions applied correctly")
        return True
    else:
        print(f"✗ Missing columns: {list(result.columns)}")
        return False


def test_configuration_options():
    """Test various configuration options."""
    
    print("\nTesting configuration options...")
    
    test_df = create_sales_test_data()
    
    # Test without keeping group columns
    step_config1 = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test without group columns',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ],
        'keep_group_columns': False
    }
    
    processor1 = AggregateDataProcessor(step_config1)
    result1 = processor1.execute(test_df)
    
    print(f"✓ Without group columns: {list(result1.columns)}")
    
    # Test backward compatibility with 'output_name' instead of 'new_column_name'
    step_config2 = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test output_name compatibility',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'output_name': 'Total_Sales'  # Using old naming
            }
        ]
    }
    
    processor2 = AggregateDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    if len(result1) > 0 and len(result2) > 0 and 'Total_Sales' in result2.columns:
        print("✓ Configuration options worked correctly")
        return True
    else:
        print("✗ Configuration options failed")
        return False


# =============================================================================
# STAGE-BASED AGGREGATION TESTS (New Features)
# =============================================================================

def test_save_to_stage():
    """Test saving aggregation results to a stage."""
    
    setup_test_stages()

    print("\nTesting save to stage...")
    
    test_df = create_sales_test_data()
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test stage saving',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ],
        'save_to_stage': 'Sales Summary',
        'stage_description': 'Regional sales totals'
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that stage was created
    if StageManager.stage_exists('Sales Summary'):
        stage_data = StageManager.load_stage('Sales Summary')
        
        if len(stage_data) == len(result) and 'Total_Sales' in stage_data.columns:
            print("✓ Save to stage worked correctly")
            return True
        else:
            print("✗ Stage data doesn't match result")
            return False
    else:
        print("✗ Stage was not created")
        return False


def test_stage_based_aggregation_config():
    """Test loading aggregation configuration from stage."""
    
    setup_test_stages()

    print("\nTesting stage-based aggregation config...")
    
    test_df = create_sales_test_data()
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test stage config',
        'aggregation_source': {
            'type': 'stage',
            'stage_name': 'Aggregation Configs',
            'format': 'table',
            'group_by_column': 'group_by',
            'aggregations_column': 'aggregations',
            'filter_condition': {
                'column': 'config_id',
                'value': 'sales_summary',
                'operator': 'equals'
            }
        }
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Stage-based config: {len(result)} groups")
    
    # Should have aggregated by Region with Total_Sales and Avg_Orders
    expected_cols = ['Region', 'Total_Sales', 'Avg_Orders']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ Stage-based aggregation config worked correctly")
        return True
    else:
        print(f"✗ Missing expected columns: {list(result.columns)}")
        return False


def test_lookup_based_aggregation_config():
    """Test loading aggregation configuration from lookup stage."""
    
    setup_test_stages()
    
    print("\nTesting lookup-based aggregation config...")
    
    # Add region mapping to test data
    test_df = create_sales_test_data()
    test_df['region_code'] = test_df['Region'].map({'North': 'N', 'South': 'S', 'East': 'E', 'West': 'W'})
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test lookup config',
        'aggregation_source': {
            'type': 'lookup',
            'lookup_stage': 'Region Lookup',
            'lookup_key': 'region_code',
            'data_key': 'region_code',
            'group_by_column': 'group_by_col',
            'aggregations_column': 'agg_spec'
        }
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Lookup-based config: {len(result)} groups")
    
    # Should have Regional_Sales column from the lookup config
    if 'Regional_Sales' in result.columns:
        print("✓ Lookup-based aggregation config worked correctly")
        return True
    else:
        print(f"✗ Missing expected column: {list(result.columns)}")
        return False


# =============================================================================
# FILE-BASED AGGREGATION TESTS (New Features)
# =============================================================================

def test_file_based_aggregation_config():
    """Test loading aggregation configuration from file."""
    
    print("\nTesting file-based aggregation config...")
    
    # Create temporary config file
    config_data = create_aggregation_config_data()
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as temp_file:
        config_data.to_csv(temp_file.name, index=False)
        temp_config_path = temp_file.name
    
    try:
        test_df = create_sales_test_data()
        
        step_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Test file config',
            'aggregation_source': {
                'type': 'file',
                'filename': temp_config_path,
                'format': 'table',
                'group_by_column': 'group_by',
                'aggregations_column': 'aggregations'
            }
        }
        
        processor = AggregateDataProcessor(step_config)
        result = processor.execute(test_df)
        
        print(f"✓ File-based config: {len(result)} groups")
        
        # Should use first config (sales_summary)
        expected_cols = ['Region', 'Total_Sales', 'Avg_Orders']
        has_expected = all(col in result.columns for col in expected_cols)
        
        if has_expected:
            print("✓ File-based aggregation config worked correctly")
            return True
        else:
            print(f"✗ Missing expected columns: {list(result.columns)}")
            return False
    
    finally:
        # Clean up temp file
        Path(temp_config_path).unlink()


def test_variable_substitution_aggregation():
    """Test variable substitution in aggregation configuration."""
    
    print("\nTesting variable substitution...")
    
    test_df = create_sales_test_data()
    
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test variable substitution',
        'group_by': '{GROUP_COLUMN}',
        'aggregations': [
            {
                'column': '{VALUE_COLUMN}',
                'function': 'sum',
                'new_column_name': '{OUTPUT_PREFIX}_Total'
            }
        ]
    }
    
    # Mock variables
    test_variables = {
        'GROUP_COLUMN': 'Region',
        'VALUE_COLUMN': 'Sales_Amount',
        'OUTPUT_PREFIX': 'Sales'
    }
    
    processor = AggregateDataProcessor(step_config)
    # Simulate variable injection (normally done by pipeline)
    processor._variables = test_variables
    
    result = processor.execute(test_df)
    
    print(f"✓ Variable substitution: {len(result)} groups")
    
    if 'Sales_Total' in result.columns and 'Region' in result.columns:
        print("✓ Variable substitution worked correctly")
        return True
    else:
        print(f"✗ Variable substitution failed: {list(result.columns)}")
        return False


# =============================================================================
# HELPER METHODS AND ANALYSIS TESTS
# =============================================================================

def test_summary_aggregation_helper():
    """Test the create_summary_aggregation helper method."""
    
    print("\nTesting summary aggregation helper...")
    
    test_df = create_sales_test_data()
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'step_description': 'Test helper',
        'group_by': 'Region',
        'aggregations': []
    })
    
    # Use helper to create standard summary
    result = processor.create_summary_aggregation(
        test_df, 
        'Region', 
        ['Sales_Amount', 'Order_Count']
    )
    
    print(f"✓ Summary helper: {len(result)} groups")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Check for expected summary columns
    expected_patterns = ['total', 'average', 'count', 'minimum', 'maximum']
    has_summaries = any(
        any(pattern in col.lower() for pattern in expected_patterns)
        for col in result.columns
    )
    
    if has_summaries:
        print("✓ Summary aggregation helper worked correctly")
        return True
    else:
        print("✗ Summary aggregation helper failed")
        return False


def test_analysis_method():
    """Test the analyze_aggregation_results method."""
    
    print("\nTesting analysis method...")
    
    test_df = create_sales_test_data()
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'group_by': 'Region',
        'aggregations': [
            {'column': 'Sales_Amount', 'function': 'sum', 'new_column_name': 'Total_Sales'}
        ]
    })
    
    result = processor.execute(test_df)
    analysis = processor.analyze_aggregation_results(result, ['Region'])
    
    print(f"✓ Analysis results: {analysis.keys()}")
    
    expected_keys = ['total_groups', 'group_columns', 'aggregated_columns', 'summary']
    has_expected = all(key in analysis for key in expected_keys)
    
    if has_expected and analysis['total_groups'] > 0:
        print("✓ Analysis method worked correctly")
        return True
    else:
        print("✗ Analysis method failed")
        return False


def test_capabilities_method():
    """Test the get_capabilities method."""
    
    print("\nTesting enhanced capabilities method...")
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'group_by': 'test',
        'aggregations': []
    })
    
    capabilities = processor.get_capabilities()
    
    print(f"✓ Capabilities: {list(capabilities.keys())}")
    
    # Check for new capability fields
    expected_keys = [
        'description', 'aggregation_functions', 'source_types', 
        'file_formats', 'integration_features'
    ]
    has_expected = all(key in capabilities for key in expected_keys)
    
    source_types = capabilities.get('source_types', [])
    integration_features = capabilities.get('integration_features', [])
    
    has_new_features = (
        'stage' in source_types and 
        'file' in source_types and
        'stage_manager_integration' in integration_features
    )
    
    if has_expected and has_new_features:
        print("✓ Enhanced capabilities method worked correctly")
        return True
    else:
        print("✗ Enhanced capabilities method failed")
        return False


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================

def test_aggregation_error_handling():
    """Test error handling for various failure cases."""
    
    setup_test_stages()

    print("\nTesting aggregation error handling...")
    
    test_df = create_sales_test_data()
    
    # Test missing stage
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Missing stage',
            'aggregation_source': {
                'type': 'stage',
                'stage_name': 'NonExistent Stage'
            }
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing stage")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for missing stage: {e}")
    
    # Test invalid aggregation source type
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Invalid source type',
            'aggregation_source': {
                'type': 'invalid_type'
            }
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid source type")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for invalid source type: {e}")
    
    # Test stage overwrite protection
    # First create a stage to test overwrite with
    step_config_create = {
        'processor_type': 'aggregate_data',
        'group_by': 'Region',
        'aggregations': [{'column': 'Sales_Amount', 'function': 'sum'}],
        'save_to_stage': 'Test Overwrite Stage',
        'stage_overwrite': True  # Allow initial creation
    }
    processor_create = AggregateDataProcessor(step_config_create)
    processor_create.execute(test_df)
    
    # Now try to overwrite without permission
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'group_by': 'Region',
            'aggregations': [{'column': 'Sales_Amount', 'function': 'sum'}],
            'save_to_stage': 'Test Overwrite Stage',  # This stage now exists
            'stage_overwrite': False
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed without overwrite=true")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for stage overwrite: {e}")
    
    print("✓ Error handling worked correctly")
    return True


def test_real_world_scenario():
    """Test a realistic aggregation scenario with multiple features."""
    
    setup_test_stages()

    print("\nTesting real-world scenario...")
    
    test_df = create_sales_test_data()
    
    # Step 1: Basic aggregation with stage saving
    step_config_1 = {
        'processor_type': 'aggregate_data',
        'step_description': 'Regional quarterly summary',
        'group_by': ['Region', 'Quarter'],
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Revenue'
            },
            {
                'column': 'Order_Count',
                'function': 'sum',
                'new_column_name': 'Total_Orders'
            },
            {
                'column': 'Sales_Amount',
                'function': 'mean',
                'new_column_name': 'Avg_Deal_Size'
            }
        ],
        'save_to_stage': 'Regional Quarterly Summary',
        'stage_description': 'Quarterly performance by region',
        'stage_overwrite': True
    }
    
    processor_1 = AggregateDataProcessor(step_config_1)
    result_1 = processor_1.execute(test_df)
    
    # Step 2: Department-level analysis
    step_config_2 = {
        'processor_type': 'aggregate_data',
        'step_description': 'Department analysis',
        'group_by': 'Department',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Dept_Revenue'
            },
            {
                'column': 'Sales_Amount',
                'function': 'count',
                'new_column_name': 'Dept_Transactions'
            }
        ]
    }
    
    processor_2 = AggregateDataProcessor(step_config_2)
    result_2 = processor_2.execute(test_df)
    
    # Verify results
    stage_exists = StageManager.stage_exists('Regional Quarterly Summary')
    has_expected_cols_1 = all(col in result_1.columns for col in ['Region', 'Quarter', 'Total_Revenue'])
    has_expected_cols_2 = all(col in result_2.columns for col in ['Department', 'Dept_Revenue'])
    
    # Check business logic
    total_revenue_1 = result_1['Total_Revenue'].sum()
    total_revenue_2 = result_2['Dept_Revenue'].sum()
    original_revenue = test_df['Sales_Amount'].sum()
    
    revenue_matches = (
        abs(total_revenue_1 - original_revenue) < 0.01 and
        abs(total_revenue_2 - original_revenue) < 0.01
    )
    
    if stage_exists and has_expected_cols_1 and has_expected_cols_2 and revenue_matches:
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


def test_backward_compatibility():
    """Test that all existing functionality still works."""
    
    setup_test_stages()

    print("\nTesting backward compatibility...")
    
    # Test old-style config still works
    test_df = create_sales_test_data()
    
    old_style_config = {
        'processor_type': 'aggregate_data',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ],
        'keep_group_columns': True,
        'sort_by_groups': True,
        'reset_index': True
    }
    
    processor = AggregateDataProcessor(old_style_config)
    result = processor.execute(test_df)
    
    # Test minimal config
    minimal_config = AggregateDataProcessor.get_minimal_config()
    has_required_fields = 'group_by' in minimal_config and 'aggregations' in minimal_config
    
    if len(result) > 0 and 'Total_Sales' in result.columns and has_required_fields:
        print("✓ Backward compatibility maintained")
        return True
    else:
        print("✗ Backward compatibility broken")
        return False


if __name__ == '__main__':
    print("Testing AggregateDataProcessor refactoring...")
    success = True
    
    # Basic regression tests
    print("\n=== Testing Basic Functionality (Regression) ===")
    success &= test_single_column_aggregation()
    success &= test_multi_column_aggregation()
    success &= test_multiple_functions_same_column()
    success &= test_configuration_options()
    
    # Stage-based aggregation tests
    print("\n=== Testing Stage-Based Aggregation (New Features) ===")
    setup_test_stages()
    try:
        success &= test_save_to_stage()
        success &= test_stage_based_aggregation_config()
        success &= test_lookup_based_aggregation_config()
    finally:
        StageManager.cleanup_stages()
    
    # File-based aggregation tests
    print("\n=== Testing File-Based Aggregation (New Features) ===")
    success &= test_file_based_aggregation_config()
    success &= test_variable_substitution_aggregation()
    
    # Helper methods and analysis tests
    print("\n=== Testing Helper Methods and Analysis ===")
    success &= test_summary_aggregation_helper()
    success &= test_analysis_method()
    success &= test_capabilities_method()
    
    # Error handling and compatibility tests
    print("\n=== Testing Error Handling and Compatibility ===")
    setup_test_stages()  # Need stages for error testing
    try:
        success &= test_aggregation_error_handling()
        success &= test_real_world_scenario()
        success &= test_backward_compatibility()
    finally:
        StageManager.cleanup_stages()
    
    if success:
        print("\n🎉 All AggregateDataProcessor refactoring tests passed!")
    else:
        print("\n❌ Some AggregateDataProcessor refactoring tests failed!")
    
    # Show processor capabilities
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'group_by': 'test', 
        'aggregations': []
    })
    
    print(f"\nSupported aggregation functions: {processor.get_supported_functions()}")
    print(f"Supported source types: {processor.get_supported_source_types()}")
    print(f"Supported file formats: {processor.get_supported_file_formats()}")
    print(f"Integration features: {processor.get_capabilities()['integration_features']}")
    
    print("\nTo run with pytest: pytest test_aggregate_data_processor_refactored.py -v")
