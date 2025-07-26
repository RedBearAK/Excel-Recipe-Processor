"""
Comprehensive tests for the refactored LookupDataProcessor with StageManager, 
FileReader, and variable substitution integration.

Tests both existing functionality (regression) and new enhanced capabilities.
"""

import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_main_data():
    """Create sample main DataFrame for testing."""
    return pd.DataFrame({
        'Order_ID': ['O001', 'O002', 'O003', 'O004', 'O005'],
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C001'],
        'Product_Code': ['P001', 'P002', 'P003', 'P001', 'P004'],
        'Quantity': [10, 5, 8, 15, 3],
        'Order_Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })


def create_customer_lookup_data():
    """Create sample customer lookup DataFrame."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'Customer_Name': ['Alice Corp', 'Bob Industries', 'Charlie Ltd', 'Delta Systems', 'Eve Enterprises'],
        'Customer_Tier': ['Gold', 'Silver', 'Gold', 'Bronze', 'Platinum'],
        'Region': ['West', 'East', 'West', 'South', 'North'],
        'Credit_Limit': [50000, 25000, 45000, 15000, 75000]
    })


def create_product_lookup_data():
    """Create sample product lookup DataFrame."""
    return pd.DataFrame({
        'Product_Code': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Product_Name': ['Widget A', 'Gadget B', 'Tool C', 'Device D', 'Component E'],
        'Category': ['Electronics', 'Hardware', 'Tools', 'Electronics', 'Components'],
        'Unit_Price': [25.50, 15.75, 32.00, 18.25, 8.50],
        'Supplier_ID': ['S001', 'S002', 'S001', 'S003', 'S002']
    })


def create_territory_lookup_data():
    """Create sample territory lookup DataFrame."""
    return pd.DataFrame({
        'Region': ['West', 'East', 'South', 'North'],
        'Territory_Manager': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince'],
        'Sales_Target': [100000, 80000, 60000, 90000],
        'Tax_Rate': [0.08, 0.06, 0.07, 0.05]
    })


def setup_test_stages():
    """Set up test stages for stage-based lookup tests."""
    StageManager.initialize_stages(max_stages=15)
    
    # Create customer lookup stage
    customer_data = create_customer_lookup_data()
    StageManager.save_stage(
        stage_name='Customer Master',
        data=customer_data,
        description='Customer master data for lookups'
    )
    
    # Create product lookup stage
    product_data = create_product_lookup_data()
    StageManager.save_stage(
        stage_name='Product Catalog',
        data=product_data,
        description='Product catalog for lookups'
    )
    
    # Create territory lookup stage
    territory_data = create_territory_lookup_data()
    StageManager.save_stage(
        stage_name='Territory Data',
        data=territory_data,
        description='Territory and sales data'
    )


def setup_test_files(temp_dir):
    """Create test files for file-based lookup tests."""
    
    # Create customer lookup file
    customer_data = create_customer_lookup_data()
    customer_file = Path(temp_dir) / "customers.xlsx"
    customer_data.to_excel(customer_file, index=False, engine='openpyxl')
    
    # Create product lookup file with variable in name
    product_data = create_product_lookup_data()
    date_str = datetime.now().strftime('%Y%m%d')
    product_file = Path(temp_dir) / f"products_{date_str}.csv"
    product_data.to_csv(product_file, index=False)
    
    # Create territory lookup file
    territory_data = create_territory_lookup_data()
    territory_file = Path(temp_dir) / "territories.tsv"
    territory_data.to_csv(territory_file, sep='\t', index=False)
    
    return {
        'customer_file': str(customer_file),
        'product_file': str(product_file),
        'product_template': str(Path(temp_dir) / "products_{date}.csv"),
        'territory_file': str(territory_file),
        'temp_dir': temp_dir
    }


def test_basic_inline_lookup():
    """Test basic lookup with inline data (regression test)."""
    print("\nTesting basic inline lookup...")
    
    main_df = create_main_data()
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Basic customer lookup',
        'lookup_source': {
            'type': 'inline',
            'data': {
                'Customer_ID': ['C001', 'C002', 'C003'],
                'Customer_Name': ['Alice Corp', 'Bob Industries', 'Charlie Ltd'],
                'Customer_Tier': ['Gold', 'Silver', 'Gold']
            }
        },
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Customer_Tier']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check that lookup worked
    if (len(result) == len(main_df) and 
        'Customer_Name' in result.columns and
        'Customer_Tier' in result.columns and
        result.iloc[0]['Customer_Name'] == 'Alice Corp'):
        print("✓ Basic inline lookup works correctly")
        return True
    else:
        print("✗ Basic inline lookup failed")
        return False


def test_dataframe_lookup():
    """Test lookup with direct DataFrame source (regression test)."""
    print("\nTesting DataFrame lookup...")
    
    main_df = create_main_data()
    lookup_df = create_customer_lookup_data()
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'DataFrame customer lookup',
        'lookup_source': lookup_df,
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Region']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check results
    if (len(result) == len(main_df) and
        'Customer_Name' in result.columns and
        'Region' in result.columns and
        result.iloc[0]['Customer_Name'] == 'Alice Corp' and
        result.iloc[1]['Customer_Name'] == 'Bob Industries'):
        print("✓ DataFrame lookup works correctly")
        return True
    else:
        print("✗ DataFrame lookup failed")
        return False


def test_multiple_column_lookup():
    """Test lookup with multiple columns (regression test)."""
    print("\nTesting multiple column lookup...")
    
    main_df = create_main_data()
    product_df = create_product_lookup_data()
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Product details lookup',
        'lookup_source': product_df,
        'lookup_key': 'Product_Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Product_Name', 'Category', 'Unit_Price']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check that all lookup columns were added
    expected_columns = ['Product_Name', 'Category', 'Unit_Price']
    has_all_columns = all(col in result.columns for col in expected_columns)
    
    # Check specific values
    first_product_name = result.iloc[0]['Product_Name']
    first_price = result.iloc[0]['Unit_Price']
    
    if (has_all_columns and 
        first_product_name == 'Widget A' and
        first_price == 25.50):
        print("✓ Multiple column lookup works correctly")
        return True
    else:
        print("✗ Multiple column lookup failed")
        return False


def test_join_types():
    """Test different join types (regression test)."""
    print("\nTesting different join types...")
    
    # Create data with some missing lookups
    main_df = pd.DataFrame({
        'ID': [1, 2, 3, 4],
        'Code': ['A', 'B', 'C', 'X']  # X won't be found
    })
    
    lookup_df = pd.DataFrame({
        'Code': ['A', 'B', 'C'],
        'Name': ['Alpha', 'Beta', 'Gamma']
    })
    
    # Test left join (default)
    step_config_left = {
        'processor_type': 'lookup_data',
        'step_description': 'Left join test',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'join_type': 'left'
    }
    
    processor_left = LookupDataProcessor(step_config_left)
    result_left = processor_left.execute(main_df)
    
    # Test inner join
    step_config_inner = {
        'processor_type': 'lookup_data',
        'step_description': 'Inner join test',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'join_type': 'inner'
    }
    
    processor_inner = LookupDataProcessor(step_config_inner)
    result_inner = processor_inner.execute(main_df)
    
    # Left join should keep all 4 rows, inner join should have 3
    if len(result_left) == 4 and len(result_inner) == 3:
        print("✓ Join types work correctly")
        return True
    else:
        print(f"✗ Join types failed: left={len(result_left)}, inner={len(result_inner)}")
        return False


def test_file_based_lookup():
    """Test file-based lookup with FileReader integration."""
    print("\nTesting file-based lookup...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        main_df = create_main_data()
        
        step_config = {
            'processor_type': 'lookup_data',
            'step_description': 'File-based customer lookup',
            'lookup_source': {
                'type': 'file',
                'filename': test_files['customer_file']
            },
            'lookup_key': 'Customer_ID',
            'source_key': 'Customer_ID',
            'lookup_columns': ['Customer_Name', 'Customer_Tier']
        }
        
        processor = LookupDataProcessor(step_config)
        result = processor.execute(main_df)
        
        # Check that file lookup worked
        if (len(result) == len(main_df) and
            'Customer_Name' in result.columns and
            result.iloc[0]['Customer_Name'] == 'Alice Corp'):
            print("✓ File-based lookup works correctly")
            return True
        else:
            print("✗ File-based lookup failed")
            return False


def test_variable_substitution_lookup():
    """Test file lookup with variable substitution."""
    print("\nTesting variable substitution in file lookup...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        main_df = create_main_data()
        
        step_config = {
            'processor_type': 'lookup_data',
            'step_description': 'Variable substitution lookup',
            'lookup_source': {
                'type': 'file',
                'filename': test_files['product_template']  # Contains {date}
            },
            'lookup_key': 'Product_Code',
            'source_key': 'Product_Code',
            'lookup_columns': ['Product_Name', 'Category']
        }
        
        processor = LookupDataProcessor(step_config)
        result = processor.execute(main_df)
        
        # Check that variable substitution worked
        if (len(result) == len(main_df) and
            'Product_Name' in result.columns and
            result.iloc[0]['Product_Name'] == 'Widget A'):
            print("✓ Variable substitution lookup works correctly")
            return True
        else:
            print("✗ Variable substitution lookup failed")
            return False


def test_stage_based_lookup():
    """Test stage-based lookup with StageManager integration."""
    print("\nTesting stage-based lookup...")
    
    main_df = create_main_data()
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Stage-based customer lookup',
        'lookup_source': {
            'type': 'stage',
            'stage_name': 'Customer Master'
        },
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Customer_Tier', 'Region']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check that stage lookup worked
    if (len(result) == len(main_df) and
        'Customer_Name' in result.columns and
        'Customer_Tier' in result.columns and
        'Region' in result.columns and
        result.iloc[0]['Customer_Name'] == 'Alice Corp'):
        print("✓ Stage-based lookup works correctly")
        return True
    else:
        print("✗ Stage-based lookup failed")
        return False


def test_chained_stage_lookups():
    """Test chained lookups across multiple stages."""
    print("\nTesting chained stage lookups...")
    
    main_df = create_main_data()
    
    # First lookup: Get customer info
    step_config_1 = {
        'processor_type': 'lookup_data',
        'step_description': 'First lookup - customer info',
        'lookup_source': {
            'type': 'stage',
            'stage_name': 'Customer Master'
        },
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Region']
    }
    
    processor_1 = LookupDataProcessor(step_config_1)
    result_1 = processor_1.execute(main_df)
    
    # Second lookup: Get territory info based on region
    step_config_2 = {
        'processor_type': 'lookup_data',
        'step_description': 'Second lookup - territory info',
        'lookup_source': {
            'type': 'stage',
            'stage_name': 'Territory Data'
        },
        'lookup_key': 'Region',
        'source_key': 'Region',
        'lookup_columns': ['Territory_Manager', 'Sales_Target']
    }
    
    processor_2 = LookupDataProcessor(step_config_2)
    result_2 = processor_2.execute(result_1)
    
    # Check that chained lookups worked
    expected_columns = ['Customer_Name', 'Region', 'Territory_Manager', 'Sales_Target']
    has_all_columns = all(col in result_2.columns for col in expected_columns)
    
    first_manager = result_2.iloc[0]['Territory_Manager']
    
    if has_all_columns and first_manager == 'Alice Johnson':
        print("✓ Chained stage lookups work correctly")
        return True
    else:
        print("✗ Chained stage lookups failed")
        return False


def test_case_insensitive_lookup():
    """Test case insensitive lookup matching."""
    print("\nTesting case insensitive lookup...")
    
    # Create data with mixed case
    main_df = pd.DataFrame({
        'ID': [1, 2, 3],
        'Code': ['abc', 'DEF', 'GhI']
    })
    
    lookup_df = pd.DataFrame({
        'Code': ['ABC', 'def', 'ghi'],
        'Name': ['Alpha', 'Delta', 'Gamma']
    })
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Case insensitive lookup',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'case_sensitive': False
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check that case insensitive matching worked
    names = result['Name'].tolist()
    expected_names = ['Alpha', 'Delta', 'Gamma']
    
    if names == expected_names:
        print("✓ Case insensitive lookup works correctly")
        return True
    else:
        print(f"✗ Case insensitive lookup failed: got {names}")
        return False


def test_prefix_suffix_naming():
    """Test prefix and suffix application to lookup columns."""
    print("\nTesting prefix/suffix naming...")
    
    main_df = create_main_data()
    customer_df = create_customer_lookup_data()
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Prefix/suffix test',
        'lookup_source': customer_df,
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Customer_Tier'],
        'prefix': 'Cust_',
        'suffix': '_Info'
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check that prefix/suffix were applied
    expected_columns = ['Cust_Customer_Name_Info', 'Cust_Customer_Tier_Info']
    has_renamed_columns = all(col in result.columns for col in expected_columns)
    
    if has_renamed_columns:
        print("✓ Prefix/suffix naming works correctly")
        return True
    else:
        print("✗ Prefix/suffix naming failed")
        return False


def test_default_values():
    """Test default value handling for missing lookups."""
    print("\nTesting default values...")
    
    # Create data with some missing lookups
    main_df = pd.DataFrame({
        'ID': [1, 2, 3],
        'Code': ['A', 'B', 'X']  # X won't be found
    })
    
    lookup_df = pd.DataFrame({
        'Code': ['A', 'B'],
        'Name': ['Alpha', 'Beta']
    })
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Default values test',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'default_value': 'Unknown'
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    # Check that default value was applied
    names = result['Name'].tolist()
    expected_names = ['Alpha', 'Beta', 'Unknown']
    
    if names == expected_names:
        print("✓ Default values work correctly")
        return True
    else:
        print(f"✗ Default values failed: got {names}")
        return False


def test_duplicate_handling():
    """Test duplicate key handling in lookup data."""
    print("\nTesting duplicate handling...")
    
    main_df = pd.DataFrame({
        'ID': [1, 2],
        'Code': ['A', 'B']
    })
    
    # Create lookup data with duplicates
    lookup_df = pd.DataFrame({
        'Code': ['A', 'A', 'B'],  # A appears twice
        'Name': ['Alpha1', 'Alpha2', 'Beta'],
        'Version': [1, 2, 1]
    })
    
    # Test 'first' handling
    step_config_first = {
        'processor_type': 'lookup_data',
        'step_description': 'Duplicate handling - first',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'handle_duplicates': 'first'
    }
    
    processor_first = LookupDataProcessor(step_config_first)
    result_first = processor_first.execute(main_df)
    
    # Test 'last' handling
    step_config_last = {
        'processor_type': 'lookup_data',
        'step_description': 'Duplicate handling - last',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'handle_duplicates': 'last'
    }
    
    processor_last = LookupDataProcessor(step_config_last)
    result_last = processor_last.execute(main_df)
    
    first_name = result_first.iloc[0]['Name']
    last_name = result_last.iloc[0]['Name']
    
    if first_name == 'Alpha1' and last_name == 'Alpha2':
        print("✓ Duplicate handling works correctly")
        return True
    else:
        print(f"✗ Duplicate handling failed: first={first_name}, last={last_name}")
        return False


def test_lookup_error_handling():
    """Test error handling for various failure scenarios."""
    print("\nTesting lookup error handling...")
    
    main_df = create_main_data()
    
    # Test missing lookup_source
    try:
        step_config = {
            'processor_type': 'lookup_data',
            'lookup_key': 'Customer_ID',
            'source_key': 'Customer_ID',
            'lookup_columns': ['Customer_Name']
            # Missing lookup_source
        }
        processor = LookupDataProcessor(step_config)
        processor.execute(main_df)
        print("✗ Should have failed with missing lookup_source")
        return False
    except StepProcessorError as e:
        if "lookup_source" in str(e):
            print("✓ Caught expected error for missing lookup_source")
        else:
            print(f"✗ Wrong error message: {e}")
            return False
    
    # Test nonexistent stage
    try:
        step_config = {
            'processor_type': 'lookup_data',
            'lookup_source': {
                'type': 'stage',
                'stage_name': 'Nonexistent Stage'
            },
            'lookup_key': 'Customer_ID',
            'source_key': 'Customer_ID',
            'lookup_columns': ['Customer_Name']
        }
        processor = LookupDataProcessor(step_config)
        processor.execute(main_df)
        print("✗ Should have failed with nonexistent stage")
        return False
    except StepProcessorError as e:
        if "not found" in str(e):
            print("✓ Caught expected error for nonexistent stage")
        else:
            print(f"✗ Wrong error message: {e}")
            return False
    
    # Test invalid source key
    try:
        step_config = {
            'processor_type': 'lookup_data',
            'lookup_source': create_customer_lookup_data(),
            'lookup_key': 'Customer_ID',
            'source_key': 'Invalid_Column',
            'lookup_columns': ['Customer_Name']
        }
        processor = LookupDataProcessor(step_config)
        processor.execute(main_df)
        print("✗ Should have failed with invalid source key")
        return False
    except StepProcessorError as e:
        if "not found" in str(e):
            print("✓ Caught expected error for invalid source key")
        else:
            print(f"✗ Wrong error message: {e}")
            return False
    
    print("✓ Error handling works correctly")
    return True


def test_capabilities_and_configuration():
    """Test processor capabilities and configuration methods."""
    print("\nTesting capabilities and configuration...")
    
    processor = LookupDataProcessor({
        'processor_type': 'lookup_data',
        'lookup_source': {},
        'lookup_key': 'key',
        'source_key': 'key',
        'lookup_columns': ['col']
    })
    
    # Test supported methods exist
    join_types = processor.get_supported_join_types()
    duplicate_handling = processor.get_supported_duplicate_handling()
    source_types = processor.get_supported_source_types()
    capabilities = processor.get_capabilities()
    
    # Check expected values
    expected_joins = ['left', 'right', 'inner', 'outer']
    expected_duplicates = ['first', 'last', 'error']
    expected_sources = ['file', 'stage', 'inline', 'dataframe']
    
    joins_correct = all(join in join_types for join in expected_joins)
    duplicates_correct = all(dup in duplicate_handling for dup in expected_duplicates)
    sources_correct = all(src in source_types for src in expected_sources)
    
    has_capabilities = (
        'description' in capabilities and
        'source_types' in capabilities and
        'stage_integration' in capabilities and
        'file_features' in capabilities
    )
    
    if joins_correct and duplicates_correct and sources_correct and has_capabilities:
        print("✓ Capabilities and configuration work correctly")
        return True
    else:
        print("✗ Capabilities and configuration failed")
        return False


def test_real_world_scenario():
    """Test a complete real-world lookup scenario."""
    print("\nTesting real-world scenario...")
    
    # Simulate order enrichment workflow
    orders_df = pd.DataFrame({
        'Order_ID': ['O001', 'O002', 'O003'],
        'Customer_ID': ['C001', 'C002', 'C003'],
        'Product_Code': ['P001', 'P002', 'P003'],
        'Quantity': [10, 5, 8]
    })
    
    # Step 1: Lookup customer information
    step_config_1 = {
        'processor_type': 'lookup_data',
        'step_description': 'Enrich with customer data',
        'lookup_source': {
            'type': 'stage',
            'stage_name': 'Customer Master'
        },
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Customer_Tier', 'Region'],
        'prefix': 'Cust_'
    }
    
    processor_1 = LookupDataProcessor(step_config_1)
    result_1 = processor_1.execute(orders_df)
    
    # Step 2: Lookup product information
    step_config_2 = {
        'processor_type': 'lookup_data',
        'step_description': 'Enrich with product data',
        'lookup_source': {
            'type': 'stage',
            'stage_name': 'Product Catalog'
        },
        'lookup_key': 'Product_Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Product_Name', 'Category', 'Unit_Price'],
        'prefix': 'Prod_'
    }
    
    processor_2 = LookupDataProcessor(step_config_2)
    result_2 = processor_2.execute(result_1)
    
    # Step 3: Lookup territory information
    step_config_3 = {
        'processor_type': 'lookup_data',
        'step_description': 'Enrich with territory data',
        'lookup_source': {
            'type': 'stage',
            'stage_name': 'Territory Data'
        },
        'lookup_key': 'Region',
        'source_key': 'Cust_Region',
        'lookup_columns': ['Territory_Manager', 'Sales_Target'],
        'prefix': 'Territory_'
    }
    
    processor_3 = LookupDataProcessor(step_config_3)
    final_result = processor_3.execute(result_2)
    
    # Verify the complete enrichment
    expected_columns = [
        'Order_ID', 'Customer_ID', 'Product_Code', 'Quantity',
        'Cust_Customer_Name', 'Cust_Customer_Tier', 'Cust_Region',
        'Prod_Product_Name', 'Prod_Category', 'Prod_Unit_Price',
        'Territory_Territory_Manager', 'Territory_Sales_Target'
    ]
    
    has_all_columns = all(col in final_result.columns for col in expected_columns)
    
    # Check some specific values
    first_customer = final_result.iloc[0]['Cust_Customer_Name']
    first_product = final_result.iloc[0]['Prod_Product_Name'] 
    first_manager = final_result.iloc[0]['Territory_Territory_Manager']
    
    if (has_all_columns and
        first_customer == 'Alice Corp' and
        first_product == 'Widget A' and
        first_manager == 'Alice Johnson'):
        print("✓ Real-world scenario works correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


if __name__ == '__main__':
    print("Testing LookupDataProcessor refactoring...")
    success = True
    
    # Basic regression tests
    print("\n=== Testing Basic Functionality (Regression) ===")
    success &= test_basic_inline_lookup()
    success &= test_dataframe_lookup()
    success &= test_multiple_column_lookup()
    success &= test_join_types()
    
    # File-based lookup tests
    print("\n=== Testing File-Based Lookups (FileReader Integration) ===")
    success &= test_file_based_lookup()
    success &= test_variable_substitution_lookup()
    
    # Stage-based lookup tests
    print("\n=== Testing Stage-Based Lookups (StageManager Integration) ===")
    setup_test_stages()
    try:
        success &= test_stage_based_lookup()
        success &= test_chained_stage_lookups()
        success &= test_real_world_scenario()
    finally:
        StageManager.cleanup_stages()
    
    # Advanced feature tests
    print("\n=== Testing Advanced Features ===")
    success &= test_case_insensitive_lookup()
    success &= test_prefix_suffix_naming()
    success &= test_default_values()
    success &= test_duplicate_handling()
    
    # Error handling and capabilities tests
    print("\n=== Testing Error Handling and Capabilities ===")
    success &= test_lookup_error_handling()
    success &= test_capabilities_and_configuration()
    
    if success:
        print("\n🎉 All LookupDataProcessor refactoring tests passed!")
    else:
        print("\n❌ Some LookupDataProcessor refactoring tests failed!")
    
    # Show processor capabilities
    processor = LookupDataProcessor({
        'processor_type': 'lookup_data',
        'lookup_source': {},
        'lookup_key': 'key',
        'source_key': 'key',
        'lookup_columns': ['col']
    })
    
    print(f"\nSupported join types: {processor.get_supported_join_types()}")
    print(f"Supported duplicate handling: {processor.get_supported_duplicate_handling()}")
    print(f"Supported source types: {processor.get_supported_source_types()}")
    
    print("\nTo run with pytest: pytest test_lookup_data_processor_refactored.py -v")
