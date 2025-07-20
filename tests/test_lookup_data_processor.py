"""
Test the LookupDataProcessor functionality.
"""

import pandas as pd
from pathlib import Path

from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_main_data():
    """Create main dataset for testing lookups."""
    return pd.DataFrame({
        'Product_Code': ['A001', 'B002', 'C003', 'A001', 'D004', 'E005'],
        'Product_Name': ['Widget A', 'Gadget B', 'Tool C', 'Widget A', 'Device D', 'Unknown E'],
        'Quantity': [100, 50, 75, 25, 200, 30],
        'Department_Code': ['ELEC', 'TOOL', 'HARD', 'ELEC', 'ELEC', 'UNKN'],
        'Supplier_ID': ['SUP1', 'SUP2', 'SUP1', 'SUP1', 'SUP3', 'SUP4']
    })


def create_product_lookup():
    """Create product lookup table for testing."""
    return pd.DataFrame({
        'Code': ['A001', 'B002', 'C003', 'D004', 'F006'],  # Note: E005 missing, F006 extra
        'Category': ['Electronics', 'Tools', 'Hardware', 'Electronics', 'Furniture'],
        'Price': [10.50, 25.00, 15.75, 8.25, 45.00],
        'Manufacturer': ['TechCorp', 'ToolCo', 'HardwareInc', 'TechCorp', 'FurniturePlus']
    })


def create_department_lookup():
    """Create department lookup table for testing."""
    return pd.DataFrame({
        'Dept_Code': ['ELEC', 'TOOL', 'HARD', 'FURN'],
        'Department_Name': ['Electronics', 'Tools', 'Hardware', 'Furniture'],
        'Manager': ['Alice Smith', 'Bob Jones', 'Carol Brown', 'Dave Wilson']
    })


def create_supplier_lookup():
    """Create supplier lookup table for testing."""
    return pd.DataFrame({
        'Supplier_ID': ['SUP1', 'SUP2', 'SUP3'],  # Note: SUP4 missing
        'Supplier_Name': ['Alpha Supplies', 'Beta Corp', 'Gamma Industries'],
        'Country': ['USA', 'Canada', 'Mexico'],
        'Rating': ['A', 'B+', 'A-']
    })


def test_basic_vlookup_style():
    """Test basic VLOOKUP-style lookup operation."""
    
    print("Testing basic VLOOKUP-style lookup...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    print(f"✓ Created main data: {len(main_df)} rows")
    print(f"✓ Created product lookup: {len(product_lookup)} rows")
    
    # Test basic lookup - get category for each product
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Lookup product categories',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    print(f"✓ Lookup result: {len(result)} rows, {len(result.columns)} columns")
    
    # Check that Category column was added
    if 'Category' not in result.columns:
        print("✗ Category column not added")
        return False
    
    # Check specific lookups
    first_category = result.iloc[0]['Category']  # A001 should be 'Electronics'
    print(f"✓ First product category: '{first_category}'")
    
    if first_category == 'Electronics':
        print("✓ Basic VLOOKUP-style lookup worked correctly")
        return True
    else:
        print(f"✗ Expected 'Electronics', got '{first_category}'")
        return False


def test_multiple_column_lookup():
    """Test looking up multiple columns at once."""
    
    print("\nTesting multiple column lookup...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    # Test multiple column lookup
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Lookup multiple product fields',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category', 'Price', 'Manufacturer']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    print(f"✓ Multiple lookup result: {len(result)} rows, {len(result.columns)} columns")
    
    # Check that all lookup columns were added
    expected_columns = ['Category', 'Price', 'Manufacturer']
    missing_columns = [col for col in expected_columns if col not in result.columns]
    
    if not missing_columns:
        print("✓ All lookup columns added successfully")
        
        # Check specific values
        first_price = result.iloc[0]['Price']
        first_manufacturer = result.iloc[0]['Manufacturer']
        
        print(f"✓ First product: Price=${first_price}, Manufacturer='{first_manufacturer}'")
        
        if first_price == 10.50 and first_manufacturer == 'TechCorp':
            print("✓ Multiple column lookup worked correctly")
            return True
        else:
            print("✗ Lookup values incorrect")
            return False
    else:
        print(f"✗ Missing lookup columns: {missing_columns}")
        return False


def test_join_types():
    """Test different join types (left, inner, outer)."""
    
    print("\nTesting different join types...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    # Test left join (default)
    step_config_left = {
        'processor_type': 'lookup_data',
        'step_description': 'Left join test',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category'],
        'join_type': 'left'
    }
    
    processor_left = LookupDataProcessor(step_config_left)
    result_left = processor_left.execute(main_df)
    
    print(f"✓ Left join: {len(main_df)} → {len(result_left)} rows")
    
    # Test inner join
    step_config_inner = {
        'processor_type': 'lookup_data',
        'step_description': 'Inner join test',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category'],
        'join_type': 'inner'
    }
    
    processor_inner = LookupDataProcessor(step_config_inner)
    result_inner = processor_inner.execute(main_df)
    
    print(f"✓ Inner join: {len(main_df)} → {len(result_inner)} rows")
    
    # Inner join should have fewer rows (only matches)
    if len(result_inner) <= len(result_left):
        print("✓ Join types worked correctly")
        return True
    else:
        print("✗ Inner join should not have more rows than left join")
        return False


def test_default_values():
    """Test default values for non-matches."""
    
    print("\nTesting default values...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    # Test with default values for non-matches
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Lookup with defaults',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category', 'Price'],
        'default_values': {
            'Category': 'Unknown Category',
            'Price': 0.0
        }
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    print(f"✓ Default values result: {len(result)} rows")
    
    # Check for default values in non-matches
    # E005 should not match and get default values
    e005_rows = result[result['Product_Code'] == 'E005']
    if len(e005_rows) > 0:
        e005_category = e005_rows.iloc[0]['Category']
        e005_price = e005_rows.iloc[0]['Price']
        
        print(f"✓ E005 (no match): Category='{e005_category}', Price={e005_price}")
        
        if e005_category == 'Unknown Category' and e005_price == 0.0:
            print("✓ Default values worked correctly")
            return True
        else:
            print("✗ Default values not applied correctly")
            return False
    else:
        print("✗ E005 row not found")
        return False


def test_prefix_suffix():
    """Test adding prefix/suffix to lookup column names."""
    
    print("\nTesting prefix and suffix...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    # Test with prefix and suffix
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Lookup with prefix/suffix',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category', 'Price'],
        'add_prefix': 'Product_',
        'add_suffix': '_Info'
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    print(f"✓ Prefix/suffix result: {len(result)} columns")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Check that columns have prefix and suffix
    expected_columns = ['Product_Category_Info', 'Product_Price_Info']
    has_expected = all(col in result.columns for col in expected_columns)
    
    if has_expected:
        print("✓ Prefix and suffix worked correctly")
        return True
    else:
        print(f"✗ Expected columns {expected_columns} not found")
        return False


def test_case_sensitivity():
    """Test case-sensitive and case-insensitive matching."""
    
    print("\nTesting case sensitivity...")
    
    # Create data with mixed case
    main_df = pd.DataFrame({
        'Product_Code': ['a001', 'B002', 'c003'],
        'step_description': ['Widget', 'Gadget', 'Tool']
    })
    
    lookup_df = pd.DataFrame({
        'Code': ['A001', 'B002', 'C003'],
        'Category': ['Electronics', 'Tools', 'Hardware']
    })
    
    # Test case-insensitive lookup
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Case insensitive lookup',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category'],
        'case_sensitive': False
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    print(f"✓ Case insensitive result: {len(result)} rows")
    
    # Check that lowercase a001 matched uppercase A001
    first_category = result.iloc[0]['Category']
    print(f"✓ First category (a001→A001): '{first_category}'")
    
    if first_category == 'Electronics':
        print("✓ Case-insensitive matching worked correctly")
        return True
    else:
        print("✗ Case-insensitive matching failed")
        return False


def test_duplicate_handling():
    """Test handling of duplicate keys in lookup data."""
    
    print("\nTesting duplicate handling...")
    
    main_df = pd.DataFrame({
        'Code': ['A001', 'B002'],
        'step_description': ['Widget', 'Gadget']
    })
    
    # Create lookup data with duplicates
    lookup_df = pd.DataFrame({
        'Code': ['A001', 'A001', 'B002'],  # A001 appears twice
        'Category': ['Electronics', 'Hardware', 'Tools'],  # Different values
        'Version': [1, 2, 1]
    })
    
    # Test 'first' handling (default)
    step_config_first = {
        'processor_type': 'lookup_data',
        'step_description': 'Handle duplicates - first',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Category'],
        'handle_duplicates': 'first'
    }
    
    processor_first = LookupDataProcessor(step_config_first)
    result_first = processor_first.execute(main_df)
    
    first_category = result_first.iloc[0]['Category']
    print(f"✓ First duplicate handling: '{first_category}'")
    
    # Test 'last' handling
    step_config_last = {
        'processor_type': 'lookup_data',
        'step_description': 'Handle duplicates - last',
        'lookup_source': lookup_df,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Category'],
        'handle_duplicates': 'last'
    }
    
    processor_last = LookupDataProcessor(step_config_last)
    result_last = processor_last.execute(main_df)
    
    last_category = result_last.iloc[0]['Category']
    print(f"✓ Last duplicate handling: '{last_category}'")
    
    if first_category == 'Electronics' and last_category == 'Hardware':
        print("✓ Duplicate handling worked correctly")
        return True
    else:
        print("✗ Duplicate handling failed")
        return False


def test_dictionary_lookup_source():
    """Test using dictionary as lookup source."""
    
    print("\nTesting dictionary lookup source...")
    
    main_df = create_main_data()
    
    # Create lookup data as dictionary
    lookup_dict = {
        'Code': ['A001', 'B002', 'C003'],
        'Category': ['Electronics', 'Tools', 'Hardware'],
        'Price': [10.50, 25.00, 15.75]
    }
    
    step_config = {
        'processor_type': 'lookup_data',
        'step_description': 'Dictionary lookup',
        'lookup_source': lookup_dict,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category']
    }
    
    processor = LookupDataProcessor(step_config)
    result = processor.execute(main_df)
    
    print(f"✓ Dictionary lookup result: {len(result)} rows")
    
    # Check that lookup worked
    first_category = result.iloc[0]['Category']
    
    if first_category == 'Electronics':
        print("✓ Dictionary lookup source worked correctly")
        return True
    else:
        print("✗ Dictionary lookup source failed")
        return False


def test_file_lookup_source():
    """Test using Excel file as lookup source."""
    
    print("\nTesting file lookup source...")
    
    main_df = create_main_data()
    lookup_df = create_product_lookup()
    
    # Create temporary Excel file
    temp_file = Path('temp_lookup.xlsx')
    try:
        lookup_df.to_excel(temp_file, index=False, sheet_name='Products')
        
        step_config = {
            'processor_type': 'lookup_data',
            'step_description': 'File lookup',
            'lookup_source': str(temp_file),
            'lookup_sheet': 'Products',
            'lookup_key': 'Code',
            'source_key': 'Product_Code',
            'lookup_columns': ['Category']
        }
        
        processor = LookupDataProcessor(step_config)
        result = processor.execute(main_df)
        
        print(f"✓ File lookup result: {len(result)} rows")
        
        # Check that lookup worked
        first_category = result.iloc[0]['Category']
        
        if first_category == 'Electronics':
            print("✓ File lookup source worked correctly")
            return True
        else:
            print("✗ File lookup source failed")
            return False
    
    finally:
        # Clean up temp file
        if temp_file.exists():
            temp_file.unlink()


def test_vlookup_helper_method():
    """Test the VLOOKUP-style helper method."""
    
    print("\nTesting VLOOKUP helper method...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    processor = LookupDataProcessor({'processor_type': 'lookup_data'})
    
    result = processor.create_vlookup_style(
        main_df, 
        product_lookup,
        lookup_key='Code',
        source_key='Product_Code',
        return_column='Category',
        default_value='Unknown'
    )
    
    print(f"✓ VLOOKUP helper result: {len(result)} rows")
    
    # Check result
    first_category = result.iloc[0]['Category']
    
    if first_category == 'Electronics':
        print("✓ VLOOKUP helper method worked correctly")
        return True
    else:
        print("✗ VLOOKUP helper method failed")
        return False


def test_index_match_helper_method():
    """Test the INDEX-MATCH style helper method."""
    
    print("\nTesting INDEX-MATCH helper method...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    processor = LookupDataProcessor({'processor_type': 'lookup_data'})
    
    result = processor.create_index_match_style(
        main_df,
        product_lookup,
        lookup_key='Code',
        source_key='Product_Code',
        return_columns=['Category', 'Price']
    )
    
    print(f"✓ INDEX-MATCH helper result: {len(result)} rows")
    
    # Check result
    first_category = result.iloc[0]['Category']
    first_price = result.iloc[0]['Price']
    
    if first_category == 'Electronics' and first_price == 10.50:
        print("✓ INDEX-MATCH helper method worked correctly")
        return True
    else:
        print("✗ INDEX-MATCH helper method failed")
        return False


def test_multi_column_lookup_helper():
    """Test the multi-column lookup helper method."""
    
    print("\nTesting multi-column lookup helper...")
    
    # Create data requiring multi-column lookup
    main_df = pd.DataFrame({
        'Product_Code': ['A001', 'B002'],
        'Supplier_ID': ['SUP1', 'SUP2'],
        'Quantity': [100, 50]
    })
    
    # Create lookup requiring both product and supplier
    lookup_df = pd.DataFrame({
        'Product_Code': ['A001', 'A001', 'B002'],
        'Supplier_ID': ['SUP1', 'SUP2', 'SUP2'],
        'Lead_Time': [5, 7, 3],
        'Cost': [10.0, 12.0, 8.0]
    })
    
    processor = LookupDataProcessor({'processor_type': 'lookup_data'})
    
    result = processor.create_multi_column_lookup(
        main_df,
        lookup_df,
        lookup_keys=['Product_Code', 'Supplier_ID'],
        source_keys=['Product_Code', 'Supplier_ID'],
        return_columns=['Lead_Time', 'Cost']
    )
    
    print(f"✓ Multi-column lookup result: {len(result)} rows")
    
    # Check result
    first_lead_time = result.iloc[0]['Lead_Time']
    
    if first_lead_time == 5:
        print("✓ Multi-column lookup helper worked correctly")
        return True
    else:
        print("✗ Multi-column lookup helper failed")
        return False


def test_lookup_analysis():
    """Test the lookup potential analysis functionality."""
    
    print("\nTesting lookup analysis...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    processor = LookupDataProcessor({'processor_type': 'lookup_data'})
    
    analysis = processor.analyze_lookup_potential(
        main_df, 
        product_lookup,
        main_key='Product_Code',
        lookup_key='Code'
    )
    
    print(f"✓ Analysis results:")
    print(f"  Main data rows: {analysis['main_data_rows']}")
    print(f"  Lookup data rows: {analysis['lookup_data_rows']}")
    print(f"  Potential matches: {analysis['potential_matches']}")
    print(f"  Match rate: {analysis['match_rate']:.2%}")
    print(f"  Recommendations: {analysis['recommendations']}")
    
    if analysis['main_data_rows'] == len(main_df):
        print("✓ Lookup analysis worked correctly")
        return True
    else:
        print("✗ Lookup analysis failed")
        return False


def test_chained_lookups():
    """Test multiple lookup operations in sequence."""
    
    print("\nTesting chained lookups...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    dept_lookup = create_department_lookup()
    
    # First lookup: get product category
    step_config1 = {
        'processor_type': 'lookup_data',
        'step_description': 'First lookup - product category',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category']
    }
    
    processor1 = LookupDataProcessor(step_config1)
    result1 = processor1.execute(main_df)
    
    # Second lookup: get department info
    step_config2 = {
        'processor_type': 'lookup_data',
        'step_description': 'Second lookup - department info',
        'lookup_source': dept_lookup,
        'lookup_key': 'Dept_Code',
        'source_key': 'Department_Code',
        'lookup_columns': ['Department_Name', 'Manager']
    }
    
    processor2 = LookupDataProcessor(step_config2)
    result2 = processor2.execute(result1)
    
    print(f"✓ Chained lookups result: {len(result2)} rows, {len(result2.columns)} columns")
    
    # Check that both lookups worked
    expected_columns = ['Category', 'Department_Name', 'Manager']
    has_all_columns = all(col in result2.columns for col in expected_columns)
    
    if has_all_columns:
        first_category = result2.iloc[0]['Category']
        first_dept = result2.iloc[0]['Department_Name']
        
        print(f"✓ First row: Category='{first_category}', Department='{first_dept}'")
        
        if first_category == 'Electronics' and first_dept == 'Electronics':
            print("✓ Chained lookups worked correctly")
            return True
    
    print("✗ Chained lookups failed")
    return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    main_df = create_main_data()
    product_lookup = create_product_lookup()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'lookup_data',
            'step_description': 'Missing fields'
            # Missing required fields
        }
        processor = LookupDataProcessor(bad_config)
        processor.execute(main_df)
        print("✗ Should have failed with missing fields")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid source key
    try:
        bad_config = {
            'processor_type': 'lookup_data',
            'step_description': 'Invalid source key',
            'lookup_source': product_lookup,
            'lookup_key': 'Code',
            'source_key': 'NonExistentColumn',
            'lookup_columns': ['Category']
        }
        processor = LookupDataProcessor(bad_config)
        processor.execute(main_df)
        print("✗ Should have failed with invalid source key")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid lookup key
    try:
        bad_config = {
            'processor_type': 'lookup_data',
            'step_description': 'Invalid lookup key',
            'lookup_source': product_lookup,
            'lookup_key': 'NonExistentColumn',
            'source_key': 'Product_Code',
            'lookup_columns': ['Category']
        }
        processor = LookupDataProcessor(bad_config)
        processor.execute(main_df)
        print("✗ Should have failed with invalid lookup key")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test error on duplicates
    try:
        duplicate_lookup = pd.DataFrame({
            'Code': ['A001', 'A001'],  # Duplicates
            'Category': ['Electronics', 'Hardware']
        })
        
        bad_config = {
            'processor_type': 'lookup_data',
            'step_description': 'Error on duplicates',
            'lookup_source': duplicate_lookup,
            'lookup_key': 'Code',
            'source_key': 'Product_Code',
            'lookup_columns': ['Category'],
            'handle_duplicates': 'error'
        }
        processor = LookupDataProcessor(bad_config)
        processor.execute(main_df)
        print("✗ Should have failed with duplicate keys")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_real_world_scenario():
    """Test a realistic lookup scenario like enriching order data."""
    
    print("\nTesting real-world scenario...")
    
    # Simulate order data that needs enrichment
    order_df = pd.DataFrame({
        'Order_ID': ['ORD001', 'ORD002', 'ORD003', 'ORD004'],
        'Product_Code': ['A001', 'B002', 'C003', 'A001'],
        'Customer_ID': ['CUST1', 'CUST2', 'CUST1', 'CUST3'],
        'Quantity': [2, 1, 3, 1],
        'Order_Date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18']
    })
    
    # Product lookup
    product_lookup = create_product_lookup()
    
    # Customer lookup
    customer_lookup = pd.DataFrame({
        'Customer_ID': ['CUST1', 'CUST2', 'CUST3'],
        'Customer_Name': ['Acme Corp', 'Beta Industries', 'Gamma LLC'],
        'Credit_Limit': [10000, 5000, 7500],
        'Region': ['West', 'East', 'Central']
    })
    
    print(f"✓ Created order data: {len(order_df)} orders")
    
    # Enrich with product information
    step_config1 = {
        'processor_type': 'lookup_data',
        'step_description': 'Enrich with product info',
        'lookup_source': product_lookup,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Category', 'Price'],
        'add_prefix': 'Product_'
    }
    
    processor1 = LookupDataProcessor(step_config1)
    enriched1 = processor1.execute(order_df)
    
    # Enrich with customer information
    step_config2 = {
        'processor_type': 'lookup_data',
        'step_description': 'Enrich with customer info',
        'lookup_source': customer_lookup,
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Customer_Name', 'Region'],
        'add_suffix': '_Info'
    }
    
    processor2 = LookupDataProcessor(step_config2)
    final_result = processor2.execute(enriched1)
    
    print(f"✓ Enriched order data: {len(final_result)} rows, {len(final_result.columns)} columns")
    print(f"✓ Final columns: {list(final_result.columns)}")
    
    # Check enrichment
    expected_columns = ['Product_Category', 'Product_Price', 'Customer_Name_Info', 'Region_Info']
    has_enrichment = all(col in final_result.columns for col in expected_columns)
    
    if has_enrichment:
        print("✓ Order enrichment example:")
        for i in range(min(2, len(final_result))):
            order_id = final_result.iloc[i]['Order_ID']
            product_cat = final_result.iloc[i]['Product_Category']
            customer_name = final_result.iloc[i]['Customer_Name_Info']
            print(f"  {order_id}: {product_cat} product for {customer_name}")
        
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_basic_vlookup_style()
    success &= test_multiple_column_lookup()
    success &= test_join_types()
    success &= test_default_values()
    success &= test_prefix_suffix()
    success &= test_case_sensitivity()
    success &= test_duplicate_handling()
    success &= test_dictionary_lookup_source()
    success &= test_file_lookup_source()
    success &= test_vlookup_helper_method()
    success &= test_index_match_helper_method()
    success &= test_multi_column_lookup_helper()
    success &= test_lookup_analysis()
    success &= test_chained_lookups()
    success &= test_real_world_scenario()
    test_error_handling()
    
    if success:
        print("\n✓ All lookup data processor tests passed!")
    else:
        print("\n✗ Some lookup data processor tests failed!")
    
    # Show supported features
    processor = LookupDataProcessor({
        'processor_type': 'lookup_data',
        'lookup_source': {},
        'lookup_key': 'key',
        'source_key': 'key', 
        'lookup_columns': ['col']
    })
    print(f"\nSupported join types: {processor.get_supported_join_types()}")
    print(f"Supported duplicate handling: {processor.get_supported_duplicate_handling()}")
