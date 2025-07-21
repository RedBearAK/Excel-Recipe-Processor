"""
Test the SplitColumnProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.split_column_processor import SplitColumnProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_test_data():
    """Create sample data for testing column splitting."""
    return pd.DataFrame({
        'Full_Name': [
            'Smith, John', 'Johnson, Mary', 'Williams, Robert', 'Brown, Sarah', 'Davis, Michael'
        ],
        'Product_Info': [
            'A001|Widget|Electronics', 'B002|Gadget|Tools', 'C003|Device|Hardware',
            'A004|Component|Electronics', 'B005|Accessory|Tools'
        ],
        'Address': [
            '123 Main St, Seattle, WA, 98101',
            '456 Oak Ave, Portland, OR, 97201', 
            '789 Pine Rd, Vancouver, BC, V6B1A1',
            '321 Elm Dr, San Francisco, CA, 94102',
            '654 Maple Ln, Los Angeles, CA, 90210'
        ],
        'Product_Code': [
            'ELEC001A', 'TOOL002B', 'HARD003C', 'ELEC004D', 'TOOL005E'
        ],
        'Phone': [
            '206-555-1234', '503-555-5678', '604-555-9012', '415-555-3456', '323-555-7890'
        ]
    })


def test_delimiter_splitting():
    """Test basic delimiter-based splitting."""
    
    print("Testing delimiter splitting...")
    
    test_df = create_test_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test splitting names by comma
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split names by comma',
        'source_column': 'Full_Name',
        'split_type': 'delimiter',
        'delimiter': ',',
        'new_column_names': ['Last_Name', 'First_Name'],
        'max_splits': 1
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Delimiter split: {len(result.columns)} columns")
    print(f"✓ New columns: {[col for col in result.columns if col not in test_df.columns]}")
    
    # Check that split worked correctly
    if 'Last_Name' in result.columns and 'First_Name' in result.columns:
        print("✓ Expected columns created")
        
        # Check specific values
        first_last = result.iloc[0]['Last_Name'].strip()
        first_first = result.iloc[0]['First_Name'].strip()
        
        print(f"✓ First record: '{first_last}', '{first_first}'")
        
        if first_last == 'Smith' and first_first == 'John':
            print("✓ Delimiter splitting worked correctly")
            return True
        else:
            print(f"✗ Expected 'Smith', 'John', got '{first_last}', '{first_first}'")
    
    print("✗ Delimiter splitting failed")
    return False


def test_pipe_delimiter_splitting():
    """Test splitting with pipe delimiter (common in data)."""
    
    print("\nTesting pipe delimiter splitting...")
    
    test_df = create_test_data()
    
    # Test splitting product info by pipe
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split product info by pipe',
        'source_column': 'Product_Info',
        'split_type': 'delimiter',
        'delimiter': '|',
        'new_column_names': ['Product_Code', 'Product_Name', 'Category']
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Pipe split: {len(result.columns)} columns")
    
    # Check results
    expected_cols = ['Product_Code', 'Product_Name', 'Category']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ All expected columns created")
        
        # Check specific values
        code = result.iloc[0]['Product_Code']
        name = result.iloc[0]['Product_Name'] 
        category = result.iloc[0]['Category']
        
        print(f"✓ First product: '{code}' | '{name}' | '{category}'")
        
        if code == 'A001' and name == 'Widget' and category == 'Electronics':
            print("✓ Pipe delimiter splitting worked correctly")
            return True
        else:
            print(f"✗ Unexpected values: {code}, {name}, {category}")
    
    print("✗ Pipe delimiter splitting failed")
    return False


def test_fixed_width_splitting():
    """Test fixed-width splitting."""
    
    print("\nTesting fixed width splitting...")
    
    test_df = create_test_data()
    
    # Test splitting product codes (ELEC001A = ELEC + 001 + A)
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split product codes by width',
        'source_column': 'Product_Code',
        'split_type': 'fixed_width',
        'widths': [4, 3, 1],
        'new_column_names': ['Category_Code', 'Item_Number', 'Variant'],
        'strip_whitespace': True
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Fixed width split: {len(result.columns)} columns")
    
    # Check results
    expected_cols = ['Category_Code', 'Item_Number', 'Variant']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ Fixed width columns created")
        
        # Check specific values
        category = result.iloc[0]['Category_Code']
        number = result.iloc[0]['Item_Number']
        variant = result.iloc[0]['Variant']
        
        print(f"✓ First code split: '{category}' + '{number}' + '{variant}'")
        
        if category == 'ELEC' and number == '001' and variant == 'A':
            print("✓ Fixed width splitting worked correctly")
            return True
        else:
            print(f"✗ Expected 'ELEC', '001', 'A', got '{category}', '{number}', '{variant}'")
    
    print("✗ Fixed width splitting failed")
    return False


def test_regex_splitting():
    """Test regex pattern splitting."""
    
    print("\nTesting regex splitting...")
    
    test_df = create_test_data()
    
    # Test splitting phone numbers by dash
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split phone by regex',
        'source_column': 'Phone',
        'split_type': 'regex',
        'pattern': r'-',
        'new_column_names': ['Area_Code', 'Exchange', 'Number']
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Regex split: {len(result.columns)} columns")
    
    # Check results
    expected_cols = ['Area_Code', 'Exchange', 'Number']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ Regex split columns created")
        
        # Check specific values
        area = result.iloc[0]['Area_Code']
        exchange = result.iloc[0]['Exchange']
        number = result.iloc[0]['Number']
        
        print(f"✓ First phone split: '{area}' - '{exchange}' - '{number}'")
        
        if area == '206' and exchange == '555' and number == '1234':
            print("✓ Regex splitting worked correctly")
            return True
        else:
            print(f"✗ Expected '206', '555', '1234', got '{area}', '{exchange}', '{number}'")
    
    print("✗ Regex splitting failed")
    return False


def test_position_splitting():
    """Test position-based splitting."""
    
    print("\nTesting position splitting...")
    
    test_df = create_test_data()
    
    # Test splitting product codes at specific positions
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split by position',
        'source_column': 'Product_Code',
        'split_type': 'position',
        'positions': [4, 7],  # Split at positions 4 and 7
        'new_column_names': ['Category_Part', 'Number_Part', 'Letter_Part']
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Position split: {len(result.columns)} columns")
    
    # Check results
    expected_cols = ['Category_Part', 'Number_Part', 'Letter_Part']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ Position split columns created")
        
        # Check specific values (ELEC001A split at 4,7 = ELEC + 001 + A)
        part1 = result.iloc[0]['Category_Part']
        part2 = result.iloc[0]['Number_Part']
        part3 = result.iloc[0]['Letter_Part']
        
        print(f"✓ Position split: '{part1}' | '{part2}' | '{part3}'")
        
        if part1 == 'ELEC' and part2 == '001' and part3 == 'A':
            print("✓ Position splitting worked correctly")
            return True
        else:
            print(f"✗ Expected 'ELEC', '001', 'A', got '{part1}', '{part2}', '{part3}'")
    
    print("✗ Position splitting failed")
    return False


def test_remove_original_option():
    """Test removing the original column after splitting."""
    
    print("\nTesting remove original option...")
    
    test_df = create_test_data()
    original_columns = set(test_df.columns)
    
    # Test with remove original
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split and remove original',
        'source_column': 'Full_Name',
        'split_type': 'delimiter',
        'delimiter': ',',
        'new_column_names': ['Last_Name', 'First_Name'],
        'remove_original': True
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ After split with remove: {len(result.columns)} columns")
    
    # Check that original column is gone and new columns exist
    has_original = 'Full_Name' in result.columns
    has_new = 'Last_Name' in result.columns and 'First_Name' in result.columns
    
    if not has_original and has_new:
        print("✓ Original column removed, new columns added")
        return True
    else:
        print(f"✗ Remove original failed: has_original={has_original}, has_new={has_new}")
        return False


def test_fill_missing_option():
    """Test handling missing values in splits."""
    
    print("\nTesting fill missing option...")
    
    # Create data with inconsistent splits
    test_df = pd.DataFrame({
        'Data': [
            'A|B|C',     # 3 parts
            'X|Y',       # 2 parts  
            'Z',         # 1 part
            'P|Q|R|S'    # 4 parts
        ]
    })
    
    # Test with fill_missing
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Split with fill missing',
        'source_column': 'Data',
        'split_type': 'delimiter',
        'delimiter': '|',
        'new_column_names': ['Part1', 'Part2', 'Part3'],
        'fill_missing': 'N/A'
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Fill missing split: {len(result.columns)} columns")
    
    # Check that missing parts are filled
    part3_values = result['Part3'].tolist()
    print(f"✓ Part3 values: {part3_values}")
    
    # Should have 'C', 'N/A', 'N/A', 'R' (truncated to 3 columns)
    has_fill_value = 'N/A' in part3_values
    
    if has_fill_value:
        print("✓ Fill missing option worked correctly")
        return True
    else:
        print("✗ Fill missing option failed")
        return False


def test_name_splitting_helper():
    """Test the split_name_column helper method."""
    
    print("\nTesting name splitting helper...")
    
    test_df = pd.DataFrame({
        'Customer_Name': [
            'Smith, John',
            'Johnson, Mary Kate', 
            'Williams, Robert'
        ],
        'Employee_Name': [
            'John Smith',
            'Mary Johnson',
            'Robert Williams'
        ]
    })
    
    processor = SplitColumnProcessor({
        'processor_type': 'split_column',
        'source_column': 'test',
        'split_type': 'delimiter'
    })
    
    # Test last, first format
    result1 = processor.split_name_column(test_df, 'Customer_Name', 'last_first')
    
    print(f"✓ Last,First split: {list(result1.columns)}")
    
    # Test first last format  
    result2 = processor.split_name_column(test_df, 'Employee_Name', 'first_last')
    
    print(f"✓ First Last split: {list(result2.columns)}")
    
    # Check results
    has_last_first = 'Last_Name' in result1.columns and 'First_Name' in result1.columns
    has_first_last = 'First_Name' in result2.columns and 'Last_Name' in result2.columns
    
    if has_last_first and has_first_last:
        # Check specific values
        last1 = result1.iloc[0]['Last_Name']
        first1 = result1.iloc[0]['First_Name']
        first2 = result2.iloc[0]['First_Name'] 
        last2 = result2.iloc[0]['Last_Name']
        
        print(f"✓ Last,First result: '{last1}', '{first1}'")
        print(f"✓ First Last result: '{first2}', '{last2}'")
        
        if last1 == 'Smith' and first1 == 'John' and first2 == 'John' and last2 == 'Smith':
            print("✓ Name splitting helper worked correctly")
            return True
    
    print("✗ Name splitting helper failed")
    return False


def test_column_analysis_helper():
    """Test the analyze_column_patterns helper method."""
    
    print("\nTesting column analysis helper...")
    
    test_df = create_test_data()
    
    processor = SplitColumnProcessor({
        'processor_type': 'split_column',
        'source_column': 'test',
        'split_type': 'delimiter'
    })
    
    # Analyze different columns
    name_analysis = processor.analyze_column_patterns(test_df, 'Full_Name')
    product_analysis = processor.analyze_column_patterns(test_df, 'Product_Info')
    
    print(f"✓ Name analysis suggestions: {len(name_analysis['suggested_splits'])}")
    print(f"✓ Product analysis suggestions: {len(product_analysis['suggested_splits'])}")
    
    # Check that analysis found patterns
    name_suggestions = name_analysis['suggested_splits']
    product_suggestions = product_analysis['suggested_splits']
    
    print(f"Name suggestions: {name_suggestions}")
    print(f"Product suggestions: {product_suggestions}")
    
    # Should detect comma in names and pipe in products
    has_name_pattern = any(',' in str(suggestion) for suggestion in name_suggestions)
    has_product_pattern = any('|' in str(suggestion) for suggestion in product_suggestions)
    
    if has_name_pattern and has_product_pattern:
        print("✓ Column analysis helper worked correctly")
        return True
    else:
        print("✗ Column analysis helper failed")
        return False


def test_auto_column_naming():
    """Test automatic column name generation."""
    
    print("\nTesting auto column naming...")
    
    test_df = create_test_data()
    
    # Test split without providing column names
    step_config = {
        'processor_type': 'split_column',
        'step_description': 'Auto column names',
        'source_column': 'Product_Info',
        'split_type': 'delimiter',
        'delimiter': '|'
        # No new_column_names provided
    }
    
    processor = SplitColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Auto naming result: {len(result.columns)} columns")
    
    # Check for auto-generated column names
    new_columns = [col for col in result.columns if col not in test_df.columns]
    print(f"✓ Auto-generated columns: {new_columns}")
    
    # Should have names like Product_Info_part_1, Product_Info_part_2, etc.
    has_auto_names = any('Product_Info_part' in col for col in new_columns)
    
    if has_auto_names and len(new_columns) > 0:
        print("✓ Auto column naming worked correctly")
        return True
    else:
        print("✗ Auto column naming failed")
        return False


def test_capabilities_method():
    """Test the get_capabilities method."""
    
    print("\nTesting capabilities method...")
    
    processor = SplitColumnProcessor({
        'processor_type': 'split_column',
        'source_column': 'test',
        'split_type': 'delimiter'
    })
    
    capabilities = processor.get_capabilities()
    
    print(f"✓ Capabilities: {capabilities.keys()}")
    
    # Check expected capability fields
    expected_keys = ['description', 'split_types', 'splitting_methods', 'helper_methods']
    has_expected = all(key in capabilities for key in expected_keys)
    
    split_types = capabilities.get('split_types', [])
    print(f"✓ Supported split types: {split_types}")
    
    if has_expected and len(split_types) > 0:
        print("✓ Capabilities method worked correctly")
        return True
    else:
        print("✗ Capabilities method failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'split_column',
            'step_description': 'Missing fields'
            # Missing source_column and split_type
        }
        processor = SplitColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing fields")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid source column
    try:
        bad_config = {
            'processor_type': 'split_column',
            'step_description': 'Invalid column',
            'source_column': 'NonExistentColumn',
            'split_type': 'delimiter',
            'delimiter': ','
        }
        processor = SplitColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test delimiter split without delimiter
    try:
        bad_config = {
            'processor_type': 'split_column',
            'step_description': 'Missing delimiter',
            'source_column': 'Full_Name',
            'split_type': 'delimiter'
            # Missing delimiter field
        }
        processor = SplitColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing delimiter")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid regex pattern
    try:
        bad_config = {
            'processor_type': 'split_column',
            'step_description': 'Invalid regex',
            'source_column': 'Full_Name',
            'split_type': 'regex',
            'pattern': '[invalid regex'
        }
        processor = SplitColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid regex")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_real_world_scenario():
    """Test a realistic column splitting scenario."""
    
    print("\nTesting real-world scenario...")
    
    # Simulate messy customer data export
    test_df = pd.DataFrame({
        'Customer_Info': [
            'Smith, John|john.smith@email.com|206-555-1234|Manager',
            'Johnson, Mary|mary.j@company.com|503-555-5678|Director',
            'Williams, Bob|bwilliams@firm.net|604-555-9012|Analyst',
            'Brown, Sarah|s.brown@corp.org|415-555-3456|Supervisor'
        ],
        'Product_Details': [
            'ELEC-001-A: Electronic Widget (Class A)',
            'TOOL-002-B: Hand Tool Set (Class B)',
            'HARD-003-C: Hardware Kit (Class C)',
            'ELEC-004-D: Electronic Component (Class D)'
        ],
        'Order_Reference': [
            'ORD-2024-001-SEA-RUSH',
            'ORD-2024-002-PDX-STD',
            'ORD-2024-003-VAN-RUSH', 
            'ORD-2024-004-SF-STD'
        ]
    })
    
    print(f"✓ Created real-world data: {len(test_df)} records")
    
    # Split customer info (multiple delimiters)
    step_config1 = {
        'processor_type': 'split_column',
        'step_description': 'Split customer info',
        'source_column': 'Customer_Info',
        'split_type': 'delimiter',
        'delimiter': '|',
        'new_column_names': ['Customer_Name', 'Email', 'Phone', 'Title'],
        'remove_original': True
    }
    
    processor1 = SplitColumnProcessor(step_config1)
    result1 = processor1.execute(test_df)
    
    print(f"✓ Customer info split: {len(result1.columns)} columns")
    
    # Split product details by colon
    step_config2 = {
        'processor_type': 'split_column',
        'step_description': 'Split product details',
        'source_column': 'Product_Details',
        'split_type': 'delimiter',
        'delimiter': ':',
        'new_column_names': ['Product_Code', 'Product_Description'],
        'max_splits': 1,
        'remove_original': True
    }
    
    processor2 = SplitColumnProcessor(step_config2)
    result2 = processor2.execute(result1)
    
    print(f"✓ Product details split: {len(result2.columns)} columns")
    
    # Split order reference by dashes
    step_config3 = {
        'processor_type': 'split_column',
        'step_description': 'Split order reference',
        'source_column': 'Order_Reference',
        'split_type': 'delimiter',
        'delimiter': '-',
        'new_column_names': ['Order_Prefix', 'Year', 'Order_Number', 'Location', 'Priority'],
        'remove_original': True
    }
    
    processor3 = SplitColumnProcessor(step_config3)
    result3 = processor3.execute(result2)
    
    print(f"✓ Order reference split: {len(result3.columns)} columns")
    print(f"✓ Final columns: {list(result3.columns)}")
    
    # Verify final results
    expected_cols = [
        'Customer_Name', 'Email', 'Phone', 'Title',
        'Product_Code', 'Product_Description', 
        'Order_Prefix', 'Year', 'Order_Number', 'Location', 'Priority'
    ]
    
    has_all_expected = all(col in result3.columns for col in expected_cols)
    
    if has_all_expected:
        # Show sample final result
        print("Sample processed record:")
        first_row = result3.iloc[0]
        print(f"  Customer: {first_row['Customer_Name']}")
        print(f"  Email: {first_row['Email']}")
        print(f"  Product: {first_row['Product_Code']} - {first_row['Product_Description']}")
        print(f"  Order: {first_row['Order_Prefix']}-{first_row['Year']}-{first_row['Order_Number']}")
        print(f"  Location: {first_row['Location']}, Priority: {first_row['Priority']}")
        
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        missing = [col for col in expected_cols if col not in result3.columns]
        print(f"✗ Missing expected columns: {missing}")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_delimiter_splitting()
    success &= test_pipe_delimiter_splitting()
    success &= test_fixed_width_splitting()
    success &= test_regex_splitting()
    success &= test_position_splitting()
    success &= test_remove_original_option()
    success &= test_fill_missing_option()
    success &= test_name_splitting_helper()
    success &= test_column_analysis_helper()
    success &= test_auto_column_naming()
    success &= test_capabilities_method()
    success &= test_real_world_scenario()
    test_error_handling()
    
    if success:
        print("\n✓ All split column processor tests passed!")
    else:
        print("\n✗ Some split column processor tests failed!")
    
    # Show supported features
    processor = SplitColumnProcessor({
        'processor_type': 'split_column',
        'source_column': 'test',
        'split_type': 'delimiter'
    })
    print(f"\nSupported split types: {processor.get_supported_split_types()}")
