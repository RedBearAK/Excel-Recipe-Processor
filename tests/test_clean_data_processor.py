"""
Test the CleanDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_messy_test_data():
    """Create sample data with various cleaning needs."""
    return pd.DataFrame({
        'Product_Name': ['  CANNED Beans  ', 'fresh fish', 'CANNED CORN!', 'dried fruit', None],
        'Component': ['FLESH', 'flesh', 'FLESH', 'FLESH', 'FLESH'],
        'Price': ['$10.50', '25.00', '$15.75', 'invalid', '12.00'],
        'Quantity': [100, 50, 75, 25, None],
        'Date_Text': ['2024-01-15', '01/15/2024', '2024-15-01', 'invalid', '2024-02-20'],
        'Status': ['active', 'ACTIVE', 'Active', 'cancelled', 'pending']
    })


def test_replace_operations():
    """Test find and replace operations like the van report workflow."""
    
    print("Testing replace operations...")
    
    test_df = create_messy_test_data()
    print(f"✓ Created messy test data: {len(test_df)} rows")
    
    # Debug: Check the actual test data
    print("Component column before replace:", test_df['Component'].tolist())
    
    # Test basic replace - like replacing FLESH with CANS in van report
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Replace FLESH with CANS',
        'rules': [
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that FLESH was replaced with CANS
    flesh_count = (result['Component'] == 'FLESH').sum()
    cans_count = (result['Component'] == 'CANS').sum()
    lowercase_flesh_count = (result['Component'] == 'flesh').sum()
    
    print(f"✓ Replace FLESH→CANS: {flesh_count} FLESH remaining, {cans_count} CANS found, {lowercase_flesh_count} lowercase flesh")
    print("Component column after replace:", result['Component'].tolist())
    
    # CORRECTED EXPECTATION: Should be 4 CANS (uppercase FLESH replaced) and 1 'flesh' (lowercase unchanged)
    if flesh_count == 0 and cans_count == 4 and lowercase_flesh_count == 1:
        print("✓ Case-sensitive replace worked correctly")
    else:
        print(f"✗ Unexpected replace result: {flesh_count} FLESH, {cans_count} CANS, {lowercase_flesh_count} lowercase")
    
    # Test case-insensitive replace
    step_config2 = {
        'processor_type': 'clean_data',
        'step_description': 'Case-insensitive replace',
        'rules': [
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'flesh',
                'new_value': 'MEAT',
                'case_sensitive': False
            }
        ]
    }
    
    processor2 = CleanDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    meat_count = (result2['Component'] == 'MEAT').sum()
    print(f"✓ Case-insensitive replace: {meat_count} MEAT found")
    
    return True


def test_text_transformations():
    """Test text case and whitespace operations."""
    
    print("\nTesting text transformations...")
    
    test_df = create_messy_test_data()
    
    # Test multiple text cleaning operations
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Text cleaning',
        'rules': [
            {
                'column': 'Product_Name',
                'action': 'strip_whitespace'
            },
            {
                'column': 'Product_Name',
                'action': 'uppercase'
            },
            {
                'column': 'Product_Name',
                'action': 'remove_special_chars'
            },
            {
                'column': 'Status',
                'action': 'lowercase'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check results
    first_product = result.iloc[0]['Product_Name']
    first_status = result.iloc[0]['Status']
    
    print(f"✓ Cleaned product name: '{first_product}'")
    print(f"✓ Lowercased status: '{first_status}'")
    
    if first_product == 'CANNED BEANS' and first_status == 'active':
        print("✓ Text transformations worked correctly")
        return True
    else:
        print("✗ Text transformations failed")
        return False


def test_numeric_cleaning():
    """Test numeric data cleaning."""
    
    print("\nTesting numeric cleaning...")
    
    test_df = create_messy_test_data()
    
    # Clean price column - remove $ signs and convert to numeric
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Clean prices',
        'rules': [
            {
                'column': 'Price',
                'action': 'fix_numeric',
                'fill_na': 0.0
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that prices are now numeric
    price_column = result['Price']
    print(f"✓ Price column dtype: {price_column.dtype}")
    print(f"✓ Sample prices: {price_column.head().tolist()}")
    
    if pd.api.types.is_numeric_dtype(price_column):
        print("✓ Numeric cleaning worked correctly")
        return True
    else:
        print("✗ Numeric cleaning failed")
        return False


def test_fill_empty_values():
    """Test filling empty/null values."""
    
    print("\nTesting fill empty values...")
    
    test_df = create_messy_test_data()
    
    # Fill empty values
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Fill empty values',
        'rules': [
            {
                'column': 'Product_Name',
                'action': 'fill_empty',
                'fill_value': 'Unknown Product',
                'method': 'value'
            },
            {
                'column': 'Quantity',
                'action': 'fill_empty',
                'fill_value': 0,
                'method': 'value'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that null values were filled
    null_products = result['Product_Name'].isnull().sum()
    null_quantities = result['Quantity'].isnull().sum()
    
    print(f"✓ Null products remaining: {null_products}")
    print(f"✓ Null quantities remaining: {null_quantities}")
    
    if null_products == 0 and null_quantities == 0:
        print("✓ Fill empty values worked correctly")
        return True
    else:
        print("✗ Fill empty values failed")
        return False


def test_regex_operations():
    """Test regex-based cleaning."""
    
    print("\nTesting regex operations...")
    
    test_df = create_messy_test_data()
    
    # Use regex to clean up product names
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Regex cleaning',
        'rules': [
            {
                'column': 'Product_Name',
                'action': 'regex_replace',
                'pattern': r'[!@#$%^&*()]',
                'replacement': ''
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that special characters were removed
    third_product = result.iloc[2]['Product_Name']
    print(f"✓ Cleaned product (was 'CANNED CORN!'): '{third_product}'")
    
    if '!' not in str(third_product):
        print("✓ Regex operations worked correctly")
        return True
    else:
        print("✗ Regex operations failed")
        return False


def test_standardize_values():
    """Test value standardization using mappings."""
    
    print("\nTesting value standardization...")
    
    test_df = create_messy_test_data()
    
    # Standardize status values
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Standardize status',
        'rules': [
            {
                'column': 'Status',
                'action': 'standardize_values',
                'mapping': {
                    'active': 'Active',
                    'ACTIVE': 'Active',
                    'cancelled': 'Cancelled',
                    'pending': 'Pending'
                }
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check standardization
    unique_statuses = result['Status'].unique()
    print(f"✓ Standardized statuses: {sorted(unique_statuses)}")
    
    expected_statuses = {'Active', 'Cancelled', 'Pending'}
    actual_statuses = set(unique_statuses)
    
    if actual_statuses == expected_statuses:
        print("✓ Value standardization worked correctly")
        return True
    else:
        print(f"✗ Expected {expected_statuses}, got {actual_statuses}")
        return False


def test_multiple_rules():
    """Test applying multiple cleaning rules in sequence."""
    
    print("\nTesting multiple cleaning rules...")
    
    test_df = create_messy_test_data()
    
    # Complex cleaning like in van report workflow
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Complex cleaning sequence',
        'rules': [
            # First strip whitespace
            {
                'column': 'Product_Name',
                'action': 'strip_whitespace'
            },
            # Then convert to uppercase
            {
                'column': 'Product_Name',
                'action': 'uppercase'
            },
            # Replace FLESH with CANS in component
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS'
            },
            # Clean up prices
            {
                'column': 'Price',
                'action': 'fix_numeric'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Verify multiple transformations
    first_product = result.iloc[0]['Product_Name']
    first_component = result.iloc[0]['Component']
    
    print(f"✓ Final product name: '{first_product}'")
    print(f"✓ Final component: '{first_component}'")
    
    if first_product == 'CANNED BEANS' and first_component == 'CANS':
        print("✓ Multiple rules applied correctly")
        return True
    else:
        print("✗ Multiple rules failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_messy_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Missing rules'
            # No 'rules' field
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing rules")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid column
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Invalid column',
            'rules': [
                {
                    'column': 'NonExistentColumn',
                    'action': 'uppercase'
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid action
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Invalid action',
            'rules': [
                {
                    'column': 'Status',
                    'action': 'invalid_action'
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid action")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")



def test_conditional_replacement():
    """Test conditional replacement like the van report FLESH->CANS scenario."""
    
    print("\nTesting conditional replacement...")
    
    # Create data that mimics the van report scenario
    test_df = pd.DataFrame({
        'Product_Name': [
            'CANNED BEANS',      # Should trigger replacement
            'FRESH SALMON',      # Should NOT trigger replacement  
            'CANNED CORN',       # Should trigger replacement
            'DRIED FISH',        # Should NOT trigger replacement
            'canned soup'        # Should trigger replacement (case insensitive)
        ],
        'Component': [
            'FLESH',             # Should become CANS
            'FLESH',             # Should stay FLESH
            'FLESH',             # Should become CANS
            'FLESH',             # Should stay FLESH
            'FLESH'              # Should become CANS
        ]
    })
    
    print(f"✓ Created van report test data: {len(test_df)} rows")
    print("Original data:")
    for i in range(len(test_df)):
        product = test_df.iloc[i]['Product_Name']
        component = test_df.iloc[i]['Component']
        print(f"  {product}: {component}")
    
    # Test conditional replacement - only replace FLESH in canned products
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Conditional FLESH to CANS replacement',
        'rules': [
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS',
                'condition_column': 'Product_Name',
                'condition': 'contains',
                'condition_value': 'CANNED',
                'case_sensitive': False  # Should catch 'canned soup'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print("\nAfter conditional replacement:")
    cans_count = 0
    flesh_count = 0
    
    for i in range(len(result)):
        product = result.iloc[i]['Product_Name']
        component = result.iloc[i]['Component']
        print(f"  {product}: {component}")
        
        if component == 'CANS':
            cans_count += 1
        elif component == 'FLESH':
            flesh_count += 1
    
    print(f"\n✓ Results: {cans_count} CANS, {flesh_count} FLESH")
    
    # Should have 3 CANS (canned products) and 2 FLESH (non-canned products)
    if cans_count == 3 and flesh_count == 2:
        print("✓ Conditional replacement worked correctly")
        return True
    else:
        print(f"✗ Expected 3 CANS and 2 FLESH, got {cans_count} CANS and {flesh_count} FLESH")
        return False


def test_conditional_replacement_with_equals():
    """Test conditional replacement with equals condition."""
    
    print("\nTesting conditional replacement with equals...")
    
    test_df = pd.DataFrame({
        'Status': ['Active', 'Inactive', 'Active', 'Pending', 'Active'],
        'Priority': ['High', 'Low', 'Medium', 'High', 'Low'],
        'Notes': ['urgent', 'normal', 'urgent', 'normal', 'urgent']
    })
    
    print("Test data:")
    for i in range(len(test_df)):
        status = test_df.iloc[i]['Status']
        notes = test_df.iloc[i]['Notes']
        print(f"  Row {i}: Status='{status}', Notes='{notes}'")
    
    # Replace 'urgent' with 'PRIORITY' only for Active status
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Conditional notes update',
        'rules': [
            {
                'column': 'Notes',
                'action': 'replace',
                'old_value': 'urgent',
                'new_value': 'PRIORITY',
                'condition_column': 'Status',
                'condition': 'equals',
                'condition_value': 'Active'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print("After conditional replacement:")
    for i in range(len(result)):
        status = result.iloc[i]['Status']
        notes = result.iloc[i]['Notes']
        print(f"  Row {i}: Status='{status}', Notes='{notes}'")
    
    # Check results - should be 3 Active rows with 'urgent' → all become 'PRIORITY'
    priority_count = (result['Notes'] == 'PRIORITY').sum()
    urgent_count = (result['Notes'] == 'urgent').sum()
    
    print(f"✓ Priority notes: {priority_count}, Urgent remaining: {urgent_count}")
    
    # CORRECTED EXPECTATION: Should have 3 PRIORITY (all Active+urgent rows) and 0 urgent (all were replaced)
    if priority_count == 3 and urgent_count == 0:
        print("✓ Conditional replacement with equals worked correctly")
        return True
    else:
        print(f"✗ Unexpected results: {priority_count} PRIORITY, {urgent_count} urgent")
        return False


def test_conditional_replacement_numeric():
    """Test conditional replacement with numeric conditions."""
    
    print("\nTesting conditional replacement with numeric conditions...")
    
    test_df = pd.DataFrame({
        'Price': [100, 50, 200, 25, 150],
        'Category': ['Standard', 'Standard', 'Standard', 'Standard', 'Standard'],
        'Quantity': [10, 5, 20, 3, 15]
    })
    
    # Change category to Premium for high-price items
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Upgrade high-price categories',
        'rules': [
            {
                'column': 'Category',
                'action': 'replace',
                'old_value': 'Standard',
                'new_value': 'Premium',
                'condition_column': 'Price',
                'condition': 'greater_than',
                'condition_value': 100
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check results
    premium_count = (result['Category'] == 'Premium').sum()
    standard_count = (result['Category'] == 'Standard').sum()
    
    print(f"✓ Premium items: {premium_count}, Standard items: {standard_count}")
    
    # Should have 2 Premium (price > 100) and 3 Standard (price <= 100)
    if premium_count == 2 and standard_count == 3:
        print("✓ Conditional replacement with numeric condition worked correctly")
        return True
    else:
        print(f"✗ Unexpected results: {premium_count} Premium, {standard_count} Standard")
        return False


def test_conditional_replacement_error_handling():
    """Test error handling for conditional replacement."""
    
    print("\nTesting conditional replacement error handling...")
    
    test_df = create_messy_test_data()
    
    # Test missing condition_column (but has other conditional fields)
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Missing condition column',
            'rules': [
                {
                    'column': 'Component',
                    'action': 'replace',
                    'old_value': 'FLESH',
                    'new_value': 'CANS',
                    'condition': 'contains',           # Has this conditional field
                    'condition_value': 'CANNED'       # Has this conditional field
                    # Missing 'condition_column' - should fail
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with incomplete conditional config")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test having only condition_column (but missing other conditional fields)
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Incomplete conditional config',
            'rules': [
                {
                    'column': 'Component',
                    'action': 'replace',
                    'old_value': 'FLESH',
                    'new_value': 'CANS',
                    'condition_column': 'Product_Name'  # Has this but missing condition and condition_value
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with incomplete conditional config")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_van_report_exact_scenario():
    """Test the exact van report scenario described in the requirements."""
    
    print("\nTesting exact van report scenario...")
    
    # Create data that exactly matches the van report description
    test_df = pd.DataFrame({
        'PRODUCT NAME': [
            'CANNED SALMON',
            'FRESH HALIBUT', 
            'CANNED TUNA',
            'FROZEN COD',
            'CANNED SARDINES',
            'FRESH SALMON'
        ],
        'COMPONENT': [
            'FLESH',    # Should become CANS
            'FLESH',    # Should stay FLESH  
            'FLESH',    # Should become CANS
            'FLESH',    # Should stay FLESH
            'FLESH',    # Should become CANS
            'FLESH'     # Should stay FLESH
        ],
        'MAJOR SPECIES': ['SALMON', 'HALIBUT', 'TUNA', 'COD', 'SARDINES', 'SALMON'],
        'PRODUCT ORIGIN': ['Naknek', 'Kodiak', 'Seward', 'Dutch Harbor', 'Sitka', 'Cordova']
    })
    
    print(f"✓ Created exact van report scenario: {len(test_df)} rows")
    print("Before processing:")
    for i in range(len(test_df)):
        product = test_df.iloc[i]['PRODUCT NAME']
        component = test_df.iloc[i]['COMPONENT']
        print(f"  {product}: {component}")
    
    # Apply the exact cleaning rule from the van report
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Van report FLESH to CANS replacement',
        'rules': [
            {
                'column': 'COMPONENT',
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS',
                'condition_column': 'PRODUCT NAME',
                'condition': 'contains',
                'condition_value': 'CANNED',
                'case_sensitive': False
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print("\nAfter van report processing:")
    canned_products_with_cans = 0
    fresh_products_with_flesh = 0
    
    for i in range(len(result)):
        product = result.iloc[i]['PRODUCT NAME']
        component = result.iloc[i]['COMPONENT']
        print(f"  {product}: {component}")
        
        if 'CANNED' in product.upper() and component == 'CANS':
            canned_products_with_cans += 1
        elif 'CANNED' not in product.upper() and component == 'FLESH':
            fresh_products_with_flesh += 1
    
    print(f"\n✓ Canned products with CANS: {canned_products_with_cans}")
    print(f"✓ Non-canned products with FLESH: {fresh_products_with_flesh}")
    
    # Should have 3 canned products with CANS and 3 non-canned with FLESH
    if canned_products_with_cans == 3 and fresh_products_with_flesh == 3:
        print("✓ Van report exact scenario worked perfectly!")
        return True
    else:
        print("✗ Van report scenario failed")
        return False


def test_remove_invisible_chars():
    """Test removal of invisible Unicode characters."""
    
    print("\nTesting remove invisible characters...")
    
    # Create test data with invisible characters that break filtering
    test_df = pd.DataFrame({
        'Component': [
            'CANS\u200B',      # Zero-width space
            'FLESH\uFEFF',     # BOM character  
            'CANS\u00A0',      # Non-breaking space
            '\u2000FLESH',     # En quad at start
            'CANS\u200C\u200D' # Multiple invisible chars
        ],
        'Product_Name': [
            'Test\u200BProduct',
            'Normal Product', 
            'Another\uFEFFTest',
            'Clean Product',
            'Final\u00A0Test'
        ]
    })
    
    print("Before invisible char removal:")
    for i in range(len(test_df)):
        component = repr(test_df.iloc[i]['Component'])  # Use repr to show invisible chars
        print(f"  Component {i}: {component}")
    
    # Test remove_invisible_chars action
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Remove invisible characters',
        'rules': [
            {
                'column': 'Component',
                'action': 'remove_invisible_chars'
            },
            {
                'column': 'Product_Name', 
                'action': 'remove_invisible_chars'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print("After invisible char removal:")
    for i in range(len(result)):
        component = repr(result.iloc[i]['Component'])
        print(f"  Component {i}: {component}")
    
    # Check that invisible characters were removed
    clean_components = result['Component'].tolist()
    expected_clean = ['CANS', 'FLESH', 'CANS', 'FLESH', 'CANS']
    
    if clean_components == expected_clean:
        print("✓ Invisible character removal worked correctly")
        return True
    else:
        print(f"✗ Expected {expected_clean}, got {clean_components}")
        return False


def test_normalize_whitespace():
    """Test comprehensive whitespace normalization."""
    
    print("\nTesting normalize whitespace...")
    
    # Create test data with various whitespace issues
    test_df = pd.DataFrame({
        'Product_Origin': [
            '  CORDOVA  ',           # Leading/trailing spaces
            'NAKNEK\u200B\u200C',    # Invisible chars
            'DILLINGHAM\n\r',        # Line endings
            'FALSE\u00A0PASS',       # Non-breaking space in middle
            '  KODIAK\t\t  '         # Mixed whitespace
        ],
        'Notes': [
            'Multiple   spaces   here',
            'Line\nbreak\rhere',
            '\u200BSome\u200Ctext\uFEFF',
            '  Clean this up  ',
            'Normal text'
        ]
    })
    
    print("Before whitespace normalization:")
    for i in range(len(test_df)):
        origin = repr(test_df.iloc[i]['Product_Origin'])
        notes = repr(test_df.iloc[i]['Notes'])
        print(f"  Row {i}: Origin={origin}, Notes={notes}")
    
    # Test normalize_whitespace action (should do invisible chars + regular whitespace)
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Normalize all whitespace',
        'rules': [
            {
                'column': 'Product_Origin',
                'action': 'normalize_whitespace'
            },
            {
                'column': 'Notes',
                'action': 'normalize_whitespace'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print("After whitespace normalization:")
    for i in range(len(result)):
        origin = repr(result.iloc[i]['Product_Origin'])
        notes = repr(result.iloc[i]['Notes'])
        print(f"  Row {i}: Origin={origin}, Notes={notes}")
    
    # Check expected results
    clean_origins = result['Product_Origin'].tolist()
    expected_origins = ['CORDOVA', 'NAKNEK', 'DILLINGHAM', 'FALSE PASS', 'KODIAK']
    
    clean_notes = result['Notes'].tolist()
    expected_notes = ['Multiple spaces here', 'Line break here', 'Sometext', 'Clean this up', 'Normal text']
    
    origins_match = clean_origins == expected_origins
    notes_match = clean_notes == expected_notes
    
    if origins_match and notes_match:
        print("✓ Whitespace normalization worked correctly")
        return True
    else:
        print(f"✗ Origins match: {origins_match}, Notes match: {notes_match}")
        print(f"Expected origins: {expected_origins}")
        print(f"Got origins: {clean_origins}")
        print(f"Expected notes: {expected_notes}")
        print(f"Got notes: {clean_notes}")
        return False


def test_invisible_chars_filtering_scenario():
    """Test the scenario where invisible chars break exact filtering."""
    
    print("\nTesting invisible chars breaking filtering scenario...")
    
    # Simulate the exact problem from the van report
    test_df = pd.DataFrame({
        'Component': [
            'CANS\u200B',      # Looks like "CANS" but has invisible char
            'FLESH',           # Clean FLESH
            'CANS\uFEFF',      # Another "CANS" with BOM
            'SALMON\u00A0',    # Looks like "SALMON" with non-breaking space
        ],
        'Major_Species': [
            'SALMON\u200C',    # Invisible char in SALMON
            'HALIBUT',         # Clean
            'SALMON\u200B',    # Another SALMON with invisible char
            'SALMON'           # Clean SALMON
        ]
    })
    
    # Simulate the filtering issue - before cleaning, exact matches fail
    print("Before cleaning - simulating failed filters:")
    cans_matches_before = (test_df['Component'] == 'CANS').sum()
    salmon_matches_before = (test_df['Major_Species'] == 'SALMON').sum()
    print(f"  'CANS' exact matches: {cans_matches_before} (should be 0 due to invisible chars)")
    print(f"  'SALMON' exact matches: {salmon_matches_before} (should be 1)")
    
    # Clean the data
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Fix filtering issues with invisible chars',
        'rules': [
            {
                'column': 'Component',
                'action': 'normalize_whitespace'
            },
            {
                'column': 'Major_Species',
                'action': 'normalize_whitespace'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # After cleaning, exact matches should work
    print("After cleaning - filters should work:")
    cans_matches_after = (result['Component'] == 'CANS').sum()
    salmon_matches_after = (result['Major_Species'] == 'SALMON').sum()
    print(f"  'CANS' exact matches: {cans_matches_after} (should be 2)")
    print(f"  'SALMON' exact matches: {salmon_matches_after} (should be 3)")
    
    if cans_matches_before == 0 and cans_matches_after == 2 and salmon_matches_after == 3:
        print("✓ Invisible chars filtering fix worked correctly")
        return True
    else:
        print("✗ Filtering fix failed")
        return False


if __name__ == '__main__':
    success = True

    success &= test_replace_operations()
    success &= test_text_transformations()
    success &= test_numeric_cleaning()
    success &= test_fill_empty_values()
    success &= test_regex_operations()
    success &= test_standardize_values()
    success &= test_multiple_rules()

    success &= test_conditional_replacement()
    success &= test_conditional_replacement_with_equals()
    success &= test_conditional_replacement_numeric()
    success &= test_van_report_exact_scenario()

    success &= test_remove_invisible_chars()
    success &= test_normalize_whitespace() 
    success &= test_invisible_chars_filtering_scenario()

    test_error_handling()
    test_conditional_replacement_error_handling()
    
    if success:
        print("\n✓ All clean data processor tests passed!")
    else:
        print("\n✗ Some clean data processor tests failed!")
    
    # Show supported actions
    processor = CleanDataProcessor({'processor_type': 'clean_data', 'rules': []})
    print(f"\nSupported actions: {processor.get_supported_actions()}")
