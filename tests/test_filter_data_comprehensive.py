"""
Comprehensive test for the enhanced FilterDataProcessor.

Tests all existing functionality (backward compatibility) plus new features:
- Case sensitivity support
- Enhanced list conditions
- Individual pattern conditions  
- Numeric list conditions
- Stage-based filtering enhancements

Run with: python test_filter_data_comprehensive.py

File: tests/test_filter_data_comprehensive.py
"""

import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor


def create_test_data():
    """Create comprehensive test dataset."""
    return pd.DataFrame({
        'Product_ID': ['PROD-001', 'PROD-002', 'TEST-003', 'DEMO-004', 'PROD-005'],
        'Product_Name': ['Widget Alpha', 'GADGET beta', 'Test Item', 'Demo Product', 'WIDGET gamma'],
        'Category': ['Electronics', 'ELECTRONICS', 'electronics', 'Tools', 'Electronics'],
        'Price': [10.50, 25.00, 15.75, 8.25, 30.00],
        'Status': ['Active', 'ACTIVE', 'active', 'Inactive', 'PENDING'],
        'Description': ['Premium widget', 'Standard gadget', 'Test product salmon', 'Demo tool', 'High-end widget'],
        'Tags': ['premium,wireless', 'standard', 'test,demo', 'basic', 'premium,wireless,waterproof'],
        'Filename': ['data.xlsx', 'report.pdf', 'test.csv', 'demo.txt', 'final.xlsx']
    })


def setup_test_stages():
    """Setup test stages for stage-based filtering."""
    StageManager.initialize_stages()
    
    # Valid categories stage
    valid_categories = pd.DataFrame({
        'Category': ['Electronics', 'Tools', 'Software']
    })
    StageManager.save_stage('valid_categories', valid_categories)
    
    # Price history stage for comparisons
    price_history = pd.DataFrame({
        'Product_ID': ['PROD-001', 'PROD-002', 'PROD-005'],
        'Historical_Price': [12.00, 20.00, 25.00]
    })
    StageManager.save_stage('price_history', price_history)


def test_backward_compatibility():
    """Test that all existing functionality still works."""
    print("\n=== Testing Backward Compatibility ===")
    
    test_df = create_test_data()
    success = True
    
    # Test basic equals
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Status', 'condition': 'equals', 'value': 'Active'}]
        })
        result = processor.execute(test_df)
        # Should match case-insensitively by default: Active, ACTIVE, active
        if len(result) == 3:
            print("✓ Basic equals (case-insensitive by default)")
        else:
            print(f"✗ Basic equals failed: expected 3, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Basic equals crashed: {e}")
        success = False
    
    # Test original in_list - FIXED: should be 5, not 4
    # Data: ['Electronics', 'ELECTRONICS', 'electronics', 'Tools', 'Electronics']
    # Filter: ['Electronics', 'Tools'] - all 5 rows match with case-insensitive
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Category', 'condition': 'in_list', 'value': ['Electronics', 'Tools']}]
        })
        result = processor.execute(test_df)
        # Should match case-insensitively: all 5 rows (4 electronics variants + 1 tools)
        if len(result) == 5:
            print("✓ Original in_list (case-insensitive by default)")
        else:
            print(f"✗ Original in_list failed: expected 5, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Original in_list crashed: {e}")
        success = False
    
    # Test numeric comparison
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Price', 'condition': 'greater_than', 'value': 20.0}]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # 25.00 and 30.00
            print("✓ Numeric comparison")
        else:
            print(f"✗ Numeric comparison failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Numeric comparison crashed: {e}")
        success = False
    
    return success


def test_case_sensitivity():
    """Test case sensitivity controls."""
    print("\n=== Testing Case Sensitivity ===")
    
    test_df = create_test_data()
    success = True
    
    # Test case-insensitive (default)
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Status', 'condition': 'equals', 'value': 'active'}]
        })
        result = processor.execute(test_df)
        if len(result) == 3:  # Should match Active, ACTIVE, active
            print("✓ Case-insensitive equals (default)")
        else:
            print(f"✗ Case-insensitive equals failed: expected 3, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Case-insensitive equals crashed: {e}")
        success = False
    
    # Test case-sensitive (explicit)
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Status', 'condition': 'equals', 'value': 'Active', 'case_sensitive': True}]
        })
        result = processor.execute(test_df)
        if len(result) == 1:  # Should only match exact 'Active'
            print("✓ Case-sensitive equals (explicit)")
        else:
            print(f"✗ Case-sensitive equals failed: expected 1, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Case-sensitive equals crashed: {e}")
        success = False
    
    return success


def test_new_individual_conditions():
    """Test new individual pattern conditions."""
    print("\n=== Testing New Individual Conditions ===")
    
    test_df = create_test_data()
    success = True
    
    # Test starts_with
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Product_ID', 'condition': 'starts_with', 'value': 'prod'}]
        })
        result = processor.execute(test_df)
        if len(result) == 3:  # Should match PROD-001, PROD-002, PROD-005 (case-insensitive)
            print("✓ starts_with condition")
        else:
            print(f"✗ starts_with failed: expected 3, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ starts_with crashed: {e}")
        success = False
    
    # Test ends_with
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Filename', 'condition': 'ends_with', 'value': '.xlsx'}]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # data.xlsx, final.xlsx
            print("✓ ends_with condition")
        else:
            print(f"✗ ends_with failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ ends_with crashed: {e}")
        success = False
    
    # Test not_starts_with
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Product_ID', 'condition': 'not_starts_with', 'value': 'PROD'}]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # TEST-003, DEMO-004
            print("✓ not_starts_with condition")
        else:
            print(f"✗ not_starts_with failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ not_starts_with crashed: {e}")
        success = False
    
    return success


def test_enhanced_list_conditions():
    """Test new enhanced list conditions."""
    print("\n=== Testing Enhanced List Conditions ===")
    
    test_df = create_test_data()
    success = True
    
    # Test contains_any_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Description', 'condition': 'contains_any_in_list', 'value': ['premium', 'standard']}]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # Premium widget, Standard gadget
            print("✓ contains_any_in_list condition")
        else:
            print(f"✗ contains_any_in_list failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ contains_any_in_list crashed: {e}")
        success = False
    
    # Test not_contains_any_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Description', 'condition': 'not_contains_any_in_list', 'value': ['test', 'demo']}]
        })
        result = processor.execute(test_df)
        if len(result) == 3:  # Should exclude Test product and Demo tool
            print("✓ not_contains_any_in_list condition")
        else:
            print(f"✗ not_contains_any_in_list failed: expected 3, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ not_contains_any_in_list crashed: {e}")
        success = False
    
    # Test contains_all_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Tags', 'condition': 'contains_all_in_list', 'value': ['premium', 'wireless']}]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # Should match rows with BOTH premium AND wireless
            print("✓ contains_all_in_list condition")
        else:
            print(f"✗ contains_all_in_list failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ contains_all_in_list crashed: {e}")
        success = False
    
    # Test starts_with_any_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Product_ID', 'condition': 'starts_with_any_in_list', 'value': ['TEST', 'DEMO']}]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # TEST-003, DEMO-004
            print("✓ starts_with_any_in_list condition")
        else:
            print(f"✗ starts_with_any_in_list failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ starts_with_any_in_list crashed: {e}")
        success = False
    
    return success


def test_numeric_list_conditions():
    """Test new numeric list conditions with explicit min/max naming."""
    print("\n=== Testing Numeric List Conditions ===")
    
    test_df = create_test_data()
    success = True
    
    # Test greater_than_min_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Price', 'condition': 'greater_than_min_in_list', 'value': [15, 20, 25]}]
        })
        result = processor.execute(test_df)
        if len(result) == 3:  # Should be > 15 (min): 25.00, 15.75, 30.00
            print("✓ greater_than_min_in_list condition")
        else:
            print(f"✗ greater_than_min_in_list failed: expected 3, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ greater_than_min_in_list crashed: {e}")
        success = False
    
    # Test greater_than_max_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Price', 'condition': 'greater_than_max_in_list', 'value': [15, 20, 25]}]
        })
        result = processor.execute(test_df)
        if len(result) == 1:  # Should be > 25 (max): only 30.00
            print("✓ greater_than_max_in_list condition")
        else:
            print(f"✗ greater_than_max_in_list failed: expected 1, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ greater_than_max_in_list crashed: {e}")
        success = False
    
    # Test less_than_max_in_list - FIXED: should be 3, not 4
    # Data: [10.50, 25.00, 15.75, 8.25, 30.00]
    # Filter: less_than_max_in_list with [15, 20, 25] (max = 25)
    # Only values < 25: 10.50, 15.75, 8.25 = 3 items (25.00 is NOT < 25)
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Price', 'condition': 'less_than_max_in_list', 'value': [15, 20, 25]}]
        })
        result = processor.execute(test_df)
        if len(result) == 3:  # Should be < 25 (max): 10.50, 15.75, 8.25 (NOT 25.00)
            print("✓ less_than_max_in_list condition")
        else:
            print(f"✗ less_than_max_in_list failed: expected 3, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ less_than_max_in_list crashed: {e}")
        success = False
    
    return success


def test_stage_based_filtering():
    """Test stage-based filtering with case sensitivity."""
    print("\n=== Testing Stage-Based Filtering ===")
    
    setup_test_stages()
    test_df = create_test_data()
    success = True
    
    # Test in_stage (case-insensitive by default) - FIXED: should be 5, not 4
    # Valid categories: ['Electronics', 'Tools', 'Software']
    # Test data categories: ['Electronics', 'ELECTRONICS', 'electronics', 'Tools', 'Electronics'] 
    # With case-insensitive matching: all 5 rows match (4 electronics + 1 tools)
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{
                'column': 'Category',
                'condition': 'in_stage',
                'stage_name': 'valid_categories',
                'stage_column': 'Category'
            }]
        })
        result = processor.execute(test_df)
        if len(result) == 5:  # Should match all Electronics variants + Tools
            print("✓ in_stage condition (case-insensitive)")
        else:
            print(f"✗ in_stage failed: expected 5, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ in_stage crashed: {e}")
        success = False
    
    # Test stage_comparison
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{
                'column': 'Price',
                'condition': 'stage_comparison',
                'stage_name': 'price_history',
                'key_column': 'Product_ID',
                'stage_key_column': 'Product_ID',
                'stage_value_column': 'Historical_Price',
                'comparison_operator': 'less_than'
            }]
        })
        result = processor.execute(test_df)
        # PROD-001: 10.50 < 12.00 ✓, PROD-002: 25.00 < 20.00 ✗, PROD-005: 30.00 < 25.00 ✗
        if len(result) == 1:
            print("✓ stage_comparison condition")
        else:
            print(f"✗ stage_comparison failed: expected 1, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ stage_comparison crashed: {e}")
        success = False
    
    return success


def test_error_handling():
    """Test error handling and validation."""
    print("\n=== Testing Error Handling ===")
    
    test_df = create_test_data()
    success = True
    
    # Test missing column
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'NonExistent', 'condition': 'equals', 'value': 'test'}]
        })
        processor.execute(test_df)
        print("✗ Should have failed with missing column")
        success = False
    except StepProcessorError:
        print("✓ Properly caught missing column")
    except Exception as e:
        print(f"✗ Wrong exception type: {e}")
        success = False
    
    # Test invalid condition
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Status', 'condition': 'invalid_condition', 'value': 'test'}]
        })
        processor.execute(test_df)
        print("✗ Should have failed with invalid condition")
        success = False
    except StepProcessorError:
        print("✓ Properly caught invalid condition")
    except Exception as e:
        print(f"✗ Wrong exception type: {e}")
        success = False
    
    return success


def test_multiple_filters():
    """Test multiple filters working together."""
    print("\n=== Testing Multiple Filters ===")
    
    test_df = create_test_data()
    success = True
    
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [
                {'column': 'Category', 'condition': 'equals', 'value': 'electronics'},
                {'column': 'Price', 'condition': 'greater_than', 'value': 20.0}
            ]
        })
        result = processor.execute(test_df)
        if len(result) == 2:  # Electronics + Price > 20: GADGET beta (25.00), WIDGET gamma (30.00)
            print("✓ Multiple filters working together")
        else:
            print(f"✗ Multiple filters failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Multiple filters crashed: {e}")
        success = False
    
    return success


def test_additional_coverage():
    """Test additional conditions and edge cases for better coverage."""
    print("\n=== Testing Additional Coverage ===")
    
    test_df = create_test_data()
    success = True
    
    # Test not_in_list
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [{'column': 'Status', 'condition': 'not_in_list', 'value': ['Active', 'ACTIVE']}]
        })
        result = processor.execute(test_df)
        # With case-insensitive (default): excludes Active, ACTIVE, active -> leaves Inactive, PENDING
        if len(result) == 2:
            print("✓ not_in_list condition")
        else:
            print(f"✗ not_in_list failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ not_in_list crashed: {e}")
        success = False
    
    # Test greater_equal and less_equal
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': [
                {'column': 'Price', 'condition': 'greater_equal', 'value': 15.75},
                {'column': 'Price', 'condition': 'less_equal', 'value': 25.00}
            ]
        })
        result = processor.execute(test_df)
        # Should include: 15.75, 25.00 
        if len(result) == 2:
            print("✓ greater_equal and less_equal conditions")
        else:
            print(f"✗ greater_equal/less_equal failed: expected 2, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ greater_equal/less_equal crashed: {e}")
        success = False
    
    # Test empty filter handling
    try:
        processor = FilterDataProcessor({
            'processor_type': 'filter_data',
            'filters': []
        })
        result = processor.execute(test_df)
        # Should return all data unchanged when no filters
        if len(result) == len(test_df):
            print("✓ Empty filter list handling")
        else:
            print(f"✗ Empty filter failed: expected {len(test_df)}, got {len(result)}")
            success = False
    except Exception as e:
        print(f"✗ Empty filter crashed: {e}")
        success = False
    
    return success


def main():
    """Run all tests and report results."""
    print("🧪 COMPREHENSIVE FILTER DATA PROCESSOR TEST")
    print("=" * 60)
    
    tests = [
        test_backward_compatibility,
        test_case_sensitivity,
        test_new_individual_conditions,
        test_enhanced_list_conditions,
        test_numeric_list_conditions,
        test_stage_based_filtering,
        test_error_handling,
        test_multiple_filters,
        test_additional_coverage
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_func.__name__} had failures")
        except Exception as e:
            print(f"💥 {test_func.__name__} crashed: {e}")
        finally:
            # Clean up stages after each test
            try:
                StageManager.cleanup_stages()
            except:
                pass
    
    print("\n" + "=" * 60)
    print(f"📊 TEST RESULTS: {passed}/{total} test suites passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! Filter processor is working correctly.")
        print("\n✅ Verified:")
        print("  • Backward compatibility maintained")
        print("  • Case sensitivity working (default: case-insensitive)")
        print("  • New individual conditions working")
        print("  • Enhanced list conditions working")
        print("  • Numeric list conditions working")
        print("  • Stage-based filtering enhanced")
        print("  • Error handling preserved")
        print("  • Multiple filters working together")
        print("  • Additional edge cases covered")
        return 0
    else:
        print("❌ Some tests failed! Check the filter processor implementation.")
        return 1


if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)


# End of file #
