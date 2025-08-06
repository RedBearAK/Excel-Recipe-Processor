"""
Test module for enhanced variable substitution with structured data support.

tests/test_enhanced_variable_substitution.py

Tests the new typed variable substitution system that supports lists, dicts,
and other structured data types alongside the original string substitution.
"""

import json

from excel_recipe_processor.core.variable_substitution import (
    VariableSubstitution, 
    VariableSubstitutionError,
    substitute_variables,
    substitute_structure
)


def test_backwards_compatibility():
    """Test that existing untyped string substitution still works."""
    print("Testing backwards compatibility...")
    
    # Test data
    custom_vars = {
        'region': 'west',
        'batch_id': 'A47',
        'date': '20250805'
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    # Test untyped string substitution (backwards compatible)
    templates = [
        "report_{region}_{date}.xlsx",
        "batch_{batch_id}_data.csv",
        "{region}_{batch_id}_{date}"
    ]
    
    expected = [
        "report_west_20250805.xlsx", 
        "batch_A47_data.csv",
        "west_A47_20250805"
    ]
    
    success = 0
    for template, expected_result in zip(templates, expected):
        result = substitution.substitute(template)
        if result == expected_result:
            print(f"  ✓ '{template}' → '{result}'")
            success += 1
        else:
            print(f"  ✗ '{template}' → '{result}' (expected '{expected_result}')")
    
    print(f"  Backwards compatibility: {success}/{len(templates)} tests passed")
    return success == len(templates)


def test_typed_string_substitution():
    """Test explicit string type substitution."""
    print("\nTesting typed string substitution...")
    
    custom_vars = {
        'filename': 'customer_report',
        'extension': 'xlsx',
        'numeric_value': 42,
        'bool_value': True
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    test_cases = [
        ("{str:filename}.{str:extension}", "customer_report.xlsx"),
        ("count_{str:numeric_value}", "count_42"),
        ("active_{str:bool_value}", "active_True")
    ]
    
    success = 0
    for template, expected in test_cases:
        try:
            result = substitution.substitute(template)
            if result == expected:
                print(f"  ✓ '{template}' → '{result}'")
                success += 1
            else:
                print(f"  ✗ '{template}' → '{result}' (expected '{expected}')")
        except Exception as e:
            print(f"  ✗ '{template}' → Error: {e}")
    
    print(f"  Typed string substitution: {success}/{len(test_cases)} tests passed")
    return success == len(test_cases)


def test_structure_substitution():
    """Test structure substitution for lists and dicts."""
    print("\nTesting structure substitution...")
    
    custom_vars = {
        'customer_columns': ['Customer_ID', 'Name', 'Region', 'Status'],
        'sales_columns': ['Product', 'Amount', 'Date'],
        'status_codes': {
            'active': 'A',
            'inactive': 'I', 
            'pending': 'P'
        },
        'region': 'west'
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    # Test list substitution
    config = {
        'columns_to_keep': '{list:customer_columns}',
        'secondary_columns': '{list:sales_columns}'
    }
    
    result = substitution.substitute_structure(config)
    
    expected_customer_cols = ['Customer_ID', 'Name', 'Region', 'Status']
    expected_sales_cols = ['Product', 'Amount', 'Date']
    
    success = 0
    tests = 0
    
    # Test list substitution
    tests += 1
    if result['columns_to_keep'] == expected_customer_cols:
        print(f"  ✓ List substitution: customer_columns correctly substituted")
        success += 1
    else:
        print(f"  ✗ List substitution failed: {result['columns_to_keep']}")
    
    tests += 1
    if result['secondary_columns'] == expected_sales_cols:
        print(f"  ✓ List substitution: sales_columns correctly substituted")
        success += 1
    else:
        print(f"  ✗ List substitution failed: {result['secondary_columns']}")
    
    # Test dict substitution
    dict_config = {
        'lookup_mapping': '{dict:status_codes}',
        'filter_region': '{region}'  # Mixed with string
    }
    
    dict_result = substitution.substitute_structure(dict_config)
    
    tests += 1
    expected_status_codes = {'active': 'A', 'inactive': 'I', 'pending': 'P'}
    if dict_result['lookup_mapping'] == expected_status_codes:
        print(f"  ✓ Dict substitution: status_codes correctly substituted")
        success += 1
    else:
        print(f"  ✗ Dict substitution failed: {dict_result['lookup_mapping']}")
    
    tests += 1
    if dict_result['filter_region'] == 'west':
        print(f"  ✓ Mixed substitution: string variable works alongside structure")
        success += 1
    else:
        print(f"  ✗ Mixed substitution failed: {dict_result['filter_region']}")
    
    print(f"  Structure substitution: {success}/{tests} tests passed")
    return success == tests


def test_nested_structure_substitution():
    """Test substitution in nested structures."""
    print("\nTesting nested structure substitution...")
    
    custom_vars = {
        'primary_cols': ['ID', 'Name'],
        'backup_cols': ['ID', 'Email'],
        'region': 'east',
        'active_only': True
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    # Complex nested structure
    complex_config = {
        'data_sources': {
            'primary': {
                'columns': '{list:primary_cols}',
                'region': '{region}'
            },
            'backup': {
                'columns': '{list:backup_cols}',
                'active_filter': '{bool:active_only}'
            }
        },
        'processing_steps': [
            {
                'type': 'filter',
                'columns': '{list:primary_cols}',
                'region': '{region}'
            }
        ]
    }
    
    result = substitution.substitute_structure(complex_config)
    
    success = 0
    tests = 0
    
    # Test nested dict substitution
    tests += 1
    if result['data_sources']['primary']['columns'] == ['ID', 'Name']:
        print(f"  ✓ Nested dict: primary columns correctly substituted")
        success += 1
    else:
        print(f"  ✗ Nested dict failed: {result['data_sources']['primary']['columns']}")
    
    tests += 1
    if result['data_sources']['primary']['region'] == 'east':
        print(f"  ✓ Nested dict: region correctly substituted")
        success += 1
    else:
        print(f"  ✗ Nested dict failed: {result['data_sources']['primary']['region']}")
    
    tests += 1
    if result['data_sources']['backup']['active_filter'] is True:
        print(f"  ✓ Nested dict: boolean correctly substituted")
        success += 1
    else:
        print(f"  ✗ Nested dict failed: {result['data_sources']['backup']['active_filter']}")
    
    # Test nested list substitution
    tests += 1
    if result['processing_steps'][0]['columns'] == ['ID', 'Name']:
        print(f"  ✓ Nested list: columns correctly substituted")
        success += 1
    else:
        print(f"  ✗ Nested list failed: {result['processing_steps'][0]['columns']}")
    
    print(f"  Nested structure substitution: {success}/{tests} tests passed")
    return success == tests


def test_type_validation():
    """Test type validation and error handling."""
    print("\nTesting type validation...")
    
    custom_vars = {
        'string_var': 'hello',
        'list_var': ['a', 'b', 'c'],
        'dict_var': {'key': 'value'},
        'int_var': 42
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test successful type conversions
    tests += 1
    try:
        result = substitution.substitute_structure('{int:int_var}')
        if result == 42:
            print(f"  ✓ Type preservation: int correctly preserved")
            success += 1
        else:
            print(f"  ✗ Type preservation failed: {result}")
    except Exception as e:
        print(f"  ✗ Type preservation error: {e}")
    
    # Test type mismatch errors
    tests += 1
    try:
        # Try to use a string as a list
        substitution.substitute_structure('{list:string_var}')
        print(f"  ✗ Type validation: should have failed for list:string_var")
    except VariableSubstitutionError as e:
        print(f"  ✓ Type validation: correctly caught type mismatch")
        success += 1
    except Exception as e:
        print(f"  ✗ Type validation: wrong error type: {e}")
    
    # Test unsupported type
    tests += 1
    try:
        substitution.substitute_structure('{unsupported:string_var}')
        print(f"  ✗ Type validation: should have failed for unsupported type")
    except VariableSubstitutionError as e:
        print(f"  ✓ Type validation: correctly caught unsupported type")
        success += 1
    except Exception as e:
        print(f"  ✗ Type validation: wrong error type: {e}")
    
    # Test string context restrictions
    tests += 1
    try:
        substitution.substitute('output_{list:list_var}.xlsx')
        print(f"  ✗ String context: should have failed for list in string")
    except VariableSubstitutionError as e:
        print(f"  ✓ String context: correctly prevented list in string context")
        success += 1
    except Exception as e:
        print(f"  ✗ String context: wrong error type: {e}")
    
    print(f"  Type validation: {success}/{tests} tests passed")
    return success == tests


def test_convenience_functions():
    """Test module-level convenience functions."""
    print("\nTesting convenience functions...")
    
    custom_vars = {
        'filename': 'report',
        'columns': ['A', 'B', 'C']
    }
    
    success = 0
    tests = 0
    
    # Test substitute_variables function
    tests += 1
    result = substitute_variables('{filename}_{date}.xlsx', custom_variables=custom_vars)
    if 'report_' in result and '.xlsx' in result:
        print(f"  ✓ substitute_variables: works correctly")
        success += 1
    else:
        print(f"  ✗ substitute_variables failed: {result}")
    
    # Test substitute_structure function  
    tests += 1
    config = {'cols': '{list:columns}', 'name': '{filename}'}
    result = substitute_structure(config, custom_variables=custom_vars)
    if result['cols'] == ['A', 'B', 'C'] and result['name'] == 'report':
        print(f"  ✓ substitute_structure: works correctly")
        success += 1
    else:
        print(f"  ✗ substitute_structure failed: {result}")
    
    print(f"  Convenience functions: {success}/{tests} tests passed")
    return success == tests


def main():
    """Run all tests and return pass/fail status."""
    print("Enhanced Variable Substitution Tests")
    print("=" * 50)
    
    test_results = []
    
    test_results.append(test_backwards_compatibility())
    test_results.append(test_typed_string_substitution())
    test_results.append(test_structure_substitution())
    test_results.append(test_nested_structure_substitution())
    test_results.append(test_type_validation())
    test_results.append(test_convenience_functions())
    
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\nTest Summary")
    print(f"=" * 20)
    print(f"Passed: {passed}/{total}")
    print(f"Success Rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    exit(main())


# End of file #
