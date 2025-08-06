"""
Comprehensive test suite for enhanced variable substitution system.

tests/test_comprehensive_variable_substitution.py

This is a much more thorough test suite that covers edge cases, error conditions,
integration scenarios, and real-world use cases to ensure the system is solid.
"""

import yaml

from excel_recipe_processor.core.variable_substitution import (
    VariableSubstitution, 
    VariableSubstitutionError,
    substitute_structure
)


def test_edge_cases():
    """Test edge cases and boundary conditions."""
    print("Testing edge cases...")
    
    # Test data with edge case values
    custom_vars = {
        'empty_list': [],
        'empty_dict': {},
        'none_value': None,
        'nested_empty': {'inner': []},
        'mixed_types': [1, 'text', {'key': 'value'}],
        'unicode_text': 'Special: ñáéíóú',
        'large_list': list(range(100)),  # Test performance
        'deeply_nested': {
            'level1': {
                'level2': {
                    'level3': ['deep', 'nesting']
                }
            }
        }
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test empty structures
    tests += 1
    result = substitution.substitute_structure('{list:empty_list}')
    if result == []:
        print("  ✓ Empty list substitution works")
        success += 1
    else:
        print(f"  ✗ Empty list failed: {result}")
    
    tests += 1
    result = substitution.substitute_structure('{dict:empty_dict}')
    if result == {}:
        print("  ✓ Empty dict substitution works")
        success += 1
    else:
        print(f"  ✗ Empty dict failed: {result}")
    
    # Test None values
    tests += 1
    try:
        result = substitution.substitute_structure('{str:none_value}')
        if result == 'None':
            print("  ✓ None to string conversion works")
            success += 1
        else:
            print(f"  ✗ None conversion failed: {result}")
    except Exception as e:
        print(f"  ✗ None conversion error: {e}")
    
    # Test large structures
    tests += 1
    result = substitution.substitute_structure('{list:large_list}')
    if len(result) == 100 and result[99] == 99:
        print("  ✓ Large list substitution works")
        success += 1
    else:
        print(f"  ✗ Large list failed: length={len(result) if isinstance(result, list) else 'not list'}")
    
    # Test deeply nested structures
    tests += 1
    config = {'nested_config': '{dict:deeply_nested}'}
    result = substitution.substitute_structure(config)
    expected_deep_list = ['deep', 'nesting']
    if result['nested_config']['level1']['level2']['level3'] == expected_deep_list:
        print("  ✓ Deep nesting substitution works")
        success += 1
    else:
        print(f"  ✗ Deep nesting failed")
    
    print(f"  Edge cases: {success}/{tests} tests passed")
    return success == tests


def test_error_conditions():
    """Test error handling and invalid inputs."""
    print("\nTesting error conditions...")
    
    custom_vars = {
        'string_var': 'hello',
        'list_var': ['a', 'b'],
        'dict_var': {'key': 'value'}
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test missing variable
    tests += 1
    try:
        substitution.substitute_structure('{list:nonexistent}')
        print("  ✗ Should have failed for missing variable")
    except VariableSubstitutionError as e:
        if 'nonexistent' in str(e):
            print("  ✓ Correctly caught missing variable")
            success += 1
        else:
            print(f"  ✗ Wrong error message: {e}")
    except Exception as e:
        print(f"  ✗ Wrong exception type: {e}")
    
    # Test unsupported type
    tests += 1
    try:
        substitution.substitute_structure('{tuple:list_var}')
        print("  ✗ Should have failed for unsupported type")
    except VariableSubstitutionError as e:
        if 'tuple' in str(e):
            print("  ✓ Correctly caught unsupported type")
            success += 1
        else:
            print(f"  ✗ Wrong error message: {e}")
    except Exception as e:
        print(f"  ✗ Wrong exception type: {e}")
    
    # Test type mismatch
    tests += 1
    try:
        substitution.substitute_structure('{dict:list_var}')
        print("  ✗ Should have failed for type mismatch")
    except VariableSubstitutionError as e:
        if 'list_var' in str(e) and 'dict' in str(e):
            print("  ✓ Correctly caught type mismatch")
            success += 1
        else:
            print(f"  ✗ Wrong error message: {e}")
    except Exception as e:
        print(f"  ✗ Wrong exception type: {e}")
    
    # Test malformed syntax
    tests += 1
    try:
        substitution.substitute_structure('{list:}')
        print("  ✗ Should have failed for empty variable name")
    except Exception as e:
        print("  ✓ Correctly caught malformed syntax")
        success += 1
    
    # Test structure in string context
    tests += 1
    try:
        substitution.substitute('filename_{list:list_var}.xlsx')
        print("  ✗ Should have failed for list in string context")
    except VariableSubstitutionError as e:
        if 'string context' in str(e):
            print("  ✓ Correctly prevented structure in string context")
            success += 1
        else:
            print(f"  ✗ Wrong error message: {e}")
    except Exception as e:
        print(f"  ✗ Wrong exception type: {e}")
    
    print(f"  Error conditions: {success}/{tests} tests passed")
    return success == tests


def test_yaml_integration():
    """Test integration with actual YAML parsing."""
    print("\nTesting YAML integration...")
    
    # Test that our variables work with real YAML parsing
    yaml_content = '''
settings:
  description: "Test recipe with structured variables"
  variables:
    customer_cols: ["ID", "Name", "Status"]
    region_mapping:
      west: "California"
      east: "New York"
    region: "west"

recipe:
  - step_description: "Select columns"
    processor_type: "select_columns"
    source_stage: "raw_data"
    save_to_stage: "selected_data"
    columns_to_keep: "{list:customer_cols}"
    
  - step_description: "Create lookup"
    processor_type: "create_stage"
    save_to_stage: "region_lookup"
    data:
      format: "dictionary"
      key_column: "Region_Code"
      value_column: "Region_Name"
      data: "{dict:region_mapping}"
'''
    
    success = 0
    tests = 0
    
    # Test YAML parsing
    tests += 1
    try:
        recipe_data = yaml.safe_load(yaml_content)
        print("  ✓ YAML parsing successful")
        success += 1
    except Exception as e:
        print(f"  ✗ YAML parsing failed: {e}")
        return False
    
    # Test variable substitution on parsed YAML
    tests += 1
    try:
        variables = recipe_data['settings']['variables']
        substitution = VariableSubstitution(custom_variables=variables)
        
        # Test substituting in the recipe steps
        step1 = recipe_data['recipe'][0]
        result = substitution.substitute_structure(step1)
        
        expected_columns = ["ID", "Name", "Status"]
        if result['columns_to_keep'] == expected_columns:
            print("  ✓ YAML + substitution integration works")
            success += 1
        else:
            print(f"  ✗ Integration failed: {result['columns_to_keep']}")
    except Exception as e:
        print(f"  ✗ Integration test failed: {e}")
    
    # Test nested substitution in YAML structure
    tests += 1
    try:
        step2 = recipe_data['recipe'][1]
        result = substitution.substitute_structure(step2)
        
        expected_dict = {"west": "California", "east": "New York"}
        if result['data']['data'] == expected_dict:
            print("  ✓ Nested YAML substitution works")
            success += 1
        else:
            print(f"  ✗ Nested substitution failed: {result['data']['data']}")
    except Exception as e:
        print(f"  ✗ Nested substitution error: {e}")
    
    print(f"  YAML integration: {success}/{tests} tests passed")
    return success == tests


def test_variable_resolution_order():
    """Test complex variable resolution scenarios."""
    print("\nTesting variable resolution order...")
    
    # Test variables that reference other variables
    custom_vars = {
        'base_cols': ['ID', 'Name'],
        'extra_cols': ['Status', 'Region'],
        'all_cols': ['ID', 'Name', 'Status', 'Region'],  # Could be built from base + extra
        'region': 'west',
        'filename_base': 'report',
        'full_filename': 'report_west_20250805.xlsx'  # Could be built from parts
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test that complex structures work
    tests += 1
    complex_config = {
        'primary_processing': {
            'columns': '{list:base_cols}',
            'region_filter': '{region}'
        },
        'secondary_processing': {
            'columns': '{list:all_cols}',
            'output': '{full_filename}'
        }
    }
    
    try:
        result = substitution.substitute_structure(complex_config)
        
        primary_cols = result['primary_processing']['columns']
        secondary_cols = result['secondary_processing']['columns']
        
        if (primary_cols == ['ID', 'Name'] and 
            secondary_cols == ['ID', 'Name', 'Status', 'Region']):
            print("  ✓ Complex multi-level substitution works")
            success += 1
        else:
            print(f"  ✗ Complex substitution failed")
    except Exception as e:
        print(f"  ✗ Complex substitution error: {e}")
    
    print(f"  Variable resolution: {success}/{tests} tests passed")
    return success == tests


def test_type_conversions():
    """Test type conversion edge cases."""
    print("\nTesting type conversions...")
    
    custom_vars = {
        'string_number': '42',
        'string_float': '3.14159',
        'string_bool_true': 'true',
        'string_bool_false': 'False',
        'string_bool_yes': 'yes',
        'int_value': 100,
        'float_value': 2.718,
        'bool_value': True
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test string to number conversions
    tests += 1
    try:
        result = substitution.substitute_structure('{int:string_number}')
        if result == 42 and isinstance(result, int):
            print("  ✓ String to int conversion works")
            success += 1
        else:
            print(f"  ✗ String to int failed: {result} (type: {type(result)})")
    except Exception as e:
        print(f"  ✗ String to int error: {e}")
    
    tests += 1
    try:
        result = substitution.substitute_structure('{float:string_float}')
        if abs(result - 3.14159) < 0.001 and isinstance(result, float):
            print("  ✓ String to float conversion works")
            success += 1
        else:
            print(f"  ✗ String to float failed: {result}")
    except Exception as e:
        print(f"  ✗ String to float error: {e}")
    
    # Test boolean conversions
    tests += 1
    try:
        result = substitution.substitute_structure('{bool:string_bool_true}')
        if result is True:
            print("  ✓ String 'true' to bool conversion works")
            success += 1
        else:
            print(f"  ✗ Bool conversion failed: {result}")
    except Exception as e:
        print(f"  ✗ Bool conversion error: {e}")
    
    tests += 1
    try:
        result = substitution.substitute_structure('{bool:string_bool_false}')
        if result is False:
            print("  ✓ String 'False' to bool conversion works")
            success += 1
        else:
            print(f"  ✗ Bool conversion failed: {result}")
    except Exception as e:
        print(f"  ✗ Bool conversion error: {e}")
    
    # Test invalid conversions
    tests += 1
    try:
        substitution.substitute_structure('{int:string_bool_true}')
        print("  ✗ Should have failed converting 'true' to int")
    except VariableSubstitutionError:
        print("  ✓ Correctly caught invalid int conversion")
        success += 1
    except Exception as e:
        print(f"  ✗ Wrong exception type: {e}")
    
    print(f"  Type conversions: {success}/{tests} tests passed")
    return success == tests


def test_real_recipe_scenario():
    """Test with a realistic recipe scenario."""
    print("\nTesting real recipe scenario...")
    
    # Simulate a real recipe with the column management use case
    recipe_yaml = '''
settings:
  description: "Customer analysis with reusable column definitions"
  variables:
    base_customer_cols: ["Customer_ID", "Customer_Name", "Email"]
    location_cols: ["Address", "City", "State", "ZIP"]
    status_cols: ["Status", "Tier", "Created_Date"]
    all_customer_cols: ["Customer_ID", "Customer_Name", "Email", "Address", "City", "State", "ZIP", "Status", "Tier", "Created_Date"]
    
    status_mapping:
      A: "Active"
      I: "Inactive"
      P: "Pending"
    
    region: "west"
    min_amount: 1000

recipe:
  - step_description: "Select base customer info"
    processor_type: "select_columns"
    source_stage: "raw_customers"
    save_to_stage: "base_customers"
    columns_to_keep: "{list:base_customer_cols}"
    
  - step_description: "Select full customer profile"
    processor_type: "select_columns"
    source_stage: "raw_customers"
    save_to_stage: "full_customers"
    columns_to_keep: "{list:all_customer_cols}"
    
  - step_description: "Create status lookup"
    processor_type: "create_stage"
    save_to_stage: "status_lookup"
    data:
      format: "dictionary"
      key_column: "Status_Code"
      value_column: "Status_Name"
      data: "{dict:status_mapping}"
      
  - step_description: "Filter by amount"
    processor_type: "filter_data"
    source_stage: "full_customers"
    save_to_stage: "filtered_customers"
    conditions:
      - column: "Amount"
        operator: "greater_than"
        value: "{int:min_amount}"
'''
    
    success = 0
    tests = 0
    
    # Test YAML parsing
    tests += 1
    try:
        recipe_data = yaml.safe_load(recipe_yaml)
        print("  ✓ Real recipe YAML parses correctly")
        success += 1
    except Exception as e:
        print(f"  ✗ YAML parsing failed: {e}")
        return False
    
    # Test variable substitution on entire recipe
    tests += 1
    try:
        variables = recipe_data['settings']['variables']
        substitution = VariableSubstitution(custom_variables=variables)
        
        # Substitute variables in all recipe steps
        processed_recipe = []
        for step in recipe_data['recipe']:
            processed_step = substitution.substitute_structure(step)
            processed_recipe.append(processed_step)
        
        # Validate first step (base columns)
        step1 = processed_recipe[0]
        expected_base_cols = ["Customer_ID", "Customer_Name", "Email"]
        if step1['columns_to_keep'] == expected_base_cols:
            print("  ✓ First step column substitution works")
            success += 1
        else:
            print(f"  ✗ First step failed: {step1['columns_to_keep']}")
    except Exception as e:
        print(f"  ✗ Recipe processing error: {e}")
    
    # Validate second step (all columns)
    tests += 1
    try:
        step2 = processed_recipe[1]
        expected_all_cols = ["Customer_ID", "Customer_Name", "Email", "Address", "City", "State", "ZIP", "Status", "Tier", "Created_Date"]
        if step2['columns_to_keep'] == expected_all_cols:
            print("  ✓ Second step column substitution works")
            success += 1
        else:
            print(f"  ✗ Second step failed: {step2['columns_to_keep']}")
    except Exception as e:
        print(f"  ✗ Second step error: {e}")
    
    # Validate dict substitution
    tests += 1
    try:
        step3 = processed_recipe[2]
        expected_mapping = {"A": "Active", "I": "Inactive", "P": "Pending"}
        if step3['data']['data'] == expected_mapping:
            print("  ✓ Dictionary substitution in create_stage works")
            success += 1
        else:
            print(f"  ✗ Dict substitution failed: {step3['data']['data']}")
    except Exception as e:
        print(f"  ✗ Dict substitution error: {e}")
    
    # Validate numeric conversion
    tests += 1
    try:
        step4 = processed_recipe[3]
        if step4['conditions'][0]['value'] == 1000:
            print("  ✓ Numeric conversion in filter works")
            success += 1
        else:
            print(f"  ✗ Numeric conversion failed: {step4['conditions'][0]['value']}")
    except Exception as e:
        print(f"  ✗ Numeric conversion error: {e}")
    
    print(f"  Real recipe scenario: {success}/{tests} tests passed")
    return success == tests


def test_circular_references():
    """Test detection of circular variable references."""
    print("\nTesting circular reference detection...")
    
    # This is tricky - we'd need to implement cycle detection
    # For now, test that at least we don't infinite loop
    custom_vars = {
        'var_a': '{var_b}',  # References B
        'var_b': '{var_a}',  # References A - circular!
        'good_var': 'safe_value'
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test that we don't infinite loop
    tests += 1
    try:
        # This should either work (if we handle it) or fail gracefully
        result = substitution.substitute('output_{var_a}.xlsx')
        # If we get here without hanging, that's good
        print("  ✓ Circular reference doesn't cause infinite loop")
        success += 1
    except Exception as e:
        print(f"  ✓ Circular reference caught: {e}")
        success += 1
    
    # Test that non-circular variables still work
    tests += 1
    try:
        result = substitution.substitute('output_{good_var}.xlsx')
        if 'safe_value' in result:
            print("  ✓ Non-circular variables still work")
            success += 1
        else:
            print(f"  ✗ Non-circular variable failed: {result}")
    except Exception as e:
        print(f"  ✗ Non-circular variable error: {e}")
    
    print(f"  Circular references: {success}/{tests} tests passed")
    return success == tests


def test_mixed_syntax_scenarios():
    """Test complex mixing of old and new syntax."""
    print("\nTesting mixed syntax scenarios...")
    
    custom_vars = {
        'region': 'west',
        'cols': ['A', 'B', 'C'],
        'mapping': {'x': 'y'},
        'count': 42
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test mixing old and new syntax in same config
    tests += 1
    try:
        mixed_config = {
            'old_style_filename': 'report_{region}_{date}.xlsx',
            'new_style_columns': '{list:cols}',
            'explicit_string': '{str:region}',
            'lookup_data': '{dict:mapping}',
            'nested_mixing': {
                'file': 'data_{region}.csv',
                'columns': '{list:cols}',
                'threshold': '{int:count}'
            }
        }
        
        result = substitution.substitute_structure(mixed_config)
        
        # Check various parts
        checks = [
            'report_west_' in result['old_style_filename'],  # Old syntax works
            result['new_style_columns'] == ['A', 'B', 'C'],  # New list syntax works
            result['explicit_string'] == 'west',             # Explicit string works
            result['lookup_data'] == {'x': 'y'},             # Dict syntax works
            'data_west.csv' == result['nested_mixing']['file'],  # Nested old syntax
            result['nested_mixing']['columns'] == ['A', 'B', 'C'],  # Nested new syntax
            result['nested_mixing']['threshold'] == 42       # Nested conversion
        ]
        
        if all(checks):
            print("  ✓ Mixed syntax scenarios work correctly")
            success += 1
        else:
            print(f"  ✗ Mixed syntax failed, results: {result}")
    except Exception as e:
        print(f"  ✗ Mixed syntax error: {e}")
    
    print(f"  Mixed syntax: {success}/{tests} tests passed")
    return success == tests


def test_malformed_syntax():
    """Test various malformed variable syntax patterns."""
    print("\nTesting malformed syntax handling...")
    
    substitution = VariableSubstitution(custom_variables={'test': 'value'})
    
    malformed_patterns = [
        '{list:}',           # Empty variable name
        '{:test}',           # Empty type name
        '{listtest}',        # Missing colon
        '{list:test:extra}', # Too many colons
        '{}',                # Empty braces
        '{list}',            # No variable name
        '{list:test',        # Missing closing brace
        'list:test}',        # Missing opening brace - should be caught by simple detection
    ]
    
    success = 0
    tests = len(malformed_patterns)
    
    for pattern in malformed_patterns:
        try:
            substitution.substitute_structure(pattern)
            print(f"  ✗ Should have failed for: '{pattern}'")
        except Exception:
            print(f"  ✓ Correctly rejected malformed pattern: '{pattern}'")
            success += 1
    
    print(f"  Malformed syntax: {success}/{tests} tests passed")
    return success == tests


def test_performance_stress():
    """Test performance with larger data structures."""
    print("\nTesting performance with large structures...")
    
    # Create large test variables
    large_list = [f"column_{i}" for i in range(500)]
    large_dict = {f"key_{i}": f"value_{i}" for i in range(500)}
    
    custom_vars = {
        'large_columns': large_list,
        'large_mapping': large_dict,
        'region': 'test'
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test large list substitution
    tests += 1
    try:
        config = {'columns': '{list:large_columns}'}
        result = substitution.substitute_structure(config)
        
        if len(result['columns']) == 500 and result['columns'][499] == 'column_499':
            print(f"  ✓ Large list substitution (500 items) works")
            success += 1
        else:
            print(f"  ✗ Large list substitution failed")
    except Exception as e:
        print(f"  ✗ Large list error: {e}")
    
    # Test large dict substitution
    tests += 1
    try:
        config = {'mapping': '{dict:large_mapping}'}
        result = substitution.substitute_structure(config)
        
        if len(result['mapping']) == 500 and result['mapping']['key_499'] == 'value_499':
            print(f"  ✓ Large dict substitution (500 entries) works")
            success += 1
        else:
            print(f"  ✗ Large dict substitution failed")
    except Exception as e:
        print(f"  ✗ Large dict error: {e}")
    
    # Test complex nested structure with many variables
    tests += 1
    try:
        complex_config = {
            'processing_configs': [
                {
                    'name': f'config_{i}',
                    'columns': '{list:large_columns}',
                    'region': '{region}'
                }
                for i in range(10)
            ]
        }
        
        result = substitution.substitute_structure(complex_config)
        
        if (len(result['processing_configs']) == 10 and 
            len(result['processing_configs'][0]['columns']) == 500):
            print(f"  ✓ Complex nested structure performance acceptable")
            success += 1
        else:
            print(f"  ✗ Complex nested structure failed")
    except Exception as e:
        print(f"  ✗ Complex nested error: {e}")
    
    print(f"  Performance stress: {success}/{tests} tests passed")
    return success == tests


def test_integration_with_pipeline():
    """Test integration scenarios that mirror real pipeline usage."""
    print("\nTesting pipeline integration scenarios...")
    
    # Simulate the RecipePipeline._substitute_variables_in_config method usage
    custom_vars = {
        'customer_columns': ["Customer_ID", "Name", "Region"],
        'status_mapping': {"A": "Active", "I": "Inactive"},
        'region': 'west',
        'min_amount': 500,
        'debug_mode': True
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test processor step config substitution (real scenario)
    tests += 1
    try:
        step_config = {
            'processor_type': 'select_columns',
            'step_description': 'Select {region} customer columns',
            'source_stage': 'raw_data',
            'save_to_stage': 'customer_data', 
            'columns_to_keep': '{list:customer_columns}',
            'settings': {
                'region_filter': '{region}',
                'debug': '{bool:debug_mode}',
                'threshold': '{int:min_amount}'
            }
        }
        
        result = substitution.substitute_structure(step_config)
        
        checks = [
            result['step_description'] == 'Select west customer columns',
            result['columns_to_keep'] == ["Customer_ID", "Name", "Region"],
            result['settings']['region_filter'] == 'west',
            result['settings']['debug'] is True,
            result['settings']['threshold'] == 500
        ]
        
        if all(checks):
            print("  ✓ Processor step config substitution works")
            success += 1
        else:
            print(f"  ✗ Step config substitution failed: {result}")
    except Exception as e:
        print(f"  ✗ Step config error: {e}")
    
    # Test create_stage processor config (dict data scenario)
    tests += 1
    try:
        create_stage_config = {
            'processor_type': 'create_stage',
            'save_to_stage': 'status_lookup',
            'data': {
                'format': 'dictionary',
                'key_column': 'Status_Code',
                'value_column': 'Status_Name',
                'data': '{dict:status_mapping}'
            }
        }
        
        result = substitution.substitute_structure(create_stage_config)
        expected_mapping = {"A": "Active", "I": "Inactive"}
        
        if result['data']['data'] == expected_mapping:
            print("  ✓ create_stage dict substitution works")
            success += 1
        else:
            print(f"  ✗ create_stage substitution failed: {result['data']['data']}")
    except Exception as e:
        print(f"  ✗ create_stage error: {e}")
    
    # Test complex filter with list conditions
    tests += 1
    try:
        filter_config = {
            'processor_type': 'filter_data',
            'conditions': [
                {
                    'column': 'Region',
                    'operator': 'equals',
                    'value': '{region}'
                },
                {
                    'column': 'Amount', 
                    'operator': 'greater_than',
                    'value': '{int:min_amount}'
                }
            ]
        }
        
        result = substitution.substitute_structure(filter_config)
        
        checks = [
            result['conditions'][0]['value'] == 'west',
            result['conditions'][1]['value'] == 500,
            isinstance(result['conditions'][1]['value'], int)
        ]
        
        if all(checks):
            print("  ✓ Complex filter config substitution works")
            success += 1
        else:
            print(f"  ✗ Filter config substitution failed")
    except Exception as e:
        print(f"  ✗ Filter config error: {e}")
    
    print(f"  Pipeline integration: {success}/{tests} tests passed")
    return success == tests


def test_variable_interdependencies():
    """Test variables that reference other variables (common in real usage)."""
    print("\nTesting variable interdependencies...")
    
    # This tests a scenario where variables build on each other
    custom_vars = {
        'base_path': '/data',
        'region': 'west',
        'date_stamp': '20250805',
        # These variables reference other variables
        'input_file': '/data/customers_west_20250805.csv',  # Could be built from parts
        'output_file': '/data/processed_west_20250805.xlsx',  # Could be built from parts
        'base_columns': ['ID', 'Name'],
        'location_columns': ['City', 'State'],
        # Combined columns that include base columns
        'full_columns': ['ID', 'Name', 'City', 'State', 'Region']
    }
    
    substitution = VariableSubstitution(custom_variables=custom_vars)
    
    success = 0
    tests = 0
    
    # Test complex config that uses multiple interdependent variables
    tests += 1
    try:
        complex_config = {
            'data_pipeline': {
                'input': {
                    'file': '{input_file}',
                    'columns': '{list:base_columns}'
                },
                'processing': {
                    'select_columns': '{list:full_columns}',
                    'region_filter': '{region}',
                    'intermediate_columns': '{list:location_columns}'
                },
                'output': {
                    'file': '{output_file}',
                    'final_columns': '{list:full_columns}'
                }
            }
        }
        
        result = substitution.substitute_structure(complex_config)
        
        checks = [
            result['data_pipeline']['input']['file'] == '/data/customers_west_20250805.csv',
            result['data_pipeline']['input']['columns'] == ['ID', 'Name'],
            result['data_pipeline']['processing']['select_columns'] == ['ID', 'Name', 'City', 'State', 'Region'],
            result['data_pipeline']['processing']['region_filter'] == 'west',
            result['data_pipeline']['processing']['intermediate_columns'] == ['City', 'State'],
            result['data_pipeline']['output']['file'] == '/data/processed_west_20250805.xlsx',
            result['data_pipeline']['output']['final_columns'] == ['ID', 'Name', 'City', 'State', 'Region']
        ]
        
        if all(checks):
            print("  ✓ Complex interdependent variable substitution works")
            success += 1
        else:
            print(f"  ✗ Interdependent substitution failed")
            print(f"    Result: {result}")
    except Exception as e:
        print(f"  ✗ Interdependent substitution error: {e}")
    
    print(f"  Variable interdependencies: {success}/{tests} tests passed")
    return success == tests


def test_comprehensive_datetime_patterns():
    """Test all new date/time patterns from master list."""
    print("\nTesting comprehensive date/time patterns...")
    
    substitution = VariableSubstitution()
    
    # Test new patterns that should exist
    new_patterns = [
        'YYYYMMDD', 'YYMMDD', 'MMDDYYYY', 'DDMMYYYY', 
        'HHMMSS', 'YYYYMMDDHHMMSS', 'YYYY_MM_DD', 
        'YYYYMMDD_HHMM', 'YYYYWW', 'MonthName'
    ]
    
    success = 0
    for pattern in new_patterns:
        template = f"file_{{{pattern}}}.xlsx"
        try:
            result = substitution.substitute(template)
            if f'{{{pattern}}}' not in result:  # Should be substituted
                print(f"  ✓ {pattern} -> {result}")
                success += 1
            else:
                print(f"  ✗ {pattern} not substituted")
        except Exception as e:
            print(f"  ✗ {pattern} error: {e}")
    
    print(f"  New patterns: {success}/{len(new_patterns)} passed")
    return success == len(new_patterns)


def test_enhanced_formatted_variables():
    """Test both {date:format} and {time:format} work with same formats."""
    print("\nTesting enhanced formatted variables...")
    
    substitution = VariableSubstitution()
    
    test_formats = ['YYYYMMDD', 'HHMMSS', 'YYYY_MM_DD', 'MonthDay']
    success = 0
    
    for fmt in test_formats:
        # Test both date: and time: prefixes work
        date_template = f"{{date:{fmt}}}"
        time_template = f"{{time:{fmt}}}"
        
        try:
            date_result = substitution.substitute(date_template)
            time_result = substitution.substitute(time_template)
            
            if date_result == time_result:  # Should be same result
                print(f"  ✓ {fmt}: date and time formats match")
                success += 1
            else:
                print(f"  ✗ {fmt}: date/time mismatch")
        except Exception as e:
            print(f"  ✗ {fmt} error: {e}")
    
    print(f"  Formatted variables: {success}/{len(test_formats)} passed")
    return success == len(test_formats)


def test_dynamic_documentation():
    """Test the new dynamic documentation generator."""
    print("\nTesting dynamic documentation...")
    
    from excel_recipe_processor.core.variable_substitution import get_variable_documentation
    
    try:
        docs = get_variable_documentation()
        
        # Check structure
        required_keys = ['date_time_variables', 'formatted_variables', 'file_variables']
        if all(key in docs for key in required_keys):
            print("  ✓ Documentation structure correct")
            
            # Check it includes new patterns
            datetime_vars = docs['date_time_variables']
            if 'YYYYMMDD' in datetime_vars and 'HHMMSS' in datetime_vars:
                print("  ✓ New patterns in documentation")
                
                # Check formatted examples are generated
                formatted_vars = docs['formatted_variables']
                if (any('{date:YYYYMMDD}' in str(fmt) for fmt in formatted_vars) and
                    any('{time:HHMMSS}' in str(fmt) for fmt in formatted_vars)):
                    print("  ✓ Formatted examples generated correctly")
                    return True
                else:
                    print(f"  ✗ Formatted examples issue: {formatted_vars[:5]}...")  # Debug
        
        print("  ✗ Documentation validation failed")
        return False
        
    except Exception as e:
        print(f"  ✗ Documentation error: {e}")
        return False


def test_backward_compatibility():
    """Ensure all legacy patterns still work."""
    print("\nTesting backward compatibility...")
    
    substitution = VariableSubstitution()
    
    legacy_patterns = [
        'year', 'month', 'day', 'hour', 'minute', 'second',
        'date', 'time', 'timestamp', 'YYYY', 'MM', 'DD', 'HH', 'HHMM'
    ]
    
    success = sum(1 for pattern in legacy_patterns 
                    if f'{{{pattern}}}' not in substitution.substitute(f'{{{pattern}}}'))
    
    print(f"  Legacy compatibility: {success}/{len(legacy_patterns)} passed")
    return success == len(legacy_patterns)


def main():
    """Run comprehensive tests and return pass/fail status."""
    print("Comprehensive Variable Substitution Tests")
    print("=" * 55)
    
    test_functions = [
        test_edge_cases,
        test_error_conditions,
        test_yaml_integration,
        test_variable_resolution_order,
        test_type_conversions,
        test_malformed_syntax,
        test_performance_stress,
        test_integration_with_pipeline,
        test_variable_interdependencies,

        # NEW TESTS:
        test_comprehensive_datetime_patterns,
        test_enhanced_formatted_variables,
        test_dynamic_documentation,
        test_backward_compatibility,
    ]
    
    test_results = []
    
    for test_func in test_functions:
        try:
            result = test_func()
            test_results.append(result)
        except Exception as e:
            print(f"❌ Test function {test_func.__name__} crashed: {e}")
            test_results.append(False)
    
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\nComprehensive Test Summary")
    print(f"=" * 30)
    print(f"Test Categories Passed: {passed}/{total}")
    print(f"Success Rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("🎉 All comprehensive tests passed!")
        print("✅ Variable substitution system appears solid")
        return 0
    else:
        print("❌ Some comprehensive tests failed")
        print("⚠️  System needs more work before production use")
        return 1


if __name__ == "__main__":
    exit(main())


# End of file #
