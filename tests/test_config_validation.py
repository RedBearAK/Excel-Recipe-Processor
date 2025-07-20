"""
Quick test to verify the enhanced configuration validation works.
"""

import yaml

from pathlib import Path


def test_enhanced_validation():
    """Test that the enhanced validation catches configuration errors."""
    
    print("🔍 Testing Enhanced Configuration Validation")
    print("=" * 50)
    
    # Create a recipe with obvious configuration errors
    broken_recipe = {
        'recipe': [
            {
                'step_description': 'Lookup with missing fields',
                'processor_type': 'lookup_data'
                # Missing: lookup_source, lookup_key, source_key, lookup_columns
            },
            {
                'step_description': 'Filter with wrong type',
                'processor_type': 'filter_data',
                'filters': 'this should be a list, not a string'
            },
            {
                'step_description': 'Clean with missing rules',
                'processor_type': 'clean_data'
                # Missing: rules
            },
            {
                'step_description': 'Group with wrong types',
                'processor_type': 'group_data',
                'source_column': 'Origin',
                'groups': 'this should be a dict, not a string'
            }
        ]
    }
    
    # Save to temp file
    temp_file = Path('test_validation.yaml')
    with open(temp_file, 'w') as f:
        yaml.dump(broken_recipe, f)
    
    try:
        # Test with the enhanced validator
        from test_recipe_validator import RecipeValidator
        
        validator = RecipeValidator()
        result = validator.validate_recipe_file(str(temp_file))
        
        print(f"📊 Validation Results:")
        print(f"   Total steps: {result['total_steps']}")
        print(f"   Valid steps: {result['valid_steps']}")
        print(f"   Compatible: {result['compatible']}")
        print(f"   Issues found: {len(result['issues'])}")
        
        print(f"\n❌ Issues Found:")
        for i, issue in enumerate(result['issues'], 1):
            print(f"   {i}. {issue}")
        
        print(f"\n📋 Step Details:")
        for step in result['step_details']:
            status = "✅" if step['config_valid'] else "❌"
            print(f"   {status} Step {step['step_number']}: {step['step_name']}")
            if step['issues']:
                for issue in step['issues']:
                    print(f"      ⚠️  {issue}")
        
        # Check if validation worked
        has_config_errors = len(result['issues']) > 0
        marked_invalid = not result['compatible']
        
        print(f"\n✅ Test Results:")
        print(f"   Found configuration errors: {has_config_errors}")
        print(f"   Recipe marked as invalid: {marked_invalid}")
        
        if has_config_errors and marked_invalid:
            print("   🎉 Enhanced validation is working correctly!")
            return True
        else:
            print("   ⚠️  Validation may need further improvement")
            return False
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False
    
    finally:
        # Clean up
        if temp_file.exists():
            temp_file.unlink()


def test_valid_recipe():
    """Test that a valid recipe passes validation."""
    
    print(f"\n" + "="*50)
    print("🔍 Testing Valid Recipe")
    print("=" * 50)
    
    # Create a properly configured recipe
    valid_recipe = {
        'recipe': [
            {
                'step_description': 'Filter products',
                'processor_type': 'filter_data',
                'filters': [
                    {
                        'column': 'Product_Name',
                        'condition': 'contains',
                        'value': 'CANNED'
                    }
                ]
            },
            {
                'step_description': 'Lookup details',
                'processor_type': 'lookup_data',
                'lookup_source': 'products.xlsx',
                'lookup_key': 'Code',
                'source_key': 'Product_Code',
                'lookup_columns': ['Category', 'Price']
            }
        ]
    }
    
    temp_file = Path('test_valid.yaml')
    with open(temp_file, 'w') as f:
        yaml.dump(valid_recipe, f)
    
    try:
        from test_recipe_validator import RecipeValidator
        
        validator = RecipeValidator()
        result = validator.validate_recipe_file(str(temp_file))
        
        print(f"📊 Valid Recipe Results:")
        print(f"   Compatible: {result['compatible']}")
        print(f"   Issues: {len(result['issues'])}")
        print(f"   Valid steps: {result['valid_steps']}/{result['total_steps']}")
        
        if result['compatible'] and len(result['issues']) == 0:
            print("   ✅ Valid recipe correctly identified as valid")
            return True
        else:
            print(f"   ⚠️  Valid recipe had unexpected issues: {result['issues']}")
            return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    finally:
        if temp_file.exists():
            temp_file.unlink()


if __name__ == '__main__':
    print("🧪 CONFIGURATION VALIDATION TEST")
    print("="*50)
    
    test1_passed = test_enhanced_validation()
    test2_passed = test_valid_recipe()
    
    print(f"\n📊 FINAL RESULTS")
    print("="*30)
    if test1_passed and test2_passed:
        print("🎉 All validation tests passed!")
        print("   The enhanced configuration validation is working correctly.")
    else:
        print("⚠️  Some validation tests failed:")
        print(f"   Enhanced validation: {'✅' if test1_passed else '❌'}")
        print(f"   Valid recipe test: {'✅' if test2_passed else '❌'}")
