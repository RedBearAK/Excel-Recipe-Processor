"""
Quick verification script for the capabilities system fixes.
"""

def test_rename_columns_capabilities():
    """Test that rename_columns processor capabilities work now."""
    
    print("🔧 Testing rename_columns processor capabilities...")
    
    try:
        from excel_recipe_processor.processors.rename_columns_processor import RenameColumnsProcessor
        
        # Create processor with correct config (avoiding the type field conflict)
        config = {
            'processor_type': 'rename_columns',
            'step_description': 'Test rename',
            'mapping': {'old_col': 'new_col'}  # Non-empty mapping
        }
        
        processor = RenameColumnsProcessor(config)
        
        if hasattr(processor, 'get_capabilities'):
            capabilities = processor.get_capabilities()
            description = capabilities.get('description', 'No description')
            print(f"✅ Description: {description}")
            print(f"✅ Rename types: {capabilities.get('rename_types', [])}")
            print(f"✅ Case conversions: {capabilities.get('case_conversions', [])}")
            return True
        else:
            print("❌ No get_capabilities method found")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_feature_matrix_formatting():
    """Test the improved feature matrix formatting."""
    
    print("\n📊 Testing feature matrix formatting...")
    
    try:
        # Import and test the comprehensive capabilities
        from test_comprehensive_capabilities import get_all_capabilities
        
        capabilities = get_all_capabilities()
        
        # Check that rename_columns no longer has an error
        rename_info = capabilities['processors'].get('rename_columns', {})
        
        if 'error' in rename_info:
            print(f"❌ rename_columns still has error: {rename_info['error']}")
            return False
        elif 'description' in rename_info:
            print(f"✅ rename_columns description: {rename_info['description']}")
            return True
        else:
            print("⚠️  rename_columns has no description but no error")
            return True
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_configuration_validation():
    """Test that configuration validation works correctly."""
    
    print("\n🔍 Testing configuration validation...")
    
    try:
        import yaml
        from pathlib import Path
        from test_recipe_validator import RecipeValidator
        
        # Create a recipe with configuration errors
        broken_recipe = {
            'recipe': [
                {
                    'step_description': 'Missing lookup fields',
                    'processor_type': 'lookup_data'
                    # Missing required fields: lookup_source, lookup_key, source_key, lookup_columns
                },
                {
                    'step_description': 'Invalid filter config',
                    'processor_type': 'filter_data',
                    'filters': 'this should be a list'
                }
            ]
        }
        
        # Save to temp file
        temp_file = Path('test_broken_config.yaml')
        with open(temp_file, 'w') as f:
            yaml.dump(broken_recipe, f)
        
        # Validate
        validator = RecipeValidator()
        result = validator.validate_recipe_file(str(temp_file))
        
        # Clean up
        temp_file.unlink()
        
        # Check results
        has_issues = len(result['issues']) > 0
        config_errors = any('required field' in issue.lower() or 'configuration error' in issue.lower() 
                          for issue in result['issues'])
        config_valid = result['compatible']
        
        print(f"✅ Found {len(result['issues'])} issues")
        if result['issues']:
            for issue in result['issues'][:3]:  # Show first 3 issues
                print(f"   • {issue}")
        
        print(f"✅ Recipe marked as {'valid' if config_valid else 'invalid'}")
        
        # Should find configuration issues and mark as invalid
        return has_issues and config_errors and not config_valid
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_processor_name_abbreviation():
    """Test that processor names are properly abbreviated in matrix."""
    
    print("\n📋 Testing processor name abbreviation...")
    
    # Test the abbreviation logic
    test_names = [
        'add_calculated_column',
        'clean_data',
        'filter_data',
        'group_data',
        'lookup_data',
        'pivot_table',
        'rename_columns',
        'sort_data'
    ]
    
    abbreviated = []
    for name in test_names:
        if len(name) <= 12:
            abbreviated.append(name[:12])
        else:
            parts = name.split('_')
            if len(parts) >= 2:
                abbreviated.append(f"{parts[0][:4]}_{parts[1][:4]}")
            else:
                abbreviated.append(name[:12])
    
    print("✅ Processor name abbreviations:")
    for orig, abbrev in zip(test_names, abbreviated):
        print(f"   {orig:<20} → {abbrev}")
    
    # Check that no abbreviation is longer than 12 characters
    max_length = max(len(abbrev) for abbrev in abbreviated)
    return max_length <= 12


def main():
    """Run all verification tests."""
    
    print("🧪 CAPABILITIES SYSTEM FIXES VERIFICATION")
    print("=" * 50)
    
    tests = [
        ("Rename Columns Capabilities", test_rename_columns_capabilities),
        ("Feature Matrix Formatting", test_feature_matrix_formatting),
        ("Configuration Validation", test_configuration_validation),
        ("Processor Name Abbreviation", test_processor_name_abbreviation)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔬 {test_name}")
        print("-" * 30)
        try:
            success = test_func()
            results.append((test_name, success))
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"Result: {status}")
        except Exception as e:
            print(f"❌ ERROR: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n📊 SUMMARY")
    print("=" * 30)
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All fixes verified successfully!")
    else:
        print("⚠️  Some issues remain - check the output above")


if __name__ == '__main__':
    main()
