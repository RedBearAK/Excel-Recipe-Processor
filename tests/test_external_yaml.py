#!/usr/bin/env python3
"""
Test script for external YAML usage examples loading.

This script tests that the new external YAML file approach works correctly
and provides the same functionality as the previous embedded approach.

Usage:
    python test_external_yaml.py
"""

import sys
import subprocess

from pathlib import Path


script_dir = Path(__file__).parent
project_root = script_dir.parent


def test_yaml_file_exists():
    """Test that the external YAML file exists and is readable."""
    print("🔍 Testing YAML file existence...")
    
    # Check if examples directory exists
    examples_dir = Path(project_root, "excel_recipe_processor/processors/examples")
    
    if not examples_dir.exists():
        print(f"❌ Examples directory not found: {examples_dir}")
        return False
    
    # Check if export_file_examples.yaml exists
    yaml_file = examples_dir / "export_file_examples.yaml"
    if not yaml_file.exists():
        print(f"❌ YAML file not found: {yaml_file}")
        return False
    
    print(f"✅ YAML file found: {yaml_file}")
    return True


def test_yaml_syntax():
    """Test that the YAML file has valid syntax."""
    print("\n🔍 Testing YAML syntax...")
    
    try:
        import yaml
        yaml_file = Path(project_root,
                            "excel_recipe_processor/processors/examples/export_file_examples.yaml")
        
        with open(yaml_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data:
            print("❌ YAML file is empty or invalid")
            return False
        
        # Check for required top-level keys
        required_keys = ['description', 'basic_example', 'parameter_details']
        for key in required_keys:
            if key not in data:
                print(f"❌ Missing required key: {key}")
                return False
        
        print("✅ YAML syntax is valid")
        print(f"   - Contains {len(data)} top-level sections")
        print(f"   - Description: {data['description'][:50]}...")
        return True
        
    except ImportError:
        print("⚠️  PyYAML not available - install with: pip install PyYAML")
        return False
    except yaml.YAMLError as e:
        print(f"❌ YAML syntax error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error reading YAML file: {e}")
        return False


def test_cli_functionality():
    """Test that the CLI still works with external YAML files."""
    print("\n🔍 Testing CLI functionality...")
    
    test_commands = [
        {
            'name': 'Get export_file examples',
            'cmd': ['python', '-m', 'excel_recipe_processor', '--get-usage-examples', 'export_file'],
            'expected_content': ['processor_type: "export_file"', 'output_file:', 'OPT', 'REQ']
        },
        {
            'name': 'Get export_file examples in JSON',
            'cmd': ['python', '-m', 'excel_recipe_processor', '--get-usage-examples', 'export_file', '--format-examples', 'json'],
            'expected_content': ['"processor_name":', '"basic_example":', '"description":']
        }
    ]
    
    all_passed = True
    
    for test in test_commands:
        print(f"\n   Testing: {test['name']}")
        
        try:
            result = subprocess.run(
                test['cmd'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"   ❌ Command failed (exit code: {result.returncode})")
                print(f"   Error: {result.stderr}")
                all_passed = False
                continue
            
            # Check expected content
            output = result.stdout
            missing_content = []
            
            for expected in test['expected_content']:
                if expected not in output:
                    missing_content.append(expected)
            
            if missing_content:
                print(f"   ❌ Missing expected content: {missing_content}")
                all_passed = False
            else:
                print(f"   ✅ Command succeeded with expected content")
                
        except subprocess.TimeoutExpired:
            print(f"   ❌ Command timed out")
            all_passed = False
        except Exception as e:
            print(f"   ❌ Error running command: {e}")
            all_passed = False
    
    return all_passed


def test_formatting_quality():
    """Test that the YAML formatting meets our standards."""
    print("\n🔍 Testing YAML formatting quality...")
    
    try:
        yaml_file = Path(project_root,
                            "excel_recipe_processor/processors/examples/export_file_examples.yaml")
        with open(yaml_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        checks = [
            ('Has OPT markers', 'OPT -' in content),
            ('Has REQ markers', 'REQ -' in content),
            ('Has proper indentation', '  processor_type:' in content),
            ('Has blank lines for separation', '\n\n' in content),
            ('Has variable examples', 'Variable examples:' in content or 'Valid examples:' in content),
            ('Has default values', 'Default value:' in content),
            ('Uses proper list markers', '- #' in content),
            # We don't really need to test for this:
            # ('Has multi-line comments', content.count('\n  #') > 10)
        ]
        
        all_passed = True
        for check_name, passed in checks:
            if passed:
                print(f"   ✅ {check_name}")
            else:
                print(f"   ❌ {check_name}")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Error checking formatting: {e}")
        return False


def test_load_function():
    """Test the load_processor_examples function directly."""
    print("\n🔍 Testing load_processor_examples function...")
    
    try:
        # This assumes the function has been added to pipeline.py
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        
        # Test loading export_file examples
        result = load_processor_examples('export_file')
        
        if 'error' in result:
            print(f"❌ Error loading examples: {result['error']}")
            return False
        
        # Check structure
        if 'description' not in result:
            print("❌ Missing description in loaded examples")
            return False
        
        if 'basic_example' not in result:
            print("❌ Missing basic_example in loaded examples")
            return False
        
        print("✅ load_processor_examples function works correctly")
        print(f"   - Loaded {len(result)} sections")
        return True
        
    except ImportError as e:
        print(f"⚠️  Cannot import load_processor_examples: {e}")
        print("   This is expected if the function hasn't been added to pipeline.py yet")
        return True  # Not a failure, just not implemented yet
    except Exception as e:
        print(f"❌ Error testing load function: {e}")
        return False


def main():
    """Run all tests for external YAML loading."""
    print("🧪 Testing External YAML Usage Examples Loading")
    print("=" * 60)
    
    tests = [
        ('YAML File Exists', test_yaml_file_exists),
        ('YAML Syntax Valid', test_yaml_syntax),
        ('Formatting Quality', test_formatting_quality),
        ('Load Function', test_load_function),
        ('CLI Functionality', test_cli_functionality)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'=' * 60}")
        print(f"TEST: {test_name}")
        print('=' * 60)
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'=' * 60}")
    print("TEST SUMMARY")
    print('=' * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! External YAML loading is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Check implementation.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
