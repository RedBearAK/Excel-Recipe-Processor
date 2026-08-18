#!/usr/bin/env python3
"""
Test script for the new usage examples CLI functionality.

This script tests the new --get-usage-examples commands and validates
that the implementation works correctly.

Usage:
    python test_usage_examples.py
"""

import sys
import json
import subprocess

from pathlib import Path


def run_command(cmd_args):
    """Run a CLI command and return result."""
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor'] + cmd_args,
            capture_output=True,
            text=True,
            timeout=30
        )
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except subprocess.TimeoutExpired:
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': 'Command timed out'
        }
    except Exception as e:
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': f'Error running command: {e}'
        }

def test_usage_examples_cli():
    """Test the new usage examples CLI functionality."""
    
    print("🧪 Testing Usage Examples CLI Functionality")
    print("=" * 60)
    
    tests = [
        {
            'name': 'Get usage examples for export_file processor',
            'args': ['--get-usage-examples', 'export_file'],
            'expect_success': True,
            'check_content': ['processor_type: "export_file"', 'output_file:', '# REQ -']
        },
        {
            'name': 'Get usage examples for export_file in JSON format',
            'args': ['--get-usage-examples', 'export_file', '--format-examples', 'json'],
            'expect_success': True,
            'check_content': ['"description":', '"basic_example":', '"multi_sheet_example":']
        },
        {
            'name': 'Get usage examples for export_file in text format',
            'args': ['--get-usage-examples', 'export_file', '--format-examples', 'text'],
            'expect_success': True,
            'check_content': ['Processor: export_file', 'Description:', 'Example: basic_example']
        },
        {
            'name': 'Get usage examples for all processors',
            'args': ['--get-usage-examples'],
            'expect_success': True,
            'check_content': ['# Complete Usage Examples', '# Total processors:', 'EXPORT_FILE PROCESSOR']
        },
        {
            'name': 'Get usage examples for all processors in JSON',
            'args': ['--get-usage-examples', '--format-examples', 'json'],
            'expect_success': True,
            'check_content': ['"system_info":', '"processors":', '"total_processors":']
        },
        {
            'name': 'Get usage examples for nonexistent processor',
            'args': ['--get-usage-examples', 'nonexistent_processor'],
            'expect_success': False,
            'check_content': ['No usage examples available', 'Available processors:']
        },
        {
            # Every processor ships examples now; verify one more succeeds
            'name': 'Get usage examples for filter_data processor',
            'args': ['--get-usage-examples', 'filter_data'],
            'expect_success': True,
            'check_content': ['processor_type: "filter_data"', '# REQ -']
        }
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        print(f"\n🔍 {test['name']}")
        print("-" * len(test['name']))
        
        result = run_command(test['args'])
        
        # Check return code
        if test['expect_success']:
            if result['returncode'] == 0:
                print("   ✅ Command succeeded (as expected)")
            else:
                print(f"   ❌ Command failed unexpectedly (exit code: {result['returncode']})")
                print(f"   Error: {result['stderr']}")
                failed += 1
                continue
        else:
            if result['returncode'] != 0:
                print("   ✅ Command failed (as expected)")
            else:
                print("   ❌ Command succeeded unexpectedly")
                failed += 1
                continue
        
        # Check content
        output = result['stdout'] + result['stderr']
        content_checks_passed = True
        
        for expected_content in test['check_content']:
            if expected_content in output:
                print(f"   ✅ Found expected content: '{expected_content[:30]}...'")
            else:
                print(f"   ❌ Missing expected content: '{expected_content}'")
                content_checks_passed = False
        
        if content_checks_passed:
            print("   ✅ All content checks passed")
            passed += 1
        else:
            print("   ❌ Some content checks failed")
            failed += 1
            
        # Show sample output for successful tests
        if result['returncode'] == 0 and len(output) > 0:
            print(f"   📄 Sample output (first 200 chars):")
            print(f"   {output[:200].replace(chr(10), chr(10) + '   ')}...")
    
    print(f"\n📊 TEST RESULTS")
    print("=" * 60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {passed + failed}")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"❌ {failed} tests failed")
        return 1

def validate_yaml_output():
    """Validate that YAML output is properly formatted."""
    print(f"\n🔍 YAML FORMAT VALIDATION")
    print("-" * 30)
    
    result = run_command(['--get-usage-examples', 'export_file'])
    
    if result['returncode'] != 0:
        print("❌ Could not get YAML output for validation")
        return False
    
    yaml_output = result['stdout']
    
    # Basic YAML validation checks
    checks = [
        ('Has comment headers', yaml_output.count('# ') > 5),
        ('Has proper processor_type', 'processor_type: "export_file"' in yaml_output),
        ('Has required parameter', 'output_file:' in yaml_output),
        ('Has proper indentation', '  processor_type:' in yaml_output or '    processor_type:' in yaml_output),
        ('Has step description', 'step_description:' in yaml_output),
        ('Has multiple examples', yaml_output.count('- step_description:') >= 2)
    ]
    
    all_passed = True
    for check_name, passed in checks:
        if passed:
            print(f"   ✅ {check_name}")
        else:
            print(f"   ❌ {check_name}")
            all_passed = False
    
    return all_passed

def demo_usage_examples():
    """Demonstrate the usage examples functionality."""
    print(f"\n🎬 USAGE EXAMPLES DEMO")
    print("=" * 60)
    
    print("1. Getting examples for export_file processor:")
    print("   Command: python -m excel_recipe_processor --get-usage-examples export_file")
    print()
    
    result = run_command(['--get-usage-examples', 'export_file'])
    if result['returncode'] == 0:
        lines = result['stdout'].split('\n')[:20]  # First 20 lines
        for line in lines:
            print(f"   {line}")
        print("   ...")
    else:
        print(f"   ❌ Demo failed: {result['stderr']}")
    
    print(f"\n2. Getting examples for all processors (summary):")
    print("   Command: python -m excel_recipe_processor --get-usage-examples --format-examples text")
    print()
    
    result = run_command(['--get-usage-examples', '--format-examples', 'text'])
    if result['returncode'] == 0:
        lines = result['stdout'].split('\n')[:15]  # First 15 lines
        for line in lines:
            print(f"   {line}")
        print("   ...")
    else:
        print(f"   ❌ Demo failed: {result['stderr']}")

def main():
    """Main test execution."""
    print("🚀 Excel Recipe Processor - Usage Examples CLI Test")
    print("=" * 60)
    print("This script tests the new --get-usage-examples functionality.")
    print()
    
    # Run tests
    test_result = test_usage_examples_cli()
    
    # Validate YAML format
    yaml_valid = validate_yaml_output()
    
    # Show demo
    demo_usage_examples()
    
    # Final summary
    print(f"\n🏁 FINAL RESULTS")
    print("=" * 60)
    if test_result == 0 and yaml_valid:
        print("✅ All tests passed - Usage examples CLI is working correctly!")
        print()
        print("🎯 NEXT STEPS:")
        print("1. Add get_usage_examples() method to remaining processors")
        print("2. Test with actual recipe creation workflows")
        print("3. Update documentation with new CLI commands")
        return 0
    else:
        print("❌ Some tests failed - check implementation")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
