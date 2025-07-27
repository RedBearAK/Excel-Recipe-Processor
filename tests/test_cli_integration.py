#!/usr/bin/env python3
"""
CLI Integration Tests for Excel Recipe Processor

Tests the actual command-line interface, argument parsing,
and CLI-specific functionality that other tests miss.
"""

import sys
import subprocess
import tempfile

from pathlib import Path


def test_cli_help_works():
    """Test that --help works and shows our new arguments."""
    print("Testing CLI help functionality...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--help'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"✗ Help command failed with return code: {result.returncode}")
            return False
        
        help_text = result.stdout
        
        # Check that our new argument is in help
        if '--var' not in help_text:
            print("✗ --var argument not found in help text")
            return False
        
        if 'Override external variable' not in help_text:
            print("✗ --var description not found in help text")
            return False
        
        if '--yaml' not in help_text:
            print("✗ --yaml argument not found in help text")
            return False
        
        if '--detailed-yaml' not in help_text:
            print("✗ --detailed-yaml argument not found in help text")
            return False
        
        print("✓ CLI help works and contains expected arguments")
        return True
        
    except Exception as e:
        print(f"✗ CLI help test failed: {e}")
        return False


def test_cli_version_works():
    """Test that --version works."""
    print("Testing CLI version command...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            print(f"✗ Version command failed with return code: {result.returncode}")
            return False
        
        if 'excel_recipe_processor' not in result.stdout:
            print("✗ Version output doesn't contain package name")
            return False
        
        print("✓ CLI version command works")
        return True
        
    except Exception as e:
        print(f"✗ CLI version test failed: {e}")
        return False


def test_cli_list_capabilities_works():
    """Test that --list-capabilities works without errors."""
    print("Testing CLI list capabilities...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--list-capabilities'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"✗ List capabilities failed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        if 'Excel Recipe Processor' not in result.stdout:
            print("✗ List capabilities output doesn't contain expected header")
            return False
        
        if 'Available Processors' not in result.stdout:
            print("✗ List capabilities output doesn't contain processor list")
            return False
        
        print("✓ CLI list capabilities works")
        return True
        
    except Exception as e:
        print(f"✗ CLI list capabilities test failed: {e}")
        return False


def test_cli_list_capabilities_formats():
    """Test all capability output formats work."""
    print("Testing CLI capability output formats...")
    
    formats = [
        (['--list-capabilities'], 'basic'),
        (['--list-capabilities', '--detailed'], 'detailed'),
        (['--list-capabilities', '--json'], 'json'),
        (['--list-capabilities', '--yaml'], 'yaml'),
        (['--list-capabilities', '--detailed-yaml'], 'detailed-yaml'),
        (['--list-capabilities', '--matrix'], 'matrix'),
    ]
    
    for args, format_name in formats:
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'excel_recipe_processor'] + args,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"✗ Format {format_name} failed with return code: {result.returncode}")
                print(f"Error: {result.stderr}")
                return False
            
            if len(result.stdout) == 0:
                print(f"✗ Format {format_name} produced no output")
                return False
            
            print(f"  ✓ Format {format_name} works")
            
        except Exception as e:
            print(f"✗ Format {format_name} test failed: {e}")
            return False
    
    print("✓ All capability output formats work")
    return True


def test_cli_validate_recipe():
    """Test that --validate-recipe works with a simple recipe."""
    print("Testing CLI recipe validation...")
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
recipe:
  - step_description: "Test step"
    processor_type: "debug_breakpoint"
    message: "Test message"
""")
            recipe_file = f.name
        
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--validate-recipe', recipe_file],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Clean up temp file
        Path(recipe_file).unlink()
        
        if result.returncode != 0:
            print(f"✗ Recipe validation failed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        output_lower = result.stdout.lower()
        if 'validation successful' not in output_lower and 'passed' not in output_lower:
            print("✗ Recipe validation output doesn't indicate success")
            print(f"Output: {result.stdout}")
            return False
        
        print("✓ CLI recipe validation works")
        return True
        
    except Exception as e:
        print(f"✗ CLI recipe validation test failed: {e}")
        return False


def test_cli_missing_required_args():
    """Test that CLI properly handles missing required arguments."""
    print("Testing CLI with no arguments...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            print("✗ No arguments should show help, not error")
            return False
        
        if 'usage:' not in result.stdout.lower() and 'Usage:' not in result.stdout:
            print("✗ No arguments should show help text")
            return False
        
        print("✓ CLI properly shows help when no arguments provided")
        return True
        
    except Exception as e:
        print(f"✗ CLI no arguments test failed: {e}")
        return False


def test_cli_with_var_arguments():
    """Test CLI behavior with --var arguments (without actually processing)."""
    print("Testing CLI with --var arguments...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', 
             'nonexistent.xlsx', '--config', 'nonexistent.yaml',
             '--var', 'batch_id=A47', '--var', 'region=west'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        # Should fail because files don't exist, but NOT because of argument parsing
        if result.returncode == 0:
            print("✗ Should have failed with missing files")
            return False
        
        # Check that it's not an argument conflict error
        combined_output = result.stdout + result.stderr
        if 'conflicting option' in combined_output:
            print("✗ Got argument conflict error - this shouldn't happen")
            return False
        
        # Should be a file error, not argument error
        combined_lower = combined_output.lower()
        if not ('not found' in combined_lower or 
                'no such file' in combined_lower or
                'input file not found' in combined_lower):
            print("✗ Expected file error, got different error")
            print(f"Output: {combined_output}")
            return False
        
        print("✓ CLI handles --var arguments correctly")
        return True
        
    except Exception as e:
        print(f"✗ CLI --var arguments test failed: {e}")
        return False


def test_import_main_module():
    """Test that the main module can be imported without errors."""
    print("Testing main module import...")
    
    try:
        import excel_recipe_processor.__main__
        
        if not hasattr(excel_recipe_processor.__main__, 'main'):
            print("✗ Main module doesn't have main() function")
            return False
        
        print("✓ Main module imports successfully")
        return True
        
    except ImportError as e:
        print(f"✗ Cannot import main module: {e}")
        return False
    except Exception as e:
        print(f"✗ Main module import test failed: {e}")
        return False


def test_cli_argument_conflicts():
    """Test that there are no CLI argument conflicts by trying to import."""
    print("Testing for CLI argument conflicts...")
    
    try:
        # Try to import and create the argument parser
        from excel_recipe_processor.__main__ import main
        
        # The fact that we can import without errors means no immediate conflicts
        print("✓ No immediate CLI argument conflicts detected")
        return True
        
    except Exception as e:
        if 'conflicting option' in str(e):
            print(f"✗ CLI argument conflict detected: {e}")
            return False
        else:
            print(f"✗ Unexpected error testing argument conflicts: {e}")
            return False


def test_var_argument_basic_functionality():
    """Test that --var argument is properly recognized."""
    print("Testing --var argument recognition...")
    
    try:
        # Test with --help to see if --var is listed
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--help'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if '--var' not in result.stdout:
            print("✗ --var argument not found in help output")
            return False
        
        if 'NAME=VALUE' not in result.stdout:
            print("✗ --var argument format not described in help")
            return False
        
        print("✓ --var argument is properly recognized")
        return True
        
    except Exception as e:
        print(f"✗ --var argument test failed: {e}")
        return False


def main():
    """Run all CLI integration tests."""
    print("CLI Integration Tests for Excel Recipe Processor")
    print("=" * 60)
    
    tests = [
        test_cli_help_works,
        test_cli_version_works,
        test_cli_list_capabilities_works,
        test_cli_list_capabilities_formats,
        test_cli_validate_recipe,
        test_cli_missing_required_args,
        test_cli_with_var_arguments,
        test_import_main_module,
        test_cli_argument_conflicts,
        test_var_argument_basic_functionality,
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            print()  # Add spacing between tests
        except Exception as e:
            print(f"✗ Test {test_func.__name__} crashed: {e}")
            print()
    
    print("=" * 60)
    print(f"CLI Integration Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All CLI integration tests passed!")
        print("✓ Command-line interface is working correctly")
        print("✓ No argument parsing conflicts detected")
        print("✓ New --var functionality is properly integrated")
        return True
    else:
        print("❌ Some CLI integration tests failed!")
        print("This indicates issues with the command-line interface that")
        print("regular unit tests don't catch.")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
