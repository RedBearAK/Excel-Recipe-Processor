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


def test_cli_get_settings_examples():
    """Test that --get-settings-examples works."""
    print("Testing CLI get settings examples...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--get-settings-examples'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"✗ Get settings examples failed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        output = result.stdout
        
        # Check for expected content
        expected_content = [
            'settings:',
            'Recipe Settings Usage Examples',
            'description:',
            'create_backup:'
        ]
        
        for content in expected_content:
            if content not in output:
                print(f"✗ Settings examples output missing expected content: '{content}'")
                return False
        
        print("✓ CLI get settings examples works")
        return True
        
    except Exception as e:
        print(f"✗ CLI get settings examples test failed: {e}")
        return False


def test_cli_get_settings_examples_formats():
    """Test settings examples in different formats."""
    print("Testing CLI settings examples output formats...")
    
    formats = [
        (['--get-settings-examples'], 'yaml'),
        (['--get-settings-examples', '--format-examples', 'json'], 'json'),
        (['--get-settings-examples', '--format-examples', 'text'], 'text'),
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
                print(f"✗ Settings examples format {format_name} failed with return code: {result.returncode}")
                print(f"Error: {result.stderr}")
                return False
            
            output = result.stdout
            
            if len(output) == 0:
                print(f"✗ Settings examples format {format_name} produced no output")
                return False
            
            # Format-specific checks
            if format_name == 'json':
                if '"description":' not in output or '"examples":' not in output:
                    print(f"✗ JSON format missing expected structure")
                    return False
            elif format_name == 'yaml':
                if 'settings:' not in output:
                    print(f"✗ YAML format missing settings section")
                    return False
            elif format_name == 'text':
                if 'Recipe Settings' not in output:
                    print(f"✗ Text format missing expected header")
                    return False
            
            print(f"  ✓ Settings examples format {format_name} works")
            
        except Exception as e:
            print(f"✗ Settings examples format {format_name} test failed: {e}")
            return False
    
    print("✓ All settings examples formats work")
    return True


def test_cli_get_usage_examples_settings():
    """Test that --get-usage-examples settings works (backup method)."""
    print("Testing CLI get usage examples with 'settings' as processor name...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--get-usage-examples', 'settings'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"✗ Get usage examples settings failed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        output = result.stdout
        
        # Should have same content as --get-settings-examples
        expected_content = [
            'settings:',
            'Recipe Settings Usage Examples',
            'description:',
            'create_backup:'
        ]
        
        for content in expected_content:
            if content not in output:
                print(f"✗ Usage examples settings output missing expected content: '{content}'")
                return False
        
        print("✓ CLI get usage examples with 'settings' works")
        return True
        
    except Exception as e:
        print(f"✗ CLI get usage examples settings test failed: {e}")
        return False


def test_cli_get_usage_examples_all_includes_settings():
    """Test that --get-usage-examples (all) includes settings first."""
    print("Testing CLI get all usage examples includes settings first...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--get-usage-examples'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"✗ Get all usage examples failed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        output = result.stdout
        lines = output.split('\n')
        
        # Check that settings appears before processors
        settings_line = -1
        processors_line = -1
        
        for i, line in enumerate(lines):
            if 'RECIPE SETTINGS' in line:
                settings_line = i
            if 'PROCESSOR' in line and 'RECIPE SETTINGS' not in line:
                if processors_line == -1:  # First processor mention
                    processors_line = i
        
        if settings_line == -1:
            print("✗ Settings section not found in all usage examples")
            return False
            
        if processors_line == -1:
            print("✗ Processor sections not found in all usage examples")
            return False
            
        if settings_line >= processors_line:
            print("✗ Settings should appear before processors in output")
            return False
        
        print("✓ CLI get all usage examples includes settings first")
        return True
        
    except Exception as e:
        print(f"✗ CLI get all usage examples test failed: {e}")
        return False


def test_cli_help_includes_new_options():
    """Test that --help includes the new settings examples options."""
    print("Testing CLI help includes new settings options...")
    
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
        
        # Check for new arguments
        expected_options = [
            '--get-settings-examples',
            '--get-usage-examples',
            '--format-examples',
            'recipe settings examples'
        ]
        
        # Special check for the settings instruction (more flexible)
        settings_instruction_found = False
        if 'Use "settings"' in help_text or "Use 'settings'" in help_text:
            settings_instruction_found = True
        
        for option in expected_options:
            if option not in help_text:
                print(f"✗ Help text missing expected option: '{option}'")
                return False
        
        # Check the settings instruction separately
        if not settings_instruction_found:
            print("✗ Help text missing settings instruction")
            print("Looking for: 'Use \"settings\"' or similar")
            return False
        
        # Check for updated examples in epilog
        expected_examples = [
            '--get-settings-examples',
            '--get-usage-examples settings',
            'Get recipe settings examples'
        ]
        
        for example in expected_examples:
            if example not in help_text:
                print(f"✗ Help text missing expected example: '{example}'")
                return False
        
        print("✓ CLI help includes new settings options")
        return True
        
    except Exception as e:
        print(f"✗ CLI help settings options test failed: {e}")
        return False


def test_cli_settings_examples_error_handling():
    """Test error handling when settings examples are not available."""
    print("Testing CLI settings examples error handling...")
    
    # This test is tricky because it depends on the file existing or not
    # We'll test that the command doesn't crash, even if it returns an error
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--get-settings-examples'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Command should not crash (return code could be 0 or 1)
        if result.returncode not in [0, 1]:
            print(f"✗ Settings examples command crashed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        # Should have some output (either examples or error message)
        if len(result.stdout) == 0 and len(result.stderr) == 0:
            print("✗ Settings examples command produced no output")
            return False
        
        # If it's an error, should be informative
        if result.returncode == 1:
            combined_output = result.stdout + result.stderr
            if 'examples' not in combined_output.lower():
                print("✗ Error message should mention examples")
                return False
        
        print("✓ CLI settings examples error handling works")
        return True
        
    except Exception as e:
        print(f"✗ CLI settings examples error handling test failed: {e}")
        return False


def test_cli_invalid_processor_with_settings_suggestion():
    """Test that invalid processor names suggest 'settings' as option."""
    print("Testing CLI invalid processor name with settings suggestion...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--get-usage-examples', 'nonexistent_processor'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Should fail for invalid processor
        if result.returncode == 0:
            print("✗ Should have failed for nonexistent processor")
            return False
        
        output = result.stdout + result.stderr
        
        # Should suggest 'settings' as an option
        if 'settings' not in output:
            print("✗ Error message should suggest 'settings' as an option")
            return False
        
        # Should list available processors
        if 'Available processors:' not in output:
            print("✗ Error message should list available processors")
            return False
        
        print("✓ CLI invalid processor suggests settings option")
        return True
        
    except Exception as e:
        print(f"✗ CLI invalid processor test failed: {e}")
        return False


def test_cli_format_examples_without_context():
    """Test that --format-examples alone shows appropriate behavior."""
    print("Testing CLI format-examples without context...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', '--format-examples', 'json'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # This should either show help (0) or give a reasonable error (1)
        # Both are acceptable since --format-examples alone doesn't make sense
        if result.returncode not in [0, 1]:
            print(f"✗ Unexpected return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
        
        # Should have some output (either help or error message)
        if len(result.stdout) == 0 and len(result.stderr) == 0:
            print("✗ Should have some output (help or error)")
            return False
        
        # If it shows help, that's fine
        if result.returncode == 0 and 'usage:' in result.stdout.lower():
            print("✓ CLI format-examples alone shows help")
            return True
        
        # If it gives an error, that's also reasonable
        if result.returncode == 1:
            print("✓ CLI format-examples alone gives appropriate error")
            return True
        
        # If return code is 0 but no help shown, that's unexpected
        print("✗ Return code 0 but no help text shown")
        print(f"Output: {result.stdout[:200]}...")
        return False
        
    except Exception as e:
        print(f"✗ CLI format-examples test failed: {e}")
        return False


def test_cli_settings_examples_with_invalid_format():
    """Test settings examples with invalid format option."""
    print("Testing CLI settings examples with invalid format...")
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'excel_recipe_processor', 
                '--get-settings-examples', '--format-examples', 'invalid_format'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Should fail with argument error
        if result.returncode == 0:
            print("✗ Should have failed with invalid format")
            return False
        
        error_output = result.stderr
        
        # Should mention the invalid choice
        if 'invalid choice' not in error_output.lower() and 'invalid_format' not in error_output:
            print("✗ Error should mention invalid format choice")
            print(f"Error output: {error_output}")
            return False
        
        print("✓ CLI settings examples handles invalid format correctly")
        return True
        
    except Exception as e:
        print(f"✗ CLI invalid format test failed: {e}")
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
        # NEW: Settings examples functionality tests
        test_cli_get_settings_examples,
        test_cli_get_settings_examples_formats,
        test_cli_get_usage_examples_settings,
        test_cli_get_usage_examples_all_includes_settings,
        test_cli_help_includes_new_options,
        test_cli_settings_examples_error_handling,

        test_cli_invalid_processor_with_settings_suggestion,
        test_cli_format_examples_without_context,
        test_cli_settings_examples_with_invalid_format,
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
        print("✓ New settings examples functionality is working")
        return True
    else:
        print("❌ Some CLI integration tests failed!")
        print("This indicates issues with the command-line interface that")
        print("regular unit tests don't catch.")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
