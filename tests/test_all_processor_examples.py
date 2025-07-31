#!/usr/bin/env python3
"""
Generic test module for all processor usage examples.

This module automatically discovers all processors in the system and validates
that they have proper usage examples available, either via external YAML files
or get_usage_examples() methods.

Usage:
    python test_all_processor_examples.py
"""

import sys
import subprocess

from pathlib import Path


def get_all_processors():
    """
    Discover all registered processors in the system.
    
    Returns:
        List of processor names
    """
    try:
        from excel_recipe_processor.core.pipeline import get_system_capabilities
        capabilities = get_system_capabilities()
        
        # Filter out base_processor and get processor names
        processors = [
            name for name in capabilities['processors'].keys() 
            if name != 'base_processor'
        ]
        
        return sorted(processors)
        
    except Exception as e:
        print(f"❌ Could not discover processors: {e}")
        return []


def get_examples_directory():
    """Get path to the examples directory."""
    project_root = Path(__file__).parent.parent
    return project_root / "excel_recipe_processor" / "processors" / "_examples"


def test_examples_directory_exists():
    """Test that the examples directory exists."""
    print("📁 Testing examples directory...")
    
    examples_dir = get_examples_directory()
    if examples_dir.exists() and examples_dir.is_dir():
        print(f"   ✅ Examples directory exists: {examples_dir}")
        return True
    else:
        print(f"   ❌ Examples directory missing: {examples_dir}")
        return False


def test_processor_discovery():
    """Test that we can discover processors."""
    print("\n📋 Testing processor discovery...")
    
    processors = get_all_processors()
    if len(processors) == 0:
        print("   ❌ No processors discovered")
        return False
    
    print(f"   ✅ Found {len(processors)} processors")
    
    # Verify we excluded base_processor
    if 'base_processor' in processors:
        print("   ❌ base_processor should be excluded")
        return False
    
    # Should have common processors
    expected_processors = ['export_file', 'import_file', 'filter_data']
    missing_expected = []
    for expected in expected_processors:
        if expected not in processors:
            missing_expected.append(expected)
    
    if missing_expected:
        print(f"   ❌ Missing expected processors: {missing_expected}")
        return False
    
    return True


def test_yaml_files_existence():
    """Test which processors have YAML files and which don't."""
    print("\n📄 Testing YAML file existence for all processors...")
    
    processors = get_all_processors()
    examples_dir = get_examples_directory()
    
    if not examples_dir.exists():
        print("   ❌ Examples directory doesn't exist")
        return False
    
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order
    processor_names_with_yaml = {f.stem.replace('_examples', '') for f in yaml_files}
    
    has_yaml = []
    missing_yaml = []
    
    print("\n   Status for all processors:")
    for processor in processors:
        if processor in processor_names_with_yaml:
            print(f"   ✅ YAML file    - {processor}")
            has_yaml.append(processor)
        else:
            print(f"   ❌ Missing YAML - {processor}")
            missing_yaml.append(processor)
    
    print(f"\n   📊 Summary:")
    print(f"      Processors with YAML files: {len(has_yaml)}")
    print(f"      Processors missing YAML files: {len(missing_yaml)}")
    print(f"      Total processors: {len(processors)}")
    
    if missing_yaml:
        print(f"\n   📝 Processors needing YAML files:")
        for processor in missing_yaml:
            print(f"      - {processor}_examples.yaml")
    
    return True  # This test is informational, not a failure condition


def test_yaml_file_syntax():
    """Test that existing YAML files have valid syntax."""
    print("\n🔍 Testing YAML file syntax...")
    
    try:
        import yaml
    except ImportError:
        print("   ⚠️  PyYAML not available - skipping syntax test")
        return True
    
    examples_dir = get_examples_directory()
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order
    
    if not yaml_files:
        print("   ⚠️  No YAML files found")
        return True
    
    all_valid = True
    
    for yaml_file in yaml_files:
        processor_name = yaml_file.stem.replace('_examples', '')
        
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data:
                print(f"   ❌ {processor_name:>24}: Empty or invalid YAML")
                all_valid = False
                continue
            
            if not isinstance(data, dict):
                print(f"   ❌ {processor_name:>24}: Root level must be a dictionary")
                all_valid = False
                continue
            
            # Check required keys
            if 'description' not in data:
                print(f"   ❌ {processor_name:>24}: Missing 'description' key")
                all_valid = False
                continue
            
            # Check for examples
            example_keys = [key for key in data.keys() if key.endswith('_example')]
            if not example_keys:
                print(f"   ❌ {processor_name:>24}: No examples found")
                all_valid = False
                continue
            
            print(f"   ✅ {processor_name:>24}: Valid YAML with {len(example_keys)} examples")
            
        except yaml.YAMLError as e:
            print(f"   ❌ {processor_name:>24}: YAML syntax error - {e}")
            all_valid = False
        except Exception as e:
            print(f"   ❌ {processor_name:>24}: Error reading file - {e}")
            all_valid = False
    
    return all_valid


def test_yaml_content_quality():
    """Test YAML content quality and formatting standards."""
    print("\n✨ Testing YAML content quality...")
    
    examples_dir = get_examples_directory()
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order
    
    if not yaml_files:
        print("   ⚠️  No YAML files to test")
        return True
    
    all_good = True
    
    for yaml_file in yaml_files:
        processor_name = yaml_file.stem.replace('_examples', '')
        
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            issues = []
            
            # Check formatting standards
            if 'OPT -' not in content:
                issues.append("missing 'OPT -' markers")
            
            if 'REQ -' not in content:
                issues.append("missing 'REQ -' markers")
            
            if f'processor_type: "{processor_name}"' not in content:
                issues.append("missing correct processor_type")
            
            if 'Default value:' not in content and 'default:' not in content.lower():
                issues.append("missing default value documentation")
            
            if issues:
                print(f"   ⚠️  {processor_name:>24}: {', '.join(issues)}")
                all_good = False
            else:
                print(f"   ✅ {processor_name:>24}: Good formatting")
                
        except Exception as e:
            print(f"   ❌ {processor_name:>24}: Error checking content - {e}")
            all_good = False
    
    return all_good


def test_cli_integration():
    """Test that CLI commands work for processors with examples."""
    print("\n🖥️  Testing CLI integration...")
    
    examples_dir = get_examples_directory()
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order
    
    if not yaml_files:
        print("   ⚠️  No YAML files to test")
        return True
    
    all_working = True
    
    for yaml_file in yaml_files:
        processor_name = yaml_file.stem.replace('_examples', '')
        
        try:
            result = subprocess.run(
                ["python", "-m", "excel_recipe_processor", "--get-usage-examples", processor_name],
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode != 0:
                print(f"   ❌ {processor_name:>24}: CLI command failed - {result.stderr.strip()}")
                all_working = False
                continue
            
            output = result.stdout
            
            # Check for expected content
            if f'processor_type: "{processor_name}"' not in output:
                print(f"   ❌ {processor_name:>24}: CLI output missing correct processor_type")
                all_working = False
                continue
            
            if 'OPT -' not in output and 'REQ -' not in output:
                print(f"   ❌ {processor_name:>24}: CLI output missing OPT/REQ markers")
                all_working = False
                continue
            
            print(f"   ✅ {processor_name:>24}: CLI working correctly")
            
        except subprocess.TimeoutExpired:
            print(f"   ❌ {processor_name:>24}: CLI command timed out")
            all_working = False
        except Exception as e:
            print(f"   ❌ {processor_name:>24}: Error testing CLI - {e}")
            all_working = False
    
    return all_working


def test_processor_method_fallbacks():
    """Test that processors without YAML files have working get_usage_examples methods."""
    print("\n🔄 Testing fallback methods for processors without YAML files...")
    
    processors = get_all_processors()
    examples_dir = get_examples_directory()
    
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order

    processors_with_yaml = {f.stem.replace('_examples', '') for f in yaml_files}
    
    processors_without_yaml = [p for p in processors if p not in processors_with_yaml]
    
    if not processors_without_yaml:
        print("   ✅ All processors have YAML files!")
        return True
    
    print(f"   Testing {len(processors_without_yaml)} processors without YAML files...")
    
    working_methods = []
    missing_methods = []
    
    for processor in processors_without_yaml:
        try:
            from excel_recipe_processor.core.pipeline import get_processor_usage_examples
            result = get_processor_usage_examples(processor)
            
            if result is not None and 'error' not in result:
                print(f"   ✅ {processor}: Has working get_usage_examples() method")
                working_methods.append(processor)
            else:
                error_msg = result.get('error', 'Unknown error') if result else 'No result'
                print(f"   ❌ {processor}: Method failed - {error_msg}")
                missing_methods.append(processor)
                
        except Exception as e:
            print(f"   ❌ {processor}: Error testing method - {e}")
            missing_methods.append(processor)
    
    if missing_methods:
        print(f"\n   ⚠️  {len(missing_methods)} processors need either YAML files or working methods:")
        for processor in missing_methods:
            print(f"      - {processor}")
    
    return len(missing_methods) == 0


def test_revision_dates():
    """Test that YAML files have today's revision date comment."""
    print("\n📅 Testing revision date comments...")
    
    examples_dir = get_examples_directory()
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order
    
    if not yaml_files:
        print("   ⚠️  No YAML files to test")
        return True
    
    today_date = "2025-07-30"  # Update this as needed
    all_current = True
    
    for yaml_file in yaml_files:
        processor_name = yaml_file.stem.replace('_examples', '')
        
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                # Read first 10 lines to look for revision date
                lines = [f.readline().strip() for _ in range(10)]
            
            # Look for revision date comment
            revision_line = None
            for i, line in enumerate(lines):
                if line.startswith("# Revision date:"):
                    revision_line = line
                    break
            
            if revision_line is None:
                print(f"   ❌ {processor_name:>24}: No revision date comment found")
                all_current = False
                continue
            
            # Extract the date from the comment
            try:
                date_part = revision_line.split("# Revision date:")[1].strip()
                if date_part == today_date:
                    print(f"   ✅ {processor_name:>24}: {revision_line}")
                else:
                    print(f"   ⚠️  {processor_name:>24}: {revision_line} (needs update to {today_date})")
                    all_current = False
            except (IndexError, ValueError):
                print(f"   ❌ {processor_name:>24}: Invalid revision date format: {revision_line}")
                all_current = False
                
        except Exception as e:
            print(f"   ❌ {processor_name:>24}: Error reading file - {e}")
            all_current = False
    
    if all_current:
        print(f"\n   🎉 All YAML files have current revision date ({today_date})")
    else:
        print(f"\n   📝 Some files need revision date updates to {today_date}")
    
    return all_current


def test_settings_description_requirement():
    """Test that all settings sections contain required description key."""
    print("\n⚙️  Testing settings description requirement...")
    
    examples_dir = get_examples_directory()
    yaml_files = list(examples_dir.glob("*_examples.yaml"))
    yaml_files.sort() # Make list come out in alphabetical order
    
    if not yaml_files:
        print("   ⚠️  No YAML files to test")
        return True
    
    all_have_descriptions = True
    
    for yaml_file in yaml_files:
        processor_name = yaml_file.stem.replace('_examples', '')
        
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            import yaml
            data = yaml.safe_load(content)
            
            if not data:
                print(f"   ❌ {processor_name:>24}: Invalid YAML structure")
                all_have_descriptions = False
                continue
            
            # Find all settings sections in the examples
            settings_sections = []
            
            # Check each example for settings sections
            for key, example in data.items():
                if key.endswith('_example') and isinstance(example, dict):
                    if 'yaml' in example:
                        try:
                            # Parse the YAML content of the example
                            example_yaml = yaml.safe_load(example['yaml'])
                            if isinstance(example_yaml, list):
                                # Look for settings in recipe steps (shouldn't be there, but check anyway)
                                for step in example_yaml:
                                    if isinstance(step, dict) and 'settings' in step:
                                        settings_sections.append((key, step['settings']))
                            elif isinstance(example_yaml, dict) and 'settings' in example_yaml:
                                settings_sections.append((key, example_yaml['settings']))
                        except yaml.YAMLError:
                            # Skip malformed YAML in examples
                            continue
            
            # Check each settings section for description
            missing_descriptions = []
            for example_name, settings in settings_sections:
                if not isinstance(settings, dict):
                    continue
                if 'description' not in settings:
                    missing_descriptions.append(example_name)
            
            if missing_descriptions:
                print(f"   ❌ {processor_name:>24}: Missing description in settings for examples: {', '.join(missing_descriptions)}")
                all_have_descriptions = False
            else:
                settings_count = len(settings_sections)
                if settings_count > 0:
                    print(f"   ✅ {processor_name:>24}: All {settings_count} settings sections have descriptions")
                else:
                    print(f"   ⚠️ {processor_name:>24}: No settings sections found")
                    all_have_descriptions = False
                    
        except Exception as e:
            print(f"   ❌ {processor_name:>24}: Error checking settings - {e}")
            all_have_descriptions = False
    
    if all_have_descriptions:
        print(f"\n   🎉 All settings sections have required descriptions")
    else:
        print(f"\n   📝 Some settings sections missing required descriptions (or no settings found)")
        print(f"      A 'settings:' section is required in every valid recipe")
        print(f"   💡 Every settings section must include: description: 'Brief description of what this recipe does'")
    
    return all_have_descriptions


def main():
    """Run all tests for processor usage examples."""
    print("🧪 Testing All Processor Usage Examples")
    print("=" * 60)
    print("Comprehensive validation of processor examples across the entire system")
    print()
    
    tests = [
            ("Examples Directory", test_examples_directory_exists),
            ("Processor Discovery", test_processor_discovery),
            ("YAML File Existence", test_yaml_files_existence),
            ("YAML Syntax Validation", test_yaml_file_syntax),
            ("Revision Dates", test_revision_dates),
            ("Settings Description Requirement", test_settings_description_requirement),  # NEW TEST
            ("Content Quality", test_yaml_content_quality),
            ("CLI Integration", test_cli_integration),
            ("Method Fallbacks", test_processor_method_fallbacks)
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
    
    # Final summary
    print(f"\n{'=' * 60}")
    print("FINAL TEST SUMMARY")
    print('=' * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    # Give actionable guidance
    if passed == total:
        print("🎉 All tests passed! Processor examples system is working correctly.")
    else:
        print("\n📋 Next Steps:")
        processors = get_all_processors()
        examples_dir = get_examples_directory()
        yaml_files = list(examples_dir.glob("*_examples.yaml"))
        yaml_files.sort() # Make list come out in alphabetical order
        processors_with_yaml = {f.stem.replace('_examples', '') for f in yaml_files}
        missing_yaml = [p for p in processors if p not in processors_with_yaml]
        
        if missing_yaml:
            print(f"   1. Create YAML files for {len(missing_yaml)} processors")
            print(f"   2. Follow the pattern from export_file_examples.yaml")
            print(f"   3. Priority processors: import_file, filter_data, add_calculated_column")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
