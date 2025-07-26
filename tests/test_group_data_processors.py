"""
Comprehensive tests for the refactored GroupDataProcessor with StageManager, 
FileReader, and variable substitution integration.

Tests both existing functionality (regression) and new enhanced capabilities.
"""

import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.group_data_processor import GroupDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_van_report_data():
    """Create sample van report data for testing."""
    return pd.DataFrame({
        'Container_ID': ['C001', 'C002', 'C003', 'C004', 'C005', 'C006'],
        'Product_Origin': ['Dillingham', 'Kodiak', 'Cordova', 'Sitka', 'Naknek', 'Kodiak West'],
        'Product_Type': ['Salmon', 'Crab', 'Salmon', 'Halibut', 'Salmon', 'Crab'],
        'Quantity': [100, 75, 120, 80, 95, 60]
    })


def create_customer_data():
    """Create sample customer data for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'State': ['CA', 'NY', 'TX', 'WA', 'FL'],
        'Industry': ['Tech', 'Finance', 'Healthcare', 'Tech', 'Retail'],
        'Size': ['Large', 'Medium', 'Small', 'Large', 'Medium']
    })


def create_group_definitions_wide():
    """Create sample group definitions in wide format."""
    return pd.DataFrame({
        'West_Coast': ['CA', 'OR', 'WA', 'NV'],
        'East_Coast': ['NY', 'MA', 'FL', 'NC'],
        'Central': ['TX', 'CO', 'IL', 'MO']
    })


def create_group_definitions_long():
    """Create sample group definitions in long format."""
    return pd.DataFrame({
        'Group_Name': ['West_Coast', 'West_Coast', 'West_Coast', 'West_Coast',
                      'East_Coast', 'East_Coast', 'East_Coast', 'East_Coast',
                      'Central', 'Central', 'Central', 'Central'],
        'Values': ['CA', 'OR', 'WA', 'NV', 'NY', 'MA', 'FL', 'NC', 'TX', 'CO', 'IL', 'MO']
    })


def create_industry_lookup_data():
    """Create sample industry lookup data for cross-reference grouping."""
    return pd.DataFrame({
        'Industry': ['Tech', 'Finance', 'Healthcare', 'Retail', 'Manufacturing'],
        'Industry_Group': ['Technology', 'Financial', 'Healthcare', 'Consumer', 'Industrial'],
        'Risk_Level': ['Medium', 'High', 'Low', 'Medium', 'High'],
        'Active': [True, True, True, True, False]
    })


def setup_test_stages():
    """Set up test stages for stage-based grouping tests."""
    StageManager.initialize_stages(max_stages=15)
    
    # Create group definitions stage (wide format)
    group_definitions_wide = create_group_definitions_wide()
    StageManager.save_stage(
        stage_name='Regional Groups Wide',
        data=group_definitions_wide,
        description='Regional group definitions in wide format'
    )
    
    # Create group definitions stage (long format)
    group_definitions_long = create_group_definitions_long()
    StageManager.save_stage(
        stage_name='Regional Groups Long',
        data=group_definitions_long,
        description='Regional group definitions in long format'
    )
    
    # Create industry lookup stage
    industry_lookup = create_industry_lookup_data()
    StageManager.save_stage(
        stage_name='Industry Lookup',
        data=industry_lookup,
        description='Industry classification data'
    )


def setup_test_files(temp_dir):
    """Create test files for file-based grouping tests."""
    
    # Create group definitions file (wide format)
    groups_wide = create_group_definitions_wide()
    groups_wide_file = Path(temp_dir) / "regional_groups.xlsx"
    groups_wide.to_excel(groups_wide_file, index=False, engine='openpyxl')
    
    # Create group definitions file with variable in name
    date_str = datetime.now().strftime('%Y%m%d')
    groups_variable_file = Path(temp_dir) / f"groups_{date_str}.csv"
    groups_wide.to_csv(groups_variable_file, index=False)
    
    # Create group definitions file (long format)
    groups_long = create_group_definitions_long()
    groups_long_file = Path(temp_dir) / "regional_groups_long.tsv"
    groups_long.to_csv(groups_long_file, sep='\t', index=False)
    
    return {
        'groups_wide_file': str(groups_wide_file),
        'groups_variable_file': str(groups_variable_file),
        'groups_template': str(Path(temp_dir) / "groups_{date}.csv"),
        'groups_long_file': str(groups_long_file),
        'temp_dir': temp_dir
    }


def test_basic_inline_grouping():
    """Test basic inline grouping (regression test)."""
    print("\nTesting basic inline grouping...")
    
    van_data = create_van_report_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Basic van report grouping',
        'source_column': 'Product_Origin',
        'target_column': 'Region',
        'groups': {
            'Bristol Bay': ['Dillingham', 'Naknek'],
            'Kodiak': ['Kodiak', 'Kodiak West'],
            'PWS': ['Cordova'],
            'SE': ['Sitka']
        }
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(van_data)
    
    # Check that grouping worked
    if (len(result) == len(van_data) and 
        'Region' in result.columns and
        result.iloc[0]['Region'] == 'Bristol Bay' and
        result.iloc[1]['Region'] == 'Kodiak'):
        print("✓ Basic inline grouping works correctly")
        return True
    else:
        print("✗ Basic inline grouping failed")
        return False


def test_case_insensitive_grouping():
    """Test case insensitive grouping (regression test)."""
    print("\nTesting case insensitive grouping...")
    
    # Create data with mixed case
    mixed_case_data = pd.DataFrame({
        'ID': [1, 2, 3, 4],
        'State': ['ca', 'NY', 'tx', 'WA']
    })
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Case insensitive grouping',
        'source_column': 'State',
        'groups': {
            'West': ['CA', 'WA', 'OR'],
            'East': ['NY', 'FL', 'MA'],
            'Central': ['TX', 'CO', 'IL']
        },
        'case_sensitive': False
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(mixed_case_data)
    
    # Check that case insensitive matching worked
    regions = result['State_Group'].tolist()
    expected_regions = ['West', 'East', 'Central', 'West']
    
    if regions == expected_regions:
        print("✓ Case insensitive grouping works correctly")
        return True
    else:
        print(f"✗ Case insensitive grouping failed: got {regions}")
        return False


def test_unmatched_value_handling():
    """Test different unmatched value handling options (regression test)."""
    print("\nTesting unmatched value handling...")
    
    test_data = pd.DataFrame({
        'ID': [1, 2, 3, 4],
        'State': ['CA', 'NY', 'XX', 'WA']  # XX is unmatched
    })
    
    # Test keep_original
    step_config_keep = {
        'processor_type': 'group_data',
        'step_description': 'Keep original unmatched',
        'source_column': 'State',
        'groups': {
            'West': ['CA', 'WA'],
            'East': ['NY']
        },
        'unmatched_action': 'keep_original'
    }
    
    processor_keep = GroupDataProcessor(step_config_keep)
    result_keep = processor_keep.execute(test_data)
    
    # Test set_default
    step_config_default = {
        'processor_type': 'group_data',
        'step_description': 'Set default unmatched',
        'source_column': 'State',
        'groups': {
            'West': ['CA', 'WA'],
            'East': ['NY']
        },
        'unmatched_action': 'set_default',
        'unmatched_value': 'Unknown'
    }
    
    processor_default = GroupDataProcessor(step_config_default)
    result_default = processor_default.execute(test_data)
    
    # Check results
    keep_values = result_keep['State_Group'].tolist()
    default_values = result_default['State_Group'].tolist()
    
    expected_keep = ['West', 'East', 'XX', 'West']
    expected_default = ['West', 'East', 'Unknown', 'West']
    
    if keep_values == expected_keep and default_values == expected_default:
        print("✓ Unmatched value handling works correctly")
        return True
    else:
        print(f"✗ Unmatched value handling failed")
        print(f"  Keep: {keep_values} (expected {expected_keep})")
        print(f"  Default: {default_values} (expected {expected_default})")
        return False


def test_replace_source_column():
    """Test replacing source column with grouped values (regression test)."""
    print("\nTesting replace source column...")
    
    van_data = create_van_report_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Replace origins with regions',
        'source_column': 'Product_Origin',
        'groups': {
            'Bristol Bay': ['Dillingham', 'Naknek'],
            'Kodiak': ['Kodiak', 'Kodiak West'],
            'PWS': ['Cordova'],
            'SE': ['Sitka']
        },
        'replace_source': True
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(van_data)
    
    # Check that source column was replaced
    if ('Product_Origin' in result.columns and
        'Bristol Bay' in result['Product_Origin'].values and
        'Kodiak' in result['Product_Origin'].values):
        print("✓ Replace source column works correctly")
        return True
    else:
        print("✗ Replace source column failed")
        return False


def test_stage_based_grouping_wide():
    """Test stage-based grouping with wide format."""
    print("\nTesting stage-based grouping (wide format)...")
    
    setup_test_stages()

    customer_data = create_customer_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Stage-based regional grouping',
        'source_column': 'State',
        'groups_source': {
            'type': 'stage',
            'stage_name': 'Regional Groups Wide',
            'format': 'wide'
        }
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(customer_data)
    
    # Check that stage-based grouping worked
    if (len(result) == len(customer_data) and
        'State_Group' in result.columns and
        'West_Coast' in result['State_Group'].values and
        'East_Coast' in result['State_Group'].values):
        print("✓ Stage-based grouping (wide format) works correctly")
        return True
    else:
        print("✗ Stage-based grouping (wide format) failed")
        return False


def test_stage_based_grouping_long():
    """Test stage-based grouping with long format."""
    print("\nTesting stage-based grouping (long format)...")
    
    setup_test_stages()

    customer_data = create_customer_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Stage-based regional grouping (long)',
        'source_column': 'State',
        'groups_source': {
            'type': 'stage',
            'stage_name': 'Regional Groups Long',
            'format': 'long',
            'group_name_column': 'Group_Name',
            'values_column': 'Values'
        }
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(customer_data)
    
    # Check that stage-based grouping worked
    if (len(result) == len(customer_data) and
        'State_Group' in result.columns and
        'West_Coast' in result['State_Group'].values):
        print("✓ Stage-based grouping (long format) works correctly")
        return True
    else:
        print("✗ Stage-based grouping (long format) failed")
        return False


def test_file_based_grouping():
    """Test file-based grouping with FileReader integration."""
    print("\nTesting file-based grouping...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        customer_data = create_customer_data()
        
        step_config = {
            'processor_type': 'group_data',
            'step_description': 'File-based regional grouping',
            'source_column': 'State',
            'groups_source': {
                'type': 'file',
                'filename': test_files['groups_wide_file'],
                'format': 'wide'
            }
        }
        
        processor = GroupDataProcessor(step_config)
        result = processor.execute(customer_data)
        
        # Check that file-based grouping worked
        if (len(result) == len(customer_data) and
            'State_Group' in result.columns and
            'West_Coast' in result['State_Group'].values):
            print("✓ File-based grouping works correctly")
            return True
        else:
            print("✗ File-based grouping failed")
            return False


def test_variable_substitution_grouping():
    """Test file-based grouping with variable substitution."""
    print("\nTesting variable substitution in grouping...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        customer_data = create_customer_data()
        
        step_config = {
            'processor_type': 'group_data',
            'step_description': 'Variable substitution grouping',
            'source_column': 'State',
            'groups_source': {
                'type': 'file',
                'filename': test_files['groups_template'],  # Contains {date}
                'format': 'wide'
            }
        }
        
        processor = GroupDataProcessor(step_config)
        result = processor.execute(customer_data)
        
        # Check that variable substitution worked
        if (len(result) == len(customer_data) and
            'State_Group' in result.columns and
            'West_Coast' in result['State_Group'].values):
            print("✓ Variable substitution grouping works correctly")
            return True
        else:
            print("✗ Variable substitution grouping failed")
            return False


def test_lookup_based_grouping():
    """Test lookup-based grouping with cross-reference validation."""
    print("\nTesting lookup-based grouping...")
    
    customer_data = create_customer_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Lookup-based industry grouping',
        'source_column': 'Industry',
        'groups_source': {
            'type': 'lookup',
            'lookup_stage': 'Industry Lookup',
            'lookup_key': 'Industry',
            'group_column': 'Industry_Group',
            'values_column': 'Industry',
            'filter_condition': {
                'column': 'Active',
                'value': True,
                'operator': 'equals'
            }
        }
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(customer_data)
    
    # Check that lookup-based grouping worked
    if (len(result) == len(customer_data) and
        'Industry_Group' in result.columns and
        'Technology' in result['Industry_Group'].values and
        'Financial' in result['Industry_Group'].values):
        print("✓ Lookup-based grouping works correctly")
        return True
    else:
        print("✗ Lookup-based grouping failed")
        return False


def test_predefined_groups():
    """Test predefined group sets."""
    print("\nTesting predefined groups...")
    
    van_data = create_van_report_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Van report regions',
        'source_column': 'Product_Origin',
        'predefined_groups': 'van_report_regions'
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(van_data)
    
    # Check that predefined groups worked
    if (len(result) == len(van_data) and
        'Product_Origin_Group' in result.columns and
        'Bristol Bay' in result['Product_Origin_Group'].values and
        'Kodiak' in result['Product_Origin_Group'].values):
        print("✓ Predefined groups work correctly")
        return True
    else:
        print("✗ Predefined groups failed")
        return False


def test_save_to_stage():
    """Test saving grouping results to a stage."""
    print("\nTesting save to stage...")
    
    setup_test_stages()

    van_data = create_van_report_data()
    
    step_config = {
        'processor_type': 'group_data',
        'step_description': 'Group and save to stage',
        'source_column': 'Product_Origin',
        'predefined_groups': 'van_report_regions',
        'save_to_stage': 'Grouped Van Data',
        'stage_description': 'Van data grouped by regions'
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(van_data)
    
    # Check that stage was created
    if (StageManager.stage_exists('Grouped Van Data') and
        len(result) == len(van_data)):
        
        # Verify stage contains correct data
        stage_data = StageManager.get_stage_data('Grouped Van Data')
        
        if (len(stage_data) == len(result) and
            'Product_Origin_Group' in stage_data.columns):
            print("✓ Save to stage works correctly")
            return True
        else:
            print("✗ Stage data doesn't match result")
            return False
    else:
        print("✗ Save to stage failed")
        return False


def test_hierarchical_grouping():
    """Test hierarchical grouping with multiple levels."""
    print("\nTesting hierarchical grouping...")
    
    customer_data = create_customer_data()
    
    processor = GroupDataProcessor({'processor_type': 'group_data'})
    
    hierarchy_levels = [
        {
            'level_name': 'Region',
            'groups': {
                'West': ['CA', 'WA', 'OR'],
                'East': ['NY', 'FL', 'MA'],
                'Central': ['TX', 'CO', 'IL']
            }
        },
        {
            'level_name': 'Territory',
            'parent_column': 'Region',
            'groups': {
                'Pacific': ['West'],
                'Atlantic': ['East'],
                'Continental': ['Central']
            }
        }
    ]
    
    result = processor.create_hierarchical_groups(customer_data, 'State', hierarchy_levels)
    
    # Check that hierarchical grouping worked
    if ('Region' in result.columns and 
        'Territory' in result.columns and
        'Pacific' in result['Territory'].values):
        print("✓ Hierarchical grouping works correctly")
        return True
    else:
        print("✗ Hierarchical grouping failed")
        return False


def test_analysis_methods():
    """Test analysis and utility methods."""
    print("\nTesting analysis methods...")
    
    customer_data = create_customer_data()
    processor = GroupDataProcessor({'processor_type': 'group_data'})
    
    # Test grouping potential analysis
    analysis = processor.analyze_grouping_potential(customer_data, 'State')
    
    # Test regional groups helper
    van_data = create_van_report_data()
    regional_result = processor.create_regional_groups(van_data, 'Product_Origin')
    
    # Check analysis results
    analysis_valid = (
        'unique_values' in analysis and
        'grouping_recommendation' in analysis and
        analysis['column_name'] == 'State'
    )
    
    # Check regional grouping
    regional_valid = (
        'Product_Origin_Region' in regional_result.columns and
        'Bristol Bay' in regional_result['Product_Origin_Region'].values
    )
    
    if analysis_valid and regional_valid:
        print("✓ Analysis methods work correctly")
        return True
    else:
        print("✗ Analysis methods failed")
        return False


def test_grouping_error_handling():
    """Test error handling for various failure scenarios."""
    print("\nTesting grouping error handling...")
    
    customer_data = create_customer_data()
    
    # Test missing source column
    try:
        step_config = {
            'processor_type': 'group_data',
            # Missing source_column
            'groups': {'Test': ['A', 'B']}
        }
        processor = GroupDataProcessor(step_config)
        processor.execute(customer_data)
        print("✗ Should have failed with missing source_column")
        return False
    except StepProcessorError as e:
        if "source_column" in str(e):
            print("✓ Caught expected error for missing source_column")
        else:
            print(f"✗ Wrong error message: {e}")
            return False
    
    # Test nonexistent stage
    try:
        step_config = {
            'processor_type': 'group_data',
            'source_column': 'State',
            'groups_source': {
                'type': 'stage',
                'stage_name': 'Nonexistent Stage'
            }
        }
        processor = GroupDataProcessor(step_config)
        processor.execute(customer_data)
        print("✗ Should have failed with nonexistent stage")
        return False
    except StepProcessorError as e:
        if "not found" in str(e):
            print("✓ Caught expected error for nonexistent stage")
        else:
            print(f"✗ Wrong error message: {e}")
            return False
    
    # Test invalid source column
    try:
        step_config = {
            'processor_type': 'group_data',
            'source_column': 'Invalid_Column',
            'groups': {'Test': ['A', 'B']}
        }
        processor = GroupDataProcessor(step_config)
        processor.execute(customer_data)
        print("✗ Should have failed with invalid source column")
        return False
    except StepProcessorError as e:
        if "not found" in str(e):
            print("✓ Caught expected error for invalid source column")
        else:
            print(f"✗ Wrong error message: {e}")
            return False
    
    print("✓ Error handling works correctly")
    return True


def test_capabilities_and_configuration():
    """Test processor capabilities and configuration methods."""
    print("\nTesting capabilities and configuration...")
    
    processor = GroupDataProcessor({'processor_type': 'group_data'})
    
    # Test supported methods exist
    unmatched_actions = processor.get_supported_unmatched_actions()
    source_types = processor.get_supported_source_types()
    file_formats = processor.get_supported_file_formats()
    predefined_types = processor.get_predefined_group_types()
    capabilities = processor.get_capabilities()
    
    # Check expected values
    expected_actions = ['keep_original', 'set_default', 'error']
    expected_sources = ['inline', 'stage', 'file', 'lookup', 'predefined']
    expected_formats = ['wide', 'long']
    expected_predefined = ['van_report_regions', 'us_regions', 'product_categories']
    
    actions_correct = all(action in unmatched_actions for action in expected_actions)
    sources_correct = all(source in source_types for source in expected_sources)
    formats_correct = all(fmt in file_formats for fmt in expected_formats)
    predefined_correct = all(pred in predefined_types for pred in expected_predefined)
    
    has_capabilities = (
        'description' in capabilities and
        'source_types' in capabilities and
        'stage_integration' in capabilities and
        'file_features' in capabilities
    )
    
    if actions_correct and sources_correct and formats_correct and predefined_correct and has_capabilities:
        print("✓ Capabilities and configuration work correctly")
        return True
    else:
        print("✗ Capabilities and configuration failed")
        return False


def test_real_world_scenario():
    """Test a complete real-world grouping scenario."""
    print("\nTesting real-world scenario...")
    
    setup_test_stages()
    
    # Simulate complex van report processing
    van_data = pd.DataFrame({
        'Container_ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'Product_Origin': ['Dillingham', 'Kodiak', 'Cordova', 'Unknown_Port', 'Naknek'],
        'Product_Type': ['Salmon', 'Crab', 'Salmon', 'Halibut', 'Salmon'],
        'Season': ['Summer', 'Fall', 'Summer', 'Winter', 'Summer']
    })
    
    # Step 1: Group by predefined regions
    step_config_1 = {
        'processor_type': 'group_data',
        'step_description': 'Group by Alaska regions',
        'source_column': 'Product_Origin',
        'predefined_groups': 'van_report_regions',
        'target_column': 'Alaska_Region',
        'unmatched_action': 'set_default',
        'unmatched_value': 'Other_Ports'
    }
    
    processor_1 = GroupDataProcessor(step_config_1)
    result_1 = processor_1.execute(van_data)
    
    # Step 2: Create seasonal groups and save to stage
    step_config_2 = {
        'processor_type': 'group_data',
        'step_description': 'Group by fishing seasons',
        'source_column': 'Season',
        'groups': {
            'Peak_Season': ['Summer', 'Fall'],
            'Off_Season': ['Winter', 'Spring']
        },
        'target_column': 'Season_Category',
        'save_to_stage': 'Processed Van Report',
        'stage_description': 'Van report with regional and seasonal grouping'
    }
    
    processor_2 = GroupDataProcessor(step_config_2)
    final_result = processor_2.execute(result_1)
    
    # Verify the complete workflow
    expected_columns = ['Container_ID', 'Product_Origin', 'Product_Type', 'Season', 
                       'Alaska_Region', 'Season_Category']
    has_all_columns = all(col in final_result.columns for col in expected_columns)
    
    # Check specific grouping results
    bristol_bay_count = (final_result['Alaska_Region'] == 'Bristol Bay').sum()
    peak_season_count = (final_result['Season_Category'] == 'Peak_Season').sum()
    other_ports_count = (final_result['Alaska_Region'] == 'Other_Ports').sum()
    
    # Check stage was created
    stage_exists = StageManager.stage_exists('Processed Van Report')
    
    if (has_all_columns and 
        bristol_bay_count > 0 and 
        peak_season_count > 0 and
        other_ports_count > 0 and  # Unknown_Port should be mapped to Other_Ports
        stage_exists):
        print("✓ Real-world scenario works correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


if __name__ == '__main__':
    print("Testing GroupDataProcessor refactoring...")
    success = True
    
    # Basic regression tests
    print("\n=== Testing Basic Functionality (Regression) ===")
    success &= test_basic_inline_grouping()
    success &= test_case_insensitive_grouping()
    success &= test_unmatched_value_handling()
    success &= test_replace_source_column()
    
    # Stage-based grouping tests
    print("\n=== Testing Stage-Based Grouping (StageManager Integration) ===")
    setup_test_stages()
    try:
        success &= test_stage_based_grouping_wide()
        success &= test_stage_based_grouping_long()
        success &= test_lookup_based_grouping()
        success &= test_save_to_stage()
        success &= test_real_world_scenario()
    finally:
        StageManager.cleanup_stages()
    
    # File-based grouping tests
    print("\n=== Testing File-Based Grouping (FileReader Integration) ===")
    success &= test_file_based_grouping()
    success &= test_variable_substitution_grouping()
    
    # Advanced feature tests
    print("\n=== Testing Advanced Features ===")
    success &= test_predefined_groups()
    success &= test_hierarchical_grouping()
    success &= test_analysis_methods()
    
    # Error handling and capabilities tests
    print("\n=== Testing Error Handling and Capabilities ===")
    success &= test_grouping_error_handling()
    success &= test_capabilities_and_configuration()
    
    if success:
        print("\n🎉 All GroupDataProcessor refactoring tests passed!")
    else:
        print("\n❌ Some GroupDataProcessor refactoring tests failed!")
    
    # Show processor capabilities
    processor = GroupDataProcessor({'processor_type': 'group_data'})
    
    print(f"\nSupported unmatched actions: {processor.get_supported_unmatched_actions()}")
    print(f"Supported source types: {processor.get_supported_source_types()}")
    print(f"Supported file formats: {processor.get_supported_file_formats()}")
    print(f"Predefined group types: {processor.get_predefined_group_types()}")
    
    print("\nTo run with pytest: pytest test_group_data_processor_refactored.py -v")
