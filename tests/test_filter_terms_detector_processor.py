#!/usr/bin/env python3
"""
Test module for FilterTermsDetectorProcessor.

tests/test_filter_terms_detector_processor.py

Tests the filter terms detection functionality including n-gram analysis,
categorical filtering detection, and result formatting.
"""

import os
import sys
import pandas as pd

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.filter_terms_detector_processor import FilterTermsDetectorProcessor


def setup_test_stages():
    """Set up test stages for filter terms detector tests."""
    StageManager.cleanup_stages()
    StageManager.initialize_stages(max_stages=50)


def create_raw_test_data():
    """Create raw test data with filter-able content."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003', 'C004', 'C005', 'C006'],
        'customer_name': ['Alice Corp', 'Bob Industries', 'Charlie Ltd', 'Delta Systems', 'Echo Enterprises', 'Foxtrot Inc'],
        'status': ['Active', 'Active', 'Cancelled', 'Active', 'Pending', 'Cancelled'],
        'notes': [
            'project completed successfully',
            'regular customer with good payment history', 
            'project cancelled due to budget constraints',
            'new customer with high potential',
            'project pending regulatory approval',
            'contract cancelled by customer request'
        ],
        'priority': ['High', 'Medium', 'Low', 'High', 'Medium', 'Low']
    })


def create_filtered_test_data():
    """Create filtered test data - removed cancelled and pending customers."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C004'],
        'customer_name': ['Alice Corp', 'Bob Industries', 'Delta Systems'],
        'status': ['Active', 'Active', 'Active'],
        'notes': [
            'project completed successfully',
            'regular customer with good payment history',
            'new customer with high potential'
        ],
        'priority': ['High', 'Medium', 'High']
    })


def test_basic_filter_detection():
    """Test basic filter terms detection functionality."""
    print("\nTesting basic filter terms detection...")
    
    setup_test_stages()
    
    # Create and save test data
    raw_data = create_raw_test_data()
    filtered_data = create_filtered_test_data()
    
    StageManager.save_stage('stg_raw_customer_data', raw_data, 'Raw customer data')
    StageManager.save_stage('stg_filtered_customer_data', filtered_data, 'Filtered customer data')
    
    # Configure processor
    step_config = {
        'processor_type': 'filter_terms_detector',
        'step_description': 'Test basic filter detection',
        'raw_stage': 'stg_raw_customer_data',
        'filtered_stage': 'stg_filtered_customer_data',
        'text_columns': ['notes'],
        'categorical_columns': ['status', 'priority'],
        'save_to_stage': 'stg_filter_analysis_results'
    }
    
    try:
        processor = FilterTermsDetectorProcessor(step_config)
        result = processor.execute(None)
        
        # Save results for inspection
        StageManager.save_stage('stg_filter_analysis_results', result, 'Filter analysis results')
        
        # Validate results structure
        expected_columns = [
            'Column_Name',
            'Filter_Term',
            'Term_Type',
            'Confidence_Percentage'
        ]
        
        for col in expected_columns:
            if col not in result.columns:
                print(f"✗ Missing expected column: {col}")
                return False
        
        # Check that we found some filter terms
        if len(result) == 0:
            print("✗ No filter terms detected - expected some results")
            return False
        
        # Look for expected patterns
        categorical_results = result[result['Term_Type'] == 'categorical_value']
        text_results = result[result['Term_Type'] == 'text_ngram']
        
        print(f"✓ Found {len(categorical_results)} categorical filter terms")
        print(f"✓ Found {len(text_results)} text filter terms")
        
        # Should detect "Cancelled" and "Pending" status values were removed
        status_terms = categorical_results[categorical_results['Column_Name'] == 'status']
        expected_status_terms = {'Cancelled', 'Pending'}
        found_status_terms = set(status_terms['Filter_Term'])
        
        if not expected_status_terms.issubset(found_status_terms):
            print(f"✗ Expected status terms {expected_status_terms}, found {found_status_terms}")
            return False
        
        print("✓ Basic filter terms detection passed")
        return True
        
    except Exception as e:
        print(f"✗ Filter detection failed: {e}")
        return False


def test_text_ngram_analysis():
    """Test n-gram analysis for text columns."""
    print("\nTesting n-gram analysis...")
    
    setup_test_stages()
    
    # Create data with clear text patterns
    raw_data = pd.DataFrame({
        'id': ['R001', 'R002', 'R003', 'R004', 'R005'],
        'description': [
            'cancelled project due to funding',
            'successful project completion',
            'cancelled project due to delays', 
            'active project in progress',
            'cancelled due to budget constraints'
        ]
    })
    
    # Filtered data removes all "cancelled" entries
    filtered_data = pd.DataFrame({
        'id': ['R002', 'R004'],
        'description': [
            'successful project completion',
            'active project in progress'
        ]
    })
    
    StageManager.save_stage('stg_raw_projects', raw_data, 'Raw project data')
    StageManager.save_stage('stg_filtered_projects', filtered_data, 'Filtered project data')
    
    step_config = {
        'processor_type': 'filter_terms_detector',
        'step_description': 'Test n-gram analysis',
        'raw_stage': 'stg_raw_projects',
        'filtered_stage': 'stg_filtered_projects',
        'text_columns': ['description'],
        'ngram_range': [1, 3],
        'min_frequency': 2,
        'score_threshold': 0.1,
        'save_to_stage': 'stg_ngram_analysis_results'
    }
    
    try:
        processor = FilterTermsDetectorProcessor(step_config)
        result = processor.execute(None)
        
        # Should detect "cancelled" and "cancelled project" as strong filter terms
        text_results = result[result['Term_Type'] == 'text_ngram']
        found_terms = set(text_results['Filter_Term'])
        
        # Look for expected patterns
        expected_patterns = ['cancelled']  # Should definitely find this
        
        found_expected = [term for term in expected_patterns if term in found_terms]
        
        if not found_expected:
            print(f"✗ Expected to find terms like {expected_patterns}, found: {found_terms}")
            return False
        
        print(f"✓ Found expected filter terms: {found_expected}")
        print(f"✓ Total terms detected: {len(text_results)}")
        
        # Check confidence scores are reasonable
        high_confidence_terms = text_results[text_results['Confidence_Percentage'] > 50.0]
        if len(high_confidence_terms) == 0:
            print("✗ Expected some high-confidence terms")
            return False
        
        print(f"✓ Found {len(high_confidence_terms)} high-confidence terms")
        print("✓ N-gram analysis passed")
        return True
        
    except Exception as e:
        print(f"✗ N-gram analysis failed: {e}")
        return False


def test_configuration_validation():
    """Test configuration validation and error handling."""
    print("\nTesting configuration validation...")
    
    # Test missing required fields
    invalid_configs = [
        {
            'processor_type': 'filter_terms_detector',
            # Missing raw_stage, filtered_stage, text_columns
        },
        {
            'processor_type': 'filter_terms_detector',
            'raw_stage': 'test_raw',
            'filtered_stage': 'test_filtered',
            'text_columns': 123  # Should be string or list, not int
        },
        {
            'processor_type': 'filter_terms_detector', 
            'raw_stage': 'test_raw',
            'filtered_stage': 'test_filtered',
            'text_columns': ['notes'],
            'ngram_range': [1, 7]  # Too high, max should be 6
        }
    ]
    
    success_count = 0
    
    for i, config in enumerate(invalid_configs):
        try:
            processor = FilterTermsDetectorProcessor(config)
            print(f"✗ Config {i+1} should have failed validation")
        except StepProcessorError as e:
            print(f"✓ Config {i+1} correctly failed: {e}")
            success_count += 1
        except Exception as e:
            print(f"✗ Config {i+1} failed with unexpected error: {e}")
    
    print(f"✓ Configuration validation: {success_count}/{len(invalid_configs)} tests passed")
    return success_count == len(invalid_configs)


def test_empty_data_handling():
    """Test handling of empty or insufficient data."""
    print("\nTesting empty data handling...")
    
    setup_test_stages()
    
    # Create minimal data that won't produce meaningful results
    minimal_raw = pd.DataFrame({
        'id': ['1'],
        'notes': ['short']
    })
    
    minimal_filtered = pd.DataFrame({
        'id': ['1'], 
        'notes': ['short']
    })
    
    StageManager.save_stage('stg_minimal_raw', minimal_raw, 'Minimal raw data')
    StageManager.save_stage('stg_minimal_filtered', minimal_filtered, 'Minimal filtered data')
    
    step_config = {
        'processor_type': 'filter_terms_detector',
        'step_description': 'Test minimal data handling',
        'raw_stage': 'stg_minimal_raw',
        'filtered_stage': 'stg_minimal_filtered',
        'text_columns': ['notes'],
        'save_to_stage': 'stg_minimal_results'
    }
    
    try:
        processor = FilterTermsDetectorProcessor(step_config)
        result = processor.execute(None)
        
        # Should handle gracefully and return empty results
        if not isinstance(result, pd.DataFrame):
            print("✗ Should return DataFrame even with no results")
            return False
        
        print(f"✓ Handled minimal data gracefully, returned {len(result)} results")
        return True
        
    except Exception as e:
        print(f"✗ Failed to handle minimal data: {e}")
        return False


def main():
    """Run all tests for FilterTermsDetectorProcessor."""
    print("=== Testing FilterTermsDetectorProcessor ===")
    
    test_results = []
    
    # Run individual tests
    test_results.append(test_basic_filter_detection())
    test_results.append(test_text_ngram_analysis())
    test_results.append(test_configuration_validation())
    test_results.append(test_empty_data_handling())
    
    # Calculate results
    passed_tests = sum(test_results)
    total_tests = len(test_results)
    
    print(f"\n=== Test Summary ===")
    print(f"Passed: {passed_tests}/{total_tests} tests")
    
    if passed_tests == total_tests:
        print("✓ All FilterTermsDetectorProcessor tests passed!")
        return True
    else:
        print("✗ Some tests failed")
        return False


if __name__ == '__main__':
    success = main()
    exit_code = 0 if success else 1
    sys.exit(exit_code)

# End of file #
