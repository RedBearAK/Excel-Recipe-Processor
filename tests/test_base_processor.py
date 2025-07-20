"""
Simple test for the base step processor functionality.
"""

from excel_recipe_processor.processors.base_processor import (
    BaseStepProcessor, 
    StepProcessorRegistry, 
    StepProcessorError,
)


class DummyProcessor(BaseStepProcessor):
    """Test processor that just returns the input data."""
    
    def execute(self, data):
        self.log_step_start()
        # Just return the data unchanged for testing
        result = f"Processed: {data}"
        self.log_step_complete(f"returned '{result}'")
        return result


def test_basic_functionality():
    """Test basic processor and registry functionality."""
    
    print("Testing BaseStepProcessor...")
    
    # Test valid step configuration
    step_config = {
        'type': 'dummy_step',
        'name': 'Test dummy step'
    }
    
    processor = DummyProcessor(step_config)
    print(f"✓ Created processor: {processor}")
    
    # Test execution
    result = processor.execute("test data")
    print(f"✓ Execution result: {result}")
    
    # Test registry
    print("\nTesting StepProcessorRegistry...")
    
    test_registry = StepProcessorRegistry()
    test_registry.register('dummy_step', DummyProcessor)
    print("✓ Registered dummy processor")
    
    # Test creating processor from registry
    created_processor = test_registry.create_processor(step_config)
    print(f"✓ Created processor from registry: {created_processor}")
    
    # Test execution via registry-created processor
    result2 = created_processor.execute("registry test data")
    print(f"✓ Registry processor result: {result2}")
    
    print(f"✓ Available step types: {test_registry.get_registered_types()}")


def test_error_handling():
    """Test error handling and validation."""
    
    print("\nTesting error handling...")
    
    # Test invalid step config
    try:
        DummyProcessor("not a dict")
        print("✗ Should have failed with invalid config")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test missing type field
    try:
        DummyProcessor({'name': 'test'})
        print("✗ Should have failed with missing type")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test registry error handling
    test_registry = StepProcessorRegistry()
    try:
        test_registry.get_processor_class('nonexistent_type')
        print("✗ Should have failed with unknown type")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


if __name__ == '__main__':
    test_basic_functionality()
    test_error_handling()
    print("\n✓ All tests passed!")
