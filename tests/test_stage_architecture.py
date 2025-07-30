from excel_recipe_processor.core.stage_manager import StageManager


def test_new_stage_methods():
    """Test that new StageManager methods work."""
    
    StageManager.initialize_stages()
    
    try:
        recipe_config = {
            'settings': {
                'stages': [
                    {'stage_name': 'test_stage', 'description': 'Test stage', 'protected': True}
                ]
            }
        }
        
        # Test declaration
        StageManager.declare_recipe_stages(recipe_config)
        
        if StageManager.is_stage_declared('test_stage'):
            print("✓ Stage declaration working")
        else:
            print("✗ Stage declaration failed")
            return False
        
        if StageManager.is_stage_protected('test_stage'):
            print("✓ Stage protection working")
        else:
            print("✗ Stage protection failed")
            return False
        
        return True
        
    finally:
        StageManager.cleanup_stages()

def test_recipe_pipeline():
    """Test new RecipePipeline class."""
    
    try:
        from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
        
        pipeline = RecipePipeline()
        print("✓ RecipePipeline created successfully")
        return True
        
    except ImportError as e:
        print(f"✗ RecipePipeline import failed: {e}")
        return False


if __name__ == '__main__':
    print("🚀 Stage Architecture Implementation Tests")
    print("=" * 50)
    
    # Initialize StageManager for all tests
    from excel_recipe_processor.core.stage_manager import StageManager
    StageManager.initialize_stages(max_stages=25)
    
    tests = [
        test_new_stage_methods,
        test_recipe_pipeline
    ]
    
    results = []
    for test_func in tests:
        print(f"\nRunning {test_func.__name__}...")
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"✗ Test {test_func.__name__} failed with exception: {e}")
            results.append(False)
    
    # Cleanup
    StageManager.cleanup_stages()
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All stage architecture tests passed!")
        print("Ready to proceed with full implementation!")
    else:
        print("❌ Some tests failed - check implementation before proceeding")
    
    exit(0 if passed == total else 1)
