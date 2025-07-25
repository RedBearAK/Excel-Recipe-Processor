import os

from excel_recipe_processor.config.recipe_loader import RecipeLoader


test_dir = os.path.dirname(os.path.abspath(__file__))

# Test basic loading
loader = RecipeLoader()
data = loader.load_file(os.path.join(test_dir, 'test_recipe.yaml'))

print(loader.summary())
print("Steps:", len(loader.get_steps()))
print("First step type:", loader.get_steps()[0]['processor_type'])
