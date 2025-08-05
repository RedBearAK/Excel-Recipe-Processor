import os

from excel_recipe_processor.config.recipe_loader import RecipeLoader


test_dir = os.path.dirname(os.path.abspath(__file__))

# Test basic loading
recipe_loader = RecipeLoader()
data = recipe_loader.load_recipe_file(os.path.join(test_dir, 'test_recipe.yaml'))

print(recipe_loader.summary())
print("Steps:", len(recipe_loader.get_steps()))
print("First step type:", recipe_loader.get_steps()[0]['processor_type'])
