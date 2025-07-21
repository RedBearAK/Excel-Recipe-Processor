# Excel Recipe Processor

🚀 **Automated Excel data processing with YAML/JSON recipes**

Transform your Excel automation workflows into reusable, version-controlled recipes. No more manual clicking through Excel - define your data processing steps once and run them repeatedly with consistent results.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 **What It Does**

Excel Recipe Processor automates complex Excel data transformations using simple YAML configuration files. Perfect for:

- **Business Report Automation** - Transform raw data exports into polished reports
- **Data Pipeline Integration** - Reliable, repeatable Excel processing in data workflows  
- **ETL Operations** - Extract, transform, and load Excel data with consistency
- **Report Standardization** - Ensure consistent data processing across teams

## ⚡ **Quick Start**

### Installation

```bash
pip install excel-recipe-processor
```

### Your First Recipe

Create a recipe file `my_recipe.yaml`:

```yaml
recipe:
  - step_description: "Filter for electronics products"
    processor_type: "filter_data"
    filters:
      - column: "Category"
        condition: "equals" 
        value: "Electronics"
        
  - step_description: "Calculate total value"
    processor_type: "add_calculated_column"
    new_column: "Total_Value"
    calculation_type: "math"
    calculation:
      operation: "multiply"
      column1: "Quantity"
      column2: "Price"
      
  - step_description: "Sort by total value"
    processor_type: "sort_data"
    columns: ["Total_Value"]
    ascending: false

settings:
  output_filename: "electronics_report.xlsx"
```

Run your recipe:

```python
from excel_recipe_processor import ExcelPipeline

pipeline = ExcelPipeline()
result = pipeline.run_complete_pipeline(
    recipe_path="my_recipe.yaml",
    input_path="raw_data.xlsx", 
    output_path="electronics_report.xlsx"
)
```

## 🔧 **Available Processors**

### Data Filtering & Selection
- **`filter_data`** - Filter rows by conditions (equals, contains, greater than, etc.)
- **`group_data`** - Group individual values into categories

### Data Transformation
- **`add_calculated_column`** - Create new columns with formulas and calculations
- **`clean_data`** - Clean and standardize data (replace values, fix formatting, etc.)
- **`split_column`** - Split single columns into multiple columns
- **`rename_columns`** - Rename columns with mapping, patterns, or transformations

### Data Analysis & Aggregation  
- **`aggregate_data`** - Group by columns and apply aggregation functions
- **`pivot_table`** - Create pivot tables with various aggregation options
- **`sort_data`** - Sort by single or multiple columns with custom orders
- **`lookup_data`** - VLOOKUP/XLOOKUP style data enrichment

## 📋 **Recipe Examples**

### Sales Report Automation
```yaml
recipe:
  # Clean and standardize data
  - step_description: "Clean product names"
    processor_type: "clean_data"
    rules:
      - column: "Product_Name"
        action: "uppercase"
      - column: "Price"
        action: "fix_numeric"
        
  # Filter for active products only
  - step_description: "Filter active products"
    processor_type: "filter_data"
    filters:
      - column: "Status"
        condition: "equals"
        value: "Active"
        
  # Enrich with category data
  - step_description: "Add product categories"
    processor_type: "lookup_data"
    lookup_source: "product_catalog.xlsx"
    lookup_key: "Product_Code"
    source_key: "Product_Code"
    lookup_columns: ["Category", "Margin"]
    
  # Calculate metrics
  - step_description: "Calculate revenue"
    processor_type: "add_calculated_column"
    new_column: "Revenue"
    calculation_type: "math"
    calculation:
      operation: "multiply"
      column1: "Quantity"
      column2: "Price"
      
  # Create summary by region and category
  - step_description: "Summarize by region and category"
    processor_type: "aggregate_data"
    group_by: ["Region", "Category"]
    aggregations:
      - column: "Revenue"
        function: "sum"
        new_column_name: "Total_Revenue"
      - column: "Quantity"
        function: "sum"
        new_column_name: "Total_Units"
      - column: "Order_ID"
        function: "nunique"
        new_column_name: "Unique_Orders"
```

### Data Cleaning Pipeline
```yaml
recipe:
  # Split customer names
  - step_description: "Split customer names"
    processor_type: "split_column"
    source_column: "Customer_Name"
    split_type: "delimiter"
    delimiter: ","
    new_column_names: ["Last_Name", "First_Name"]
    remove_original: true
    
  # Standardize phone numbers
  - step_description: "Clean phone numbers"
    processor_type: "clean_data"
    rules:
      - column: "Phone"
        action: "regex_replace"
        pattern: "[^0-9]"
        replacement: ""
        
  # Group states into regions
  - step_description: "Group states into regions"
    processor_type: "group_data"
    source_column: "State"
    target_column: "Region"
    groups:
      West: ["CA", "OR", "WA", "NV"]
      East: ["NY", "MA", "CT", "NJ"]
      South: ["TX", "FL", "GA", "NC"]
      
  # Rename columns for consistency
  - step_description: "Standardize column names"
    processor_type: "rename_columns"
    rename_type: "transform"
    case_conversion: "snake_case"
    replace_spaces: "_"
```

## 🏗️ **Recipe Structure**

### Basic Recipe Format
```yaml
recipe:
  - step_description: "Human readable step description"
    processor_type: "processor_name"
    # Processor-specific configuration
    
settings:
  output_filename: "result.xlsx"
  create_backup: true  # Optional: backup original file
```

### Step Configuration

Each step must have:
- **`step_description`** - Human-readable description
- **`processor_type`** - Which processor to use
- **Processor-specific fields** - Configuration for the chosen processor

### Common Options
- **`remove_original`** - Remove source columns after transformation
- **`overwrite`** - Overwrite existing columns  
- **`fill_missing`** - Value to use for missing data

## 🔍 **Processor Details**

<details>
<summary><strong>filter_data</strong> - Filter rows based on conditions</summary>

```yaml
processor_type: "filter_data"
filters:
  - column: "Price"
    condition: "greater_than"
    value: 100
  - column: "Category"
    condition: "in_list"
    value: ["Electronics", "Tools"]
```

**Supported conditions:** `equals`, `not_equals`, `contains`, `not_contains`, `greater_than`, `less_than`, `greater_equal`, `less_equal`, `in_list`, `not_in_list`, `is_empty`, `not_empty`
</details>

<details>
<summary><strong>add_calculated_column</strong> - Create new columns with calculations</summary>

```yaml
processor_type: "add_calculated_column"
new_column: "Profit_Margin"
calculation_type: "math"
calculation:
  operation: "subtract"
  column1: "Revenue"
  column2: "Cost"
```

**Calculation types:** `math`, `conditional`, `concat`, `date`, `text`, `expression`  
**Math operations:** `add`, `subtract`, `multiply`, `divide`, `sum`, `mean`, `min`, `max`
</details>

<details>
<summary><strong>aggregate_data</strong> - Group and summarize data</summary>

```yaml
processor_type: "aggregate_data"
group_by: ["Region", "Product_Type"]
aggregations:
  - column: "Sales"
    function: "sum"
    new_column_name: "Total_Sales"
  - column: "Orders"
    function: "count"
    new_column_name: "Order_Count"
```

**Functions:** `sum`, `mean`, `median`, `min`, `max`, `count`, `nunique`, `std`, `var`, `first`, `last`
</details>

<details>
<summary><strong>split_column</strong> - Split columns into multiple columns</summary>

```yaml
processor_type: "split_column"
source_column: "Full_Name"
split_type: "delimiter"
delimiter: ","
new_column_names: ["Last_Name", "First_Name"]
max_splits: 1
```

**Split types:** `delimiter`, `fixed_width`, `regex`, `position`
</details>

<details>
<summary><strong>lookup_data</strong> - Enrich data with lookups</summary>

```yaml
processor_type: "lookup_data"
lookup_source: "products.xlsx"
lookup_key: "Product_Code"
source_key: "Product_ID"
lookup_columns: ["Product_Name", "Category", "Price"]
join_type: "left"
```

**Join types:** `left`, `inner`, `outer`  
**Data sources:** Excel files, CSV files, dictionaries, DataFrames
</details>

## 🛠️ **Advanced Features**

### Recipe Validation
```python
from excel_recipe_processor.validation import RecipeValidator

validator = RecipeValidator()
result = validator.validate_recipe_file("my_recipe.yaml")
validator.print_validation_report(result)
```

### System Capabilities
```python
from excel_recipe_processor import get_system_capabilities

capabilities = get_system_capabilities()
print(f"Available processors: {capabilities['system_info']['total_processors']}")
```

### Programmatic Usage
```python
from excel_recipe_processor import ExcelPipeline

# Step-by-step execution
pipeline = ExcelPipeline()
pipeline.load_recipe("recipe.yaml")
pipeline.load_input_file("data.xlsx")
result = pipeline.execute_recipe()
pipeline.save_result("output.xlsx")

# Pipeline summary
summary = pipeline.get_pipeline_summary()
```

### Error Handling
```python
from excel_recipe_processor import PipelineError

try:
    result = pipeline.run_complete_pipeline(
        recipe_path="recipe.yaml",
        input_path="data.xlsx", 
        output_path="result.xlsx"
    )
except PipelineError as e:
    print(f"Pipeline failed: {e}")
```

## 📊 **Use Cases**

### Business Intelligence
- **Monthly Sales Reports** - Automate recurring report generation
- **KPI Dashboards** - Transform raw data into dashboard-ready formats
- **Financial Analysis** - Standardize financial data processing

### Data Operations  
- **ETL Pipelines** - Reliable Excel processing in data workflows
- **Data Quality** - Consistent cleaning and validation rules
- **Report Distribution** - Automated report generation and formatting

### Team Collaboration
- **Standardized Processes** - Share processing recipes across teams
- **Version Control** - Track changes to data processing logic
- **Documentation** - Self-documenting data transformation workflows

## 🧪 **Development**

### Running Tests
```bash
# Run all tests
pytest

# Run specific processor tests
python tests/test_filter_data_processor.py
python tests/test_aggregate_data_processor.py
```

### Project Structure
```
excel_recipe_processor/
├── config/          # Recipe loading and validation
├── core/           # Pipeline orchestration
├── processors/     # Data processing modules
├── readers/        # Excel file reading
├── writers/        # Excel file writing
└── tests/          # Comprehensive test suite
```

### Adding New Processors

1. **Create processor class** inheriting from `BaseStepProcessor`
2. **Implement `execute()` method** with your processing logic  
3. **Add validation** and error handling
4. **Register processor** in pipeline
5. **Write comprehensive tests**

Example processor skeleton:
```python
from excel_recipe_processor.processors.base_processor import BaseStepProcessor

class MyProcessor(BaseStepProcessor):
    def execute(self, data):
        self.log_step_start()
        self.validate_data_not_empty(data)
        self.validate_required_fields(['required_field'])
        
        # Your processing logic here
        result = process_data(data)
        
        self.log_step_complete("processing info")
        return result
```

## 📝 **Recipe Library**

### Common Patterns

**Clean Survey Data:**
```yaml
recipe:
  - processor_type: "clean_data"
    rules:
      - column: "Response"
        action: "strip_whitespace"
      - column: "Age"
        action: "fix_numeric"
        
  - processor_type: "filter_data"
    filters:
      - column: "Age"
        condition: "greater_than"
        value: 0
```

**Sales Territory Analysis:**
```yaml
recipe:
  - processor_type: "group_data"
    source_column: "State"
    target_column: "Territory" 
    groups:
      Northeast: ["NY", "MA", "CT"]
      Southeast: ["FL", "GA", "NC"]
      
  - processor_type: "aggregate_data"
    group_by: "Territory"
    aggregations:
      - column: "Revenue"
        function: "sum"
```

**Customer Data Enrichment:**
```yaml
recipe:
  - processor_type: "split_column"
    source_column: "Customer_Name"
    split_type: "delimiter"
    delimiter: ", "
    new_column_names: ["Last_Name", "First_Name"]
    
  - processor_type: "lookup_data"
    lookup_source: "customer_segments.xlsx"
    lookup_key: "Customer_ID"
    source_key: "Customer_ID"
    lookup_columns: ["Segment", "Tier"]
```

## 🤝 **Contributing**

Contributions welcome! Please:

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-processor`)
3. **Add tests** for new functionality
4. **Follow existing code patterns** 
5. **Submit a pull request**

### Code Guidelines
- Use native Python types (`list`, `dict`) instead of `typing` imports
- Follow existing error handling patterns
- Add comprehensive test coverage
- Include docstrings for public methods

## 📄 **License**

GNU General Public License 3.0 - see LICENSE file for details.

## 🔗 **Links**

- **Documentation:** [Full documentation](https://github.com/your-repo/docs)
- **Issues:** [Report bugs](https://github.com/your-repo/issues)
