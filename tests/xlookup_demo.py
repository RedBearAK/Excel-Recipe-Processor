"""
Demonstration of XLOOKUP-equivalent functionality with LookupDataProcessor.

Shows how the processor provides all the key XLOOKUP features:
- Lookup in any direction
- Return multiple values
- Default values for non-matches
- Exact and flexible matching
- Search order control
"""

import pandas as pd
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def demo_xlookup_capabilities():
    """Demonstrate XLOOKUP-equivalent capabilities."""
    
    print("🔍 XLOOKUP-Equivalent Capabilities Demo\n")
    
    # Sample data - orders that need product and customer info
    orders = pd.DataFrame({
        'Order_ID': ['ORD001', 'ORD002', 'ORD003', 'ORD004', 'ORD005'],
        'Product_ID': ['P001', 'P002', 'P999', 'P001', 'P003'],  # P999 doesn't exist
        'Customer_ID': ['C100', 'C200', 'C100', 'C300', 'C999'],  # C999 doesn't exist
        'Quantity': [5, 3, 2, 1, 4]
    })
    
    # Product lookup table 
    products = pd.DataFrame({
        'Product_ID': ['P001', 'P002', 'P003', 'P001'],  # P001 appears twice (duplicate)
        'Product_Name': ['Widget A', 'Gadget B', 'Tool C', 'Widget A v2'],
        'Category': ['Electronics', 'Tools', 'Hardware', 'Electronics'],
        'Unit_Price': [25.50, 45.00, 12.75, 27.00],
        'Supplier': ['TechCorp', 'ToolCo', 'HardwareInc', 'TechCorp']
    })
    
    # Customer lookup table
    customers = pd.DataFrame({
        'Customer_ID': ['C100', 'C200', 'C300'],
        'Company_Name': ['Acme Corp', 'Beta Industries', 'Gamma LLC'],
        'Country': ['USA', 'Canada', 'Mexico'],
        'Credit_Rating': ['A', 'B+', 'A-']
    })
    
    print(f"📊 Sample Data:")
    print(f"Orders: {len(orders)} rows")
    print(f"Products: {len(products)} rows (with duplicate P001)")
    print(f"Customers: {len(customers)} rows")
    print()
    
    # XLOOKUP Feature 1: Lookup in any direction + Multiple return values
    print("🚀 XLOOKUP Feature 1: Multi-column lookup (any direction)")
    step_config1 = {
        'processor_type': 'lookup_data',
        'step_description': 'Enrich with product details',
        'lookup_source': products,
        'lookup_key': 'Product_ID',
        'source_key': 'Product_ID',
        'lookup_columns': ['Product_Name', 'Category', 'Unit_Price'],  # Multiple columns
        'handle_duplicates': 'last'  # Take latest version
    }
    
    processor1 = LookupDataProcessor(step_config1)
    enriched_orders = processor1.execute(orders)
    
    print("Results:")
    for i in range(3):
        order_id = enriched_orders.iloc[i]['Order_ID']
        product_name = enriched_orders.iloc[i]['Product_Name']
        category = enriched_orders.iloc[i]['Category']
        price = enriched_orders.iloc[i]['Unit_Price']
        print(f"  {order_id}: {product_name} ({category}) - ${price}")
    print()
    
    # XLOOKUP Feature 2: Default values for non-matches
    print("🛡️ XLOOKUP Feature 2: Default values for non-matches")
    step_config2 = {
        'processor_type': 'lookup_data',
        'step_description': 'Add customer info with defaults',
        'lookup_source': customers,
        'lookup_key': 'Customer_ID',
        'source_key': 'Customer_ID',
        'lookup_columns': ['Company_Name', 'Country', 'Credit_Rating'],
        'default_values': {
            'Company_Name': 'Unknown Customer',
            'Country': 'Unknown',
            'Credit_Rating': 'Unrated'
        }
    }
    
    processor2 = LookupDataProcessor(step_config2)
    final_orders = processor2.execute(enriched_orders)
    
    print("Results (note C999 gets default values):")
    for i in range(len(final_orders)):
        order_id = final_orders.iloc[i]['Order_ID']
        customer_id = final_orders.iloc[i]['Customer_ID']
        company = final_orders.iloc[i]['Company_Name']
        rating = final_orders.iloc[i]['Credit_Rating']
        print(f"  {order_id}: Customer {customer_id} = {company} (Rating: {rating})")
    print()
    
    # XLOOKUP Feature 3: Case-insensitive matching
    print("🔤 XLOOKUP Feature 3: Case-insensitive matching")
    mixed_case_orders = pd.DataFrame({
        'Product_Code': ['p001', 'P002', 'p003'],  # Mixed case
        'Description': ['Widget', 'Gadget', 'Tool']
    })
    
    lookup_table = pd.DataFrame({
        'Code': ['P001', 'P002', 'P003'],  # Uppercase
        'Status': ['Active', 'Discontinued', 'Active']
    })
    
    step_config3 = {
        'processor_type': 'lookup_data',
        'step_description': 'Case-insensitive lookup',
        'lookup_source': lookup_table,
        'lookup_key': 'Code',
        'source_key': 'Product_Code',
        'lookup_columns': ['Status'],
        'case_sensitive': False  # XLOOKUP-style flexible matching
    }
    
    processor3 = LookupDataProcessor(step_config3)
    result = processor3.execute(mixed_case_orders)
    
    print("Results (lowercase matches uppercase):")
    for i in range(len(result)):
        code = result.iloc[i]['Product_Code']
        status = result.iloc[i]['Status']
        print(f"  {code} → {status}")
    print()
    
    # XLOOKUP Feature 4: Join types (like XLOOKUP's search modes)
    print("🔗 XLOOKUP Feature 4: Different join types")
    
    # Inner join = only exact matches (like XLOOKUP exact match)
    step_config4 = {
        'processor_type': 'lookup_data',
        'step_description': 'Inner join - exact matches only',
        'lookup_source': products,
        'lookup_key': 'Product_ID',
        'source_key': 'Product_ID',
        'lookup_columns': ['Product_Name'],
        'join_type': 'inner'
    }
    
    processor4 = LookupDataProcessor(step_config4)
    exact_matches = processor4.execute(orders)
    
    print(f"Inner join (exact matches only): {len(orders)} → {len(exact_matches)} rows")
    print(f"  Filtered out orders with non-existent Product_ID")
    print()
    
    print("✅ XLOOKUP-Equivalent Features Demonstrated:")
    print("  🔄 Multi-direction lookup (any column)")
    print("  📋 Multiple return values")
    print("  🛡️ Default values for non-matches")
    print("  🔤 Case-insensitive matching")
    print("  🎯 Duplicate handling (first/last)")
    print("  🔗 Different join types")
    print("  📊 Array-style operations")
    print()
    print("🎉 This processor provides XLOOKUP-equivalent functionality!")


if __name__ == '__main__':
    demo_xlookup_capabilities()
