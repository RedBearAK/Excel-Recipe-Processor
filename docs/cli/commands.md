# Command line

Generated from `python -m excel_recipe_processor --help`. Regenerate after any CLI change.

```
usage: excel-recipe-processor [-h] [--version] [--var NAME=VALUE]
                              [--set NAME VALUE] [--dump-stage NAME[:SPEC]]
                              [--dump-dir DIR] [--stop-after STAGE]
                              [--list-stages RECIPE] [--log-file PATH]
                              [--verbose] [--validate] [--list-capabilities]
                              [--detailed] [--json] [--export-docs DIR]
                              [--export-schemas FORMAT] [--yaml]
                              [--detailed-yaml] [--matrix]
                              [--validate-recipe RECIPE.yaml]
                              [--get-usage-examples [PROCESSOR_NAME]]
                              [--format-examples {yaml,text,json}]
                              [--get-settings-examples]
                              [RECIPE.yaml]

Automate complex manual Excel workflows as YAML-configured recipes: import, clean, enrich, verify, and export with live formulas, named ranges, and professional formatting

Process data using YAML recipes with dynamic variables and stage-based architecture.

positional arguments:
  RECIPE.yaml           YAML recipe file defining processing steps with
                        import_file and export_file processors

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --var NAME=VALUE      Override external variable (repeatable). Example:
                        --var batch_id=A47 --var region=west
  --set NAME VALUE      Override external variable, value as a separate
                        argument so paths tab-complete. Example: --set
                        source_download downloads/latest.xlsx
  --dump-stage NAME[:SPEC]
                        Write a stage to CSV as it is produced, then carry on
                        (repeatable). SPEC selects rows: 20 first, -20 last,
                        100-150 a range, 20,-20 both ends. Omit SPEC for every
                        row. Example: --dump-stage stg_enriched:20
  --dump-dir DIR        Where --dump-stage writes its CSVs (default: current
                        directory)
  --stop-after STAGE    Halt once this stage has been written. Pairs with
                        --dump-stage to avoid running the rest of a pipeline
                        you are not inspecting yet.
  --list-stages RECIPE  Print the stages a recipe declares, with descriptions,
                        and exit. Tells you what to ask --dump-stage for
                        without reading the YAML.
  --log-file PATH       Mirror the run log to this file (same content as the
                        terminal, UTF-8)
  --verbose, -v         Enable verbose output and debug logging
  --validate            Load the recipe, resolve variables, validate every
                        step against its processor schema and check the stage
                        graph, then stop without running. Exit 1 on errors;
                        warnings alone exit 0. The same checks run at the
                        start of every real run.
  --list-capabilities   List all available processors and their capabilities
  --detailed            Show detailed capabilities (use with --list-
                        capabilities)
  --json                Output capabilities as JSON (use with --list-
                        capabilities)
  --export-docs DIR     Write one generated Markdown page per processor
                        (description, declared keys, validated examples) plus
                        an index into DIR, e.g. docs/processors
  --export-schemas FORMAT
                        Print every processor's declared step schema
                        (families, keys, kinds, required, defaults, choices,
                        variants) as json or md - the reference to read before
                        writing a recipe or a new processor
  --yaml                Output capabilities as YAML (use with --list-
                        capabilities)
  --detailed-yaml       Show detailed capabilities with YAML listings (use
                        with --list-capabilities)
  --matrix              Show feature matrix (use with --list-capabilities)
  --validate-recipe RECIPE.yaml
                        Validate recipe file syntax and processor availability
  --get-usage-examples [PROCESSOR_NAME]
                        Show usage examples for specific processor or all
                        processors. Use "settings" as the name for recipe
                        settings examples
  --format-examples {yaml,text,json}
                        Format for usage examples output (default: yaml)
  --get-settings-examples
                        Show recipe settings configuration examples

examples:
  BASIC RECIPE PROCESSING:
    # Process recipe with external variables from CLI
    excel-recipe-processor recipe.yaml --var batch_id=A47 --var region=west
    
    # Process recipe with interactive prompting for missing variables
    excel-recipe-processor daily_report.yaml
    
    # Combine CLI variables with interactive prompting for others
    excel-recipe-processor report.yaml --var batch_id=A47
    
    # Complex variables with spaces and special characters
    excel-recipe-processor recipe.yaml --var "description=Q4 Sales Report" --var dept=FINANCE

  DEBUGGING AND DEVELOPMENT:
    # Verbose output for debugging recipe execution
    excel-recipe-processor recipe.yaml --var date=20250729 --verbose
    
    # Validate recipe syntax before processing
    excel-recipe-processor --validate-recipe recipe.yaml
    
    # Validate multiple recipes
    excel-recipe-processor --validate-recipe sales.yaml
    excel-recipe-processor --validate-recipe finance.yaml

  SYSTEM INFORMATION:
    # List all available processors
    excel-recipe-processor --list-capabilities
    
    # Detailed processor information
    excel-recipe-processor --list-capabilities --detailed
    
    # Output capabilities in different formats
    excel-recipe-processor --list-capabilities --json
    excel-recipe-processor --list-capabilities --yaml
    excel-recipe-processor --list-capabilities --detailed-yaml
    
    # Feature comparison matrix
    excel-recipe-processor --list-capabilities --matrix
    
    # Save capabilities to files for documentation
    excel-recipe-processor --list-capabilities --json > capabilities.json
    excel-recipe-processor --list-capabilities --yaml > capabilities.yaml

  USAGE EXAMPLES AND HELP:
    # Get examples for specific processor
    excel-recipe-processor --get-usage-examples import_file
    excel-recipe-processor --get-usage-examples export_file
    excel-recipe-processor --get-usage-examples filter_data
    
    # Get examples for all processors
    excel-recipe-processor --get-usage-examples
    
    # Get examples in different formats
    excel-recipe-processor --get-usage-examples import_file --format-examples yaml
    excel-recipe-processor --get-usage-examples export_file --format-examples text
    excel-recipe-processor --get-usage-examples --format-examples json
    
    # Get recipe settings examples (both forms below are equivalent)
    excel-recipe-processor --get-usage-examples settings
    excel-recipe-processor --get-settings-examples

  ADVANCED SCENARIOS:
    # Process recipe with date-based variables
    excel-recipe-processor monthly.yaml --var month=12 --var year=2024
    
    # Process with multiple batch identifiers
    excel-recipe-processor batch.yaml --var batch_id=A47 --var sub_batch=001
    
    # Process with region-specific settings
    excel-recipe-processor regional.yaml --var region=west --var timezone=PST
    
    # Debug complex recipes with verbose output
    excel-recipe-processor complex.yaml --var env=prod --verbose

  RECIPE EXAMPLES:
    # Simple data processing recipe
    excel-recipe-processor simple_filter.yaml --var input_date=20250729
    
    # Multi-file processing with lookups
    excel-recipe-processor lookup_report.yaml --var quarter=Q4 --var dept=sales
    
    # Automated daily report generation
    excel-recipe-processor daily_report.yaml --var region=west --var format=xlsx

note: External variables can be defined in recipes with validation, defaults, and choices.
      If required variables are missing from CLI, you'll be prompted interactively.
      Use --validate-recipe to check recipe syntax before processing.

For detailed documentation and more examples:
  https://github.com/yourusername/excel-recipe-processor
```
