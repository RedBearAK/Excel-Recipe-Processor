"""Main functionality for excel_recipe_processor package."""

import yaml
import logging

from pathlib import Path
from argparse import Namespace

from excel_recipe_processor.core.pipeline import get_system_capabilities  # Keep for compatibility
from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.recipe_pipeline import RecipePipeline, RecipePipelineError
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
from excel_recipe_processor.core.interactive_variables import (
    InteractiveVariablePrompt,
    InteractiveVariableError,
    parse_cli_variables
)

# Set up logging
from excel_recipe_processor.core.log_format import q, now_stamp

logger = logging.getLogger(__name__)


def list_recipe_stages(recipe_path: str) -> int:
    """
    Print the stages a recipe declares, without running it.

    Answers "what can I ask --dump-stage for" without reading the YAML.

    Args:
        recipe_path: Recipe to inspect

    Returns:
        Exit code
    """
    path = Path(recipe_path)

    if not path.exists():
        print(f"Recipe file not found: {recipe_path}")
        return 1

    try:
        recipe = yaml.safe_load(path.read_text())
    except yaml.YAMLError as error:
        print(f"Could not parse recipe: {error}")
        return 1

    settings = recipe.get('settings', {}) or {}
    declared = settings.get('stages', []) or []
    steps = recipe.get('recipe', []) or []

    # Which step writes each stage, so a name can be traced to its origin
    written_by = {}
    for position, step in enumerate(steps, start=1):
        target = step.get('save_to_stage') or step.get('stage_name')
        if target and target not in written_by:
            written_by[target] = position

    print(f"\n{path.name}: {len(steps)} steps, {len(declared)} declared stages\n")

    for entry in declared:
        name = entry.get('stage_name', '?')
        description = entry.get('description', '')
        position = written_by.get(name)
        marker = f"step {position:>2}" if position else "   --  "
        print(f"  {marker}  {name}")
        if description:
            print(f"            {description}")

    undeclared = [
        name for name in written_by
        if not any(e.get('stage_name') == name for e in declared)
    ]

    if undeclared:
        print(f"\n  Written but not declared: {', '.join(sorted(undeclared))}")

    print("\nDump one with:   --dump-stage <name>[:20|-20|100-150|20,-20]")
    print("Stop there with: --stop-after <name>\n")

    return 0


def run_main(args: Namespace) -> int:
    """
    Main entry point for the package functionality.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Handle special commands first (before setting up logging)
        if getattr(args, 'list_stages_recipe', None):
            return list_recipe_stages(args.list_stages_recipe)

        if hasattr(args, 'list_capabilities') and args.list_capabilities:
            # Check for output format flags
            detailed = getattr(args, 'detailed', False)
            json_output = getattr(args, 'json', False)
            yaml_output = getattr(args, 'yaml', False)
            detailed_yaml = getattr(args, 'detailed_yaml', False)
            matrix = getattr(args, 'matrix', False)
            
            if json_output:
                return list_system_capabilities_json()
            elif yaml_output:
                return list_system_capabilities_yaml()
            elif detailed_yaml:
                return list_system_capabilities_detailed_yaml()
            elif detailed:
                return list_system_capabilities_detailed()
            elif matrix:
                return list_system_capabilities_matrix()
            else:
                return list_system_capabilities()  # Basic format
        
        # Handle settings examples command
        if hasattr(args, 'get_settings_examples') and args.get_settings_examples:
            format_type = getattr(args, 'format_examples', 'yaml')
            return get_settings_examples(format_type)
        
        # Handle usage examples command
        if hasattr(args, 'get_usage_examples') and args.get_usage_examples:
            processor_name = args.get_usage_examples
            format_type = getattr(args, 'format_examples', 'yaml')
            return get_usage_examples(processor_name, format_type)
        
        # Handle recipe validation
        if hasattr(args, 'validate_recipe') and args.validate_recipe:
            return validate_recipe_file(args.validate_recipe)
        
        # Set up logging level
        if hasattr(args, 'verbose') and args.verbose:
            logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
        else:
            logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

        # Startup records buffer until the log-file decision: a recipe
        # log_file attaches too late to see them live, so they are held
        # and flushed as the file's opening lines - or discarded at the
        # seam when nobody wants a file.
        install_early_log_buffer()

        # CLI log file attaches from startup, so it captures everything
        # including recipe loading. A recipe-settings log_file attaches
        # later (it needs external variables resolved for paths like
        # {output_dir}); the CLI flag wins when both are given.
        if getattr(args, 'log_file', None):
            attach_log_file(args.log_file, source='cli')
        
        # Main processing workflow
        if hasattr(args, 'recipe_file') and args.recipe_file:
            return process_recipe(args)
        else:
            # No recipe specified - show help
            print("Error: Recipe file is required")
            print("Usage: python -m excel_recipe_processor recipe.yaml [--var name=value ...]")
            print("Use --help for full usage information")
            return 1
            
    except Exception as e:
        # For unexpected errors, always show them clearly
        print(f"Error: {e}")
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        return 1


_attached_log_files = {'cli': None, 'recipe': None}
_attached_log_streams = []


def mirror_print(text='') -> None:
    """print() that also lands in any attached log file, verbatim.

    The run summary and the blank separator lines are deliberately
    UNPREFIXED terminal output (no 'INFO:'), so they live outside the
    logging system and no FileHandler ever sees them - the tail of the
    log file was silently missing the summary block until 2026-08-16.
    This writes the same bytes to both places: the terminal via print,
    and each attached log stream raw (no formatter, so the file matches
    the terminal exactly). Before any attachment it is just print.
    """
    print(text)
    if _attached_log_streams:
        for stream in _attached_log_streams:
            try:
                stream.write(str(text) + '\n')
                stream.flush()
            except Exception:
                pass  # a dead stream must not kill the run for a log line
    elif _early_buffer is not None:
        # No file yet: hold the line in emission order beside the log
        # records, so a later attach replays the head exactly
        _early_buffer.add_text(str(text))


class _EarlyLogBuffer(logging.Handler):
    """Holds startup log records until the log-file decision is made.

    A recipe's log_file directive cannot attach until external variables
    resolve, but the loading lines are worth keeping - so this buffer
    captures everything from startup, attach_log_file() flushes it as
    the FILE'S OPENING LINES, and the pipeline discards it at the seam
    when the recipe declines and no --log-file was given. Capped so a
    pathological pre-seam flood cannot grow memory unbounded.
    """

    MAX_RECORDS = 1000

    def __init__(self):
        super().__init__()
        # Ordered mixed items: ('record', LogRecord) from the logging
        # system, ('text', str) from mirror_print - so the head of the
        # file replays in TRUE emission order, blanks included.
        self.items = []

    def emit(self, record):
        if len(self.items) < self.MAX_RECORDS:
            self.items.append(('record', record))

    def add_text(self, text: str) -> None:
        if len(self.items) < self.MAX_RECORDS:
            self.items.append(('text', text))


_early_buffer = None


def install_early_log_buffer() -> None:
    """Start capturing startup records (called right after basicConfig)."""
    global _early_buffer
    if _early_buffer is None:
        _early_buffer = _EarlyLogBuffer()
        logging.getLogger().addHandler(_early_buffer)


def discard_early_log_buffer() -> None:
    """The decision point passed with no log file wanted: drop the buffer."""
    global _early_buffer
    if _early_buffer is not None:
        logging.getLogger().removeHandler(_early_buffer)
        _early_buffer.items.clear()
        _early_buffer = None


def attach_log_file(file_path, source='cli') -> bool:
    """
    Mirror the log stream to a file - same format, emoji and all.

    The stream carries no ANSI codes (plain basicConfig), so the file is
    byte-identical to the terminal's content. UTF-8 is explicit so the
    Unicode symbols survive on every platform. Overwrites per run (each
    run's log pairs with that run's output file). A recipe-settings
    attachment is skipped when the CLI already attached one - the person
    at the command line outranks the recipe.

    EXTENSION POLICY (2026-08-16, decided): the path is taken VERBATIM -
    no extension appended, corrected, or warned about. Rewriting an
    extensionless path would be the framework overriding stated intent,
    and the 🪵 announcement (terminal + first line of the file itself)
    already makes any path mistake immediately visible. House default is
    '_log.txt': the NAME carries the semantics, the txt EXTENSION the
    ergonomics (double-click opens a text editor everywhere; macOS
    routes .log to Console.app, which reads worse for this content).
    Anyone preferring .log simply writes it.

    Returns True if attached, False if skipped.
    """
    if source == 'recipe' and _attached_log_files['cli']:
        logging.getLogger(__name__).info(
            f"🪵 Recipe log_file skipped; --log-file already active: "
            f"{_attached_log_files['cli']}")
        return False
    if _attached_log_files.get(source):
        return False

    resolved = Path(file_path).expanduser()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(resolved, mode='w', encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

    # The buffered startup lines become the FILE'S OPENING LINES - they
    # already reached the terminal, so they flush to the file handler
    # only, before it joins the root logger for live records.
    global _early_buffer
    if _early_buffer is not None:
        for kind, payload in _early_buffer.items:
            if kind == 'record':
                handler.handle(payload)
            else:
                handler.stream.write(payload + '\n')
        handler.stream.flush()
        discard_early_log_buffer()

    logging.getLogger().addHandler(handler)
    _attached_log_files[source] = str(resolved)
    _attached_log_streams.append(handler.stream)
    from excel_recipe_processor.core.log_format import q
    logging.getLogger(__name__).info(f"🪵 Logging to file: {q(resolved)}")
    return True


def process_recipe(args: Namespace) -> int:
    """
    Process a recipe using the new RecipePipeline system.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    recipe_file = args.recipe_file
    verbose = getattr(args, 'verbose', False)
    
    if verbose:
        logger.info(f"Processing recipe: {q(recipe_file)}")
    
    try:
        # Parse CLI variable overrides
        cli_variables = {}

        # --set NAME VALUE pairs are merged first, so an explicit --var of the
        # same name later on the command line wins
        if getattr(args, 'variable_pairs', None):
            for name, value in args.variable_pairs:
                cli_variables[name] = value
            mirror_print()  # separator; buffered for the file's head
            logger.info(f"Parsed {len(cli_variables)} variable overrides from --set")

        if hasattr(args, 'variable_overrides') and args.variable_overrides:
            try:
                cli_variables.update(parse_cli_variables(args.variable_overrides))
                if cli_variables:
                    mirror_print()  # separator; buffered for the file's head
                    logger.info(f"Parsed {len(cli_variables)} variable overrides from CLI")
            except InteractiveVariableError as e:
                print(f"Error parsing variable overrides: {e}")
                return 1
        
        # Create pipeline and run complete workflow
        pipeline = RecipePipeline()

        # Development-time inspection, all driven from the command line
        dump_requests = {}
        if getattr(args, 'dump_stages', None):
            from excel_recipe_processor.core.stage_inspection import (
                parse_dump_argument, validate_spec, describe_spec, StageInspectionError
            )
            try:
                for argument in args.dump_stages:
                    name, spec = parse_dump_argument(argument)
                    validate_spec(spec)
                    dump_requests[name] = spec
                    print(f"🔎 Will dump '{name}' ({describe_spec(spec)})")
            except StageInspectionError as error:
                print(f"Error: {error}")
                return 1

        pipeline.configure_inspection(
            dump_requests=dump_requests,
            stop_after_stage=getattr(args, 'stop_after_stage', None),
            dump_output_dir=getattr(args, 'dump_dir', '.')
        )
        
        try:
            # Use the integrated pipeline method that handles everything
            completion_report = pipeline.run_complete_recipe(recipe_file, cli_variables)
            
        except RecipePipelineError as e:
            # Pipeline handles friendly error messages internally
            print(f"Recipe processing failed: {e}")
            return 1
        except FileNotFoundError:
            print(f"Recipe file not found: {recipe_file}")
            return 1
        except InteractiveVariableError as e:
            print(f"Error collecting variables: {e}")
            return 1
        
        # Report completion with same level of detail as before
        steps_executed = completion_report.get('steps_executed', 0)
        stages_created = completion_report.get('stages_created', [])
        stages_declared = completion_report.get('stages_declared', [])
        
        mirror_print()  # blank line to separate from last logging line
        elapsed = completion_report.get('elapsed_seconds')
        elapsed_text = ""
        if elapsed is not None:
            minutes, seconds = divmod(elapsed, 60)
            elapsed_text = f" in {int(minutes)}m {seconds:.1f}s" if minutes else f" in {seconds:.1f}s"

        logger.info(f"🕐 [{now_stamp()}] Recipe processing finished")
        mirror_print(f"✓ Recipe completed successfully{elapsed_text}")
        mirror_print(f"  Steps executed: {steps_executed}")
        # The pipeline's report is flattened; derive the survivors
        alive_at_end = sorted(
            set(completion_report.get('stages_created', []))
            - set(completion_report.get('stages_freed', [])))
        if alive_at_end:
            from excel_recipe_processor.core.log_format import qblock
            mirror_print(f"  Stages alive at run end ({len(alive_at_end)}):"
                         f"{qblock(alive_at_end)}")
        stages_freed = completion_report.get('stages_freed', [])
        if stages_freed:
            mirror_print(f"  Data stages created: {len(stages_created)} ({len(stages_freed)} freed during the run)")
        else:
            mirror_print(f"  Data stages created: {len(stages_created)}")
        mirror_print(f"  Data stages declared: {len(stages_declared)}")
        stage_memory = completion_report.get('stage_memory')
        if stage_memory and stage_memory.get('total_allocated_mb'):
            def _mb(value):
                # Tiny runs deserve a decimal; big numbers stay round
                return f"{value:.1f}" if value < 10 else f"{value:.0f}"
            mirror_print(
                f"  Stage memory: ~{_mb(stage_memory['peak_concurrent_mb'])} MB peak concurrent, "
                f"~{_mb(stage_memory['total_allocated_mb'])} MB allocated, "
                f"~{_mb(stage_memory['total_freed_mb'])} MB freed during the run")
        mirror_print()  # blank line to separate from next command prompt
        
        # Verbose stage details (preserving current behavior)
        if verbose and stages_created:
            print("  Stages created:")
            for stage_name in stages_created:
                print(f"    - {stage_name}")
        
        # Verbose stage details (preserving current behavior)
        if verbose and stages_declared:
            print("  Stages created:")
            for stage_name in stages_declared:
                print(f"    - {stage_name}")
        
        return 0
        
    except RecipePipelineError as e:
        print(f"Recipe processing failed: {e}")
        return 1
    except FileNotFoundError:
        print(f"Recipe file not found: {recipe_file}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1


def list_system_capabilities() -> int:
    """List available processors in basic format."""
    try:
        capabilities = get_system_capabilities()
        
        print("Available Excel Recipe Processors")
        print("=" * 40)
        
        system_info = capabilities.get('system_info', {})
        processors = capabilities.get('processors', {})
        
        print(f"System: {system_info.get('description', 'Excel Recipe Processor')}")
        print(f"Total Processors: {system_info.get('total_processors', len(processors))}")
        print()
        
        for processor_name, info in sorted(processors.items()):
            if 'error' in info:
                print(f"{processor_name:<25} ❌ {info['error']}")
            else:
                description = info.get('description', 'No description available')
                print(f"{processor_name:<25} {description}")
        
        return 0
        
    except Exception as e:
        print(f"Error listing capabilities: {e}")
        return 1


def list_system_capabilities_detailed() -> int:
    """List capabilities with detailed information."""
    try:
        capabilities = get_system_capabilities()
        
        print("Detailed Excel Recipe Processor Capabilities")
        print("=" * 50)
        
        system_info = capabilities.get('system_info', {})
        processors = capabilities.get('processors', {})
        
        print(f"System: {system_info.get('description', 'Excel Recipe Processor')}")
        print(f"Total Processors: {system_info.get('total_processors', len(processors))}")
        print()
        
        for processor_name, info in sorted(processors.items()):
            print(f"\n{processor_name}")
            print("-" * len(processor_name))
            
            if 'error' in info:
                print(f"❌ Error: {info['error']}")
                continue
                
            print(f"Description: {info.get('description', 'No description available')}")
            
            # Show parameters if available
            if 'parameters' in info:
                print("Parameters:")
                for param_name, param_info in info['parameters'].items():
                    if isinstance(param_info, dict):
                        required = "Required" if param_info.get('required', False) else "Optional"
                        param_desc = param_info.get('description', 'No description')
                        print(f"  {param_name} ({required}): {param_desc}")
                    else:
                        # Some processors supply a plain description string
                        print(f"  {param_name}: {param_info}")
            
            # Show capabilities if available
            if 'supported_actions' in info:
                print(f"Supported Actions: {', '.join(info['supported_actions'])}")
            
            if 'calculation_types' in info:
                print(f"Calculation Types: {', '.join(info['calculation_types'])}")
            
            if 'supported_conditions' in info:
                print(f"Filter Conditions: {', '.join(info['supported_conditions'])}")
            
            if 'join_types' in info:
                print(f"Join Types: {', '.join(info['join_types'])}")
            
            if 'aggregation_functions' in info:
                print(f"Aggregation Functions: {', '.join(info['aggregation_functions'])}")
            
            # Show feature counts
            feature_counts = []
            for key, value in info.items():
                if isinstance(value, list) and key not in ['parameters']:
                    feature_counts.append(f"{key}: {len(value)}")
            
            if feature_counts:
                print(f"Features: {', '.join(feature_counts)}")
        
        print(f"\nTotal: {len(processors)} processors available")
        return 0
        
    except Exception as e:
        print(f"Error listing detailed capabilities: {e}")
        return 1


def list_system_capabilities_json() -> int:
    """List capabilities in JSON format."""
    try:
        import json
        capabilities = get_system_capabilities()
        print(json.dumps(capabilities, indent=2))
        return 0
    except Exception as e:
        print(f"Error generating JSON capabilities: {e}")
        return 1


def list_system_capabilities_yaml() -> int:
    """List capabilities in YAML format."""
    try:
        import yaml
        capabilities = get_system_capabilities()
        print(yaml.dump(capabilities, default_flow_style=False))
        return 0
    except Exception as e:
        print(f"Error generating YAML capabilities: {e}")
        return 1


def list_system_capabilities_detailed_yaml() -> int:
    """List capabilities with detailed YAML format."""
    try:
        import yaml
        capabilities = get_system_capabilities()
        
        print("# Excel Recipe Processor - Detailed Capabilities")
        print("# Generated automatically")
        print()
        print(yaml.dump(capabilities, default_flow_style=False))
        return 0
    except Exception as e:
        print(f"Error generating detailed YAML capabilities: {e}")
        return 1


def list_system_capabilities_matrix() -> int:
    """Show capabilities in matrix format."""
    try:
        capabilities = get_system_capabilities()
        
        print("Excel Recipe Processor - Feature Matrix")
        print("=" * 50)
        
        system_info = capabilities.get('system_info', {})
        processors = capabilities.get('processors', {})
        
        print(f"System: {system_info.get('description', 'Excel Recipe Processor')}")
        print(f"Total Processors: {len(processors)}")
        print()
        
        # Collect all feature types across processors
        all_features = set()
        processor_features = {}
        
        for processor_name, info in processors.items():
            if 'error' not in info:
                features = set()
                for key, value in info.items():
                    if isinstance(value, list) and key not in ['parameters']:
                        features.update([f"{key}:{item}" for item in value])
                        all_features.update([f"{key}:{item}" for item in value])
                    elif key in ['join_types', 'calculation_types', 'supported_conditions', 'aggregation_functions']:
                        if isinstance(value, list):
                            features.update([f"{key}:{item}" for item in value])
                            all_features.update([f"{key}:{item}" for item in value])
                
                processor_features[processor_name] = features
        
        # Group features by category
        feature_categories = {}
        for feature in sorted(all_features):
            if ':' in feature:
                category, item = feature.split(':', 1)
                if category not in feature_categories:
                    feature_categories[category] = []
                feature_categories[category].append(item)
        
        # Display matrix by category
        for category, items in feature_categories.items():
            print(f"\n{category.replace('_', ' ').title()}:")
            print("-" * 30)
            
            for item in sorted(items):
                supporting_processors = []
                for proc_name, proc_features in processor_features.items():
                    if f"{category}:{item}" in proc_features:
                        supporting_processors.append(proc_name)
                
                if supporting_processors:
                    print(f"  {item:<20} → {', '.join(supporting_processors)}")
        
        return 0
    except Exception as e:
        print(f"Error generating capability matrix: {e}")
        return 1


# def validate_recipe_file(recipe_path: str) -> int:
#     """Validate a recipe file."""
#     try:
#         from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
#         from excel_recipe_processor.core.stage_manager import StageManager
        
#         # Load and validate recipe
#         recipe_loader = RecipeLoader()
#         recipe_data = recipe_loader.load_recipe_file(recipe_path)
        
#         # Validate stages
#         StageManager.declare_recipe_stages(recipe_data)
#         stage_errors = StageManager.validate_recipe_stages(recipe_data)
        
#         if stage_errors:
#             print(f"Recipe validation failed for: {recipe_path}")
#             for error in stage_errors:
#                 print(f"  ❌ {error}")
#             return 1
#         else:
#             print(f"✓ Recipe validation successful: {recipe_path}")
            
#             # Show summary
#             recipe_steps = recipe_data.get('recipe', [])
#             settings = recipe_data.get('settings', {})
#             external_vars = settings.get('required_external_vars', {})
#             custom_vars = settings.get('variables', {})
            
#             print(f"  Steps: {len(recipe_steps)}")
#             print(f"  External variables: {len(external_vars)}")
#             print(f"  Custom variables: {len(custom_vars)}")
            
#             return 0
            
#     except RecipeValidationError as e:
#         print(f"Recipe validation error: {e}")
#         return 1
#     except FileNotFoundError:
#         print(f"Recipe file not found: {recipe_path}")
#         return 1
#     except Exception as e:
#         print(f"Error validating recipe: {e}")
#         return 1


def validate_recipe_file(recipe_path: str) -> int:
    """
    Validate a recipe file without executing it.
    
    Args:
        recipe_path: Path to recipe file
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # from excel_recipe_processor.core.stage_manager import StageManager
        # from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
        
        # Load and validate recipe
        loader = RecipeLoader()
        recipe_data = loader.load_recipe_file(recipe_path)  # Using the corrected method name
        
        # Validate stages - this returns a DICTIONARY, not a list!
        StageManager.declare_recipe_stages(recipe_data)
        stage_validation = StageManager.validate_recipe_stages(recipe_data)
        
        # Extract the actual error information from the dictionary
        warnings = stage_validation.get('warnings', [])
        undeclared_stages = stage_validation.get('undeclared_stages', set())
        suggested_declarations = stage_validation.get('suggested_declarations', '')
        protection_issues = stage_validation.get('protection_issues', [])
        has_undeclared = stage_validation.get('has_undeclared', False)
        
        # Check if there are any issues to report
        has_issues = bool(warnings or has_undeclared)
        
        if has_issues:
            print(f"Recipe validation completed with warnings: {recipe_path}")
            print()
            
            # Show warnings
            if warnings:
                print("⚠️  Warnings:")
                for warning in warnings:
                    print(f"    • {warning}")
                print()
            
            # Show undeclared stages
            if undeclared_stages:
                print(f"💡 Found {len(undeclared_stages)} undeclared stages:")
                for stage in sorted(undeclared_stages):
                    print(f"    • {stage}")
                print()
            
            # Show suggested stage declarations
            if suggested_declarations:
                print("🔧 Suggested improvements:")
                print(suggested_declarations)
                print()
            
            # Show protection issues
            if protection_issues:
                print("🛡️  Protection recommendations:")
                for issue in protection_issues:
                    print(f"    • {issue}")
                print()
            
            print("✓ Recipe is valid but could be improved with the suggestions above")
            
            # Show summary
            recipe_steps = recipe_data.get('recipe', [])
            settings = recipe_data.get('settings', {})
            external_vars = settings.get('required_external_vars', {})
            custom_vars = settings.get('variables', {})
            
            print(f"📊 Recipe Summary:")
            print(f"    Steps: {len(recipe_steps)}")
            print(f"    External variables: {len(external_vars)}")
            print(f"    Custom variables: {len(custom_vars)}")
            print(f"    Declared stages: {len(settings.get('stages', []))}")
            print(f"    Undeclared stages: {len(undeclared_stages)}")
            
            return 0  # Warnings don't cause failure
            
        else:
            print(f"✅ Recipe validation successful: {recipe_path}")
            
            # Show summary
            recipe_steps = recipe_data.get('recipe', [])
            settings = recipe_data.get('settings', {})
            external_vars = settings.get('required_external_vars', {})
            custom_vars = settings.get('variables', {})
            
            print(f"📊 Recipe Summary:")
            print(f"    Steps: {len(recipe_steps)}")
            print(f"    External variables: {len(external_vars)}")
            print(f"    Custom variables: {len(custom_vars)}")
            print(f"    Declared stages: {len(settings.get('stages', []))}")
            
            return 0
            
    except RecipeValidationError as e:
        print(f"❌ Recipe validation error: {e}")
        return 1
    except FileNotFoundError:
        print(f"❌ Recipe file not found: {recipe_path}")
        return 1
    except Exception as e:
        print(f"❌ Error validating recipe: {e}")
        return 1


# Add these missing CLI functions to excel_recipe_processor/core/main.py

def get_settings_examples(format_type: str = 'yaml') -> int:
    """
    Get and display recipe settings configuration examples.
    
    Args:
        format_type: Output format ('yaml', 'text', 'json')
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Import the settings examples loading function
        from excel_recipe_processor.utils.processor_examples_loader import load_settings_examples
        
        # Try to load settings examples from YAML file
        settings_examples = load_settings_examples()
        
        if 'error' in settings_examples:
            # No YAML file found, use built-in examples
            return _display_builtin_settings_examples(format_type)
        
        # Successfully loaded YAML file
        return _display_yaml_settings_examples(settings_examples, format_type)
        
    except Exception as e:
        print(f"Error getting settings examples: {e}")
        return 1


def get_usage_examples(processor_name: str, format_type: str = 'yaml') -> int:
    """
    Get and display usage examples for a specific processor or all processors.
    
    Args:
        processor_name: Name of the processor to get examples for, or 'all' for all processors
        format_type: Output format ('yaml', 'text', 'json')
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Special case: handle 'settings' as processor name
        if processor_name == 'settings':
            return get_settings_examples(format_type)
        
        # Import the YAML loading function and pipeline functions
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        from excel_recipe_processor.core.pipeline import get_processor_usage_examples, get_all_usage_examples
        
        # Handle 'all' processors case
        if processor_name == 'all':
            return _get_all_usage_examples(format_type)
        
        # First try to load from YAML file
        examples_data = load_processor_examples(processor_name)
        
        if 'error' not in examples_data:
            # Successfully loaded YAML file
            return _display_yaml_examples(processor_name, examples_data, format_type)
        
        # Fallback to processor method
        method_examples = get_processor_usage_examples(processor_name)
        
        if method_examples and 'error' not in method_examples:
            # Successfully got examples from processor method
            return _display_method_examples(processor_name, method_examples, format_type)
        
        # No examples available
        print(f"No usage examples available for processor: {processor_name}")
        print()
        print('Tip: use "settings" as the name for recipe settings examples')
        print()
        print("Available processors:")
        
        # Show available processors
        capabilities = get_system_capabilities()
        processor_names = [name for name in capabilities['processors'].keys() if name != 'base_processor']
        
        for name in sorted(processor_names):
            print(f"  - {name}")
        
        return 1
        
    except Exception as e:
        print(f"Error getting usage examples: {e}")
        return 1


def _display_yaml_settings_examples(settings_examples: dict, format_type: str) -> int:
    """Display settings examples loaded from YAML file."""
    try:
        print("Recipe Settings Usage Examples")
        print("=" * 40)
        
        if format_type == 'json':
            import json
            print(json.dumps(settings_examples, indent=2))
            return 0
        
        if format_type == 'text':
            print(f"Description: {settings_examples.get('description', 'No description available')}")
            print()
            
            # List examples
            example_keys = [key for key in settings_examples.keys() if key.endswith('_example')]
            for example_key in example_keys:
                example = settings_examples[example_key]
                print(f"Example: {example_key}")
                print(f"  Description: {example.get('description', 'No description')}")
                print()
            
            return 0
        
        # YAML format (default)
        print(f"# {settings_examples.get('description', 'Recipe settings configuration examples')}")
        print()
        
        # Extract and display the actual YAML examples
        example_keys = [key for key in settings_examples.keys() if key.endswith('_example')]
        
        for example_key in example_keys:
            example = settings_examples[example_key]
            if 'yaml' in example:
                print(f"# {example.get('description', example_key)}")
                print(example['yaml'])
                print()
        
        return 0
        
    except Exception as e:
        print(f"Error displaying settings examples: {e}")
        return 1


def _display_builtin_settings_examples(format_type: str) -> int:
    """Display built-in settings examples when no YAML file is available."""
    try:
        print("Recipe Settings Usage Examples")
        print("=" * 40)
        
        if format_type == 'json':
            builtin_examples = {
                "description": "Recipe settings configuration examples",
                "basic_example": {
                    "description": "Minimal settings section",
                    "yaml": "settings:\n  description: \"Process daily sales data\""
                },
                "variables_example": {
                    "description": "Settings with custom variables",
                    "yaml": "settings:\n  description: \"Process with variables\"\n  variables:\n    region: \"west\"\n    batch_id: \"A47\""
                }
            }
            import json
            print(json.dumps(builtin_examples, indent=2))
            return 0
        
        if format_type == 'text':
            print("Description: Recipe settings configuration options")
            print()
            print("Available settings:")
            print("  - description: Recipe description")
            print("  - variables: Custom variables")
            print("  - required_external_vars: External variable definitions")
            print("  - stages: Stage declarations")
            print()
            return 0
        
        # YAML format (default)
        print("# Recipe settings configuration examples")
        print()
        
        print("# Minimal settings section")
        print("settings:")
        print("  description: \"Process daily sales data\"")
        print()
        
        print("# Settings with custom variables")
        print("settings:")
        print("  description: \"Process with dynamic variables\"")
        print("  variables:")
        print("    region: \"west\"")
        print("    batch_id: \"A47\"")
        print("    output_prefix: \"processed\"")
        print()
        
        print("# Advanced settings with external variables and stages")
        print("settings:")
        print("  description: \"Complete recipe configuration\"")
        print("  ")
        print("  # Custom variables for reuse")
        print("  variables:")
        print("    region: \"west\"")
        print("    output_prefix: \"processed\"")
        print("  ")
        print("  # External variables with validation")
        print("  required_external_vars:")
        print("    batch_id:")
        print("      description: \"Batch identifier\"")
        print("      validation: \"^[A-Z]\\\\d+$\"")
        print("      example: \"A47\"")
        print("  ")
        print("  # Stage declarations")
        print("  stages:")
        print("    - stage_name: \"raw_data\"")
        print("      description: \"Raw imported data\"")
        print("      protected: false")
        print("    - stage_name: \"processed_data\"")
        print("      description: \"Final processed results\"")
        print("      protected: false")
        print()
        
        return 0
        
    except Exception as e:
        print(f"Error displaying built-in settings examples: {e}")
        return 1


def _get_all_usage_examples(format_type: str) -> int:
    """Get usage examples for all processors."""
    try:
        from excel_recipe_processor.core.pipeline import get_all_usage_examples
        
        all_examples = get_all_usage_examples()
        
        if format_type == 'json':
            import json
            print(json.dumps(all_examples, indent=2))
        elif format_type == 'text':
            _display_all_examples_text(all_examples)
        else:  # yaml format
            _display_all_examples_yaml(all_examples)
        
        return 0
        
    except Exception as e:
        print(f"Error getting all usage examples: {e}")
        return 1


def _display_yaml_examples(processor_name: str, examples_data: dict, format_type: str) -> int:
    """Display examples loaded from YAML file."""
    try:
        print(f"Usage Examples for: {processor_name}")
        print("=" * 40)
        
        if format_type == 'json':
            import json
            print(json.dumps(examples_data, indent=2))
            return 0
        
        if format_type == 'text':
            print(f"Processor: {processor_name}")
            print(f"Description: {examples_data.get('description', 'No description available')}")
            print()
            
            # List examples
            example_keys = [key for key in examples_data.keys() if key.endswith('_example')]
            for example_key in example_keys:
                example = examples_data[example_key]
                print(f"Example: {example_key}")
                print(f"  Description: {example.get('description', 'No description')}")
                print()
            
            return 0
        
        # YAML format (default)
        print(f"# {examples_data.get('description', 'Usage examples')}")
        print()
        
        # Extract and display the actual YAML examples
        example_keys = [key for key in examples_data.keys() if key.endswith('_example')]
        
        for example_key in example_keys:
            example = examples_data[example_key]
            if 'yaml' in example:
                print(f"# {example.get('description', example_key)}")
                print(example['yaml'])
                print()
        
        return 0
        
    except Exception as e:
        print(f"Error displaying YAML examples: {e}")
        return 1


def _display_method_examples(processor_name: str, method_examples: dict, format_type: str) -> int:
    """Display examples from processor method."""
    try:
        print(f"Usage Examples for: {processor_name}")
        print("=" * 40)
        
        if format_type == 'json':
            print(method_examples.get('formatted_json', '{}'))
        elif format_type == 'text':
            print(method_examples.get('formatted_text', 'No text format available'))
        else:  # yaml format
            print(method_examples.get('formatted_yaml', '# No YAML format available'))
        
        return 0
        
    except Exception as e:
        print(f"Error displaying method examples: {e}")
        return 1


def _display_all_examples_yaml(all_examples: dict) -> None:
    """Display all examples in YAML format."""
    print("# Complete Usage Examples for Excel Recipe Processor")
    print("# =" * 50)
    print()
    
    system_info = all_examples.get('system_info', {})
    print(f"# Total processors: {system_info.get('total_processors', 0)}")
    print(f"# Processors with examples: {system_info.get('processors_with_examples', 0)}")
    print(f"# Processors missing examples: {system_info.get('processors_missing_examples', 0)}")
    print()

    # Designed behavior: the complete reference opens with the RECIPE
    # SETTINGS section, so a copy-paste starting point precedes the
    # per-processor steps.
    try:
        from excel_recipe_processor.utils.processor_examples_loader import load_settings_examples
        settings_examples = load_settings_examples()
        if 'error' not in settings_examples:
            print("# RECIPE SETTINGS")
            print(f"# {'-' * 20}")
            settings_keys = [key for key in settings_examples.keys()
                             if key.endswith('_example')]
            for settings_key in settings_keys:
                settings_entry = settings_examples[settings_key]
                if isinstance(settings_entry, dict) and 'yaml' in settings_entry:
                    print(f"# {settings_entry.get('description', settings_key)}")
                    print(settings_entry['yaml'])
                    print()
            print()
    except Exception as settings_error:
        print(f"# RECIPE SETTINGS - unavailable: {settings_error}")
        print()

    processors = all_examples.get('processors', {})
    
    for processor_name in sorted(processors.keys()):
        processor_data = processors[processor_name]
        
        if 'error' not in processor_data:
            print(f"# {processor_name.upper()} PROCESSOR")
            print(f"# {'-' * 20}")
            
            if 'formatted_yaml' in processor_data:
                print(processor_data['formatted_yaml'])
            else:
                print(f"# Examples available but not formatted for {processor_name}")
        else:
            print(f"# {processor_name.upper()} PROCESSOR - {processor_data['error']}")
        
        print()


def _display_all_examples_text(all_examples: dict) -> None:
    """Display all examples in text format."""
    print("Complete Usage Examples for Excel Recipe Processor")
    print("=" * 50)
    
    system_info = all_examples.get('system_info', {})
    print(f"Total processors: {system_info.get('total_processors', 0)}")
    print(f"Processors with examples: {system_info.get('processors_with_examples', 0)}")
    print(f"Processors missing examples: {system_info.get('processors_missing_examples', 0)}")
    print()
    
    processors = all_examples.get('processors', {})
    
    for processor_name in sorted(processors.keys()):
        processor_data = processors[processor_name]
        
        print(f"Processor: {processor_name}")
        
        if 'error' not in processor_data:
            if 'formatted_text' in processor_data:
                print(processor_data['formatted_text'])
            else:
                print("  Examples available but not formatted for text display")
        else:
            print(f"  Error: {processor_data['error']}")
        
        print("-" * 30)
