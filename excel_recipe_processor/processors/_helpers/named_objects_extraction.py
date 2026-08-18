"""
Shared named-object extraction: classification and display translation.

excel_recipe_processor/processors/_helpers/named_objects_extraction.py

Extracted 2026-08-15 from manage_named_objects (whose methods are now
thin delegates) so profile_named_objects can share the exact same
classification and human-translation logic - one truth for what a
defined name IS and how its stored grammar reads as display syntax.
"""

from excel_recipe_processor.processors._helpers.named_objects_patterns import (
    excel_param_name_rgx,
    excel_lambda_body_rgx,
    excel_lambda_params_rgx,
    excel_prefix_cleanup_rgx,
    excel_lambda_detection_rgx,
)
from excel_recipe_processor.processors._helpers.inject_formulas_rgx import (
    function_call_rgx,
)


def detect_object_type(defined_name) -> str:
    """Classify a defined name: 'lambda', 'formula', 'range', or 'constant'."""
    attr_text = defined_name.attr_text or ""

    if excel_lambda_detection_rgx.search(attr_text):
        return 'lambda'
    if function_call_rgx.search(attr_text):
        return 'formula'
    if '!' in attr_text or ':' in attr_text:
        return 'range'
    return 'constant'


def clean_formula_for_display(formula: str) -> str:
    """Remove Excel storage prefixes for human-readable display."""
    return excel_prefix_cleanup_rgx.sub('', formula).strip()


def translate_lambda_to_human(excel_formula: str) -> tuple:
    """Stored '_xlfn.LAMBDA(_xlpm.p,...)' -> ('LAMBDA(p, ...)', [params])."""
    if not isinstance(excel_formula, str):
        excel_formula = str(excel_formula) if excel_formula else ""

    params_match = excel_lambda_params_rgx.search(excel_formula)
    if not params_match:
        return excel_formula, []

    param_names = excel_param_name_rgx.findall(params_match.group(1))

    body_match = excel_lambda_body_rgx.search(excel_formula)
    if not body_match:
        return excel_formula, param_names

    clean_body = clean_formula_for_display(body_match.group(2))
    human_formula = f"LAMBDA({', '.join(param_names)}, {clean_body})"
    return human_formula, param_names

# End of file #
