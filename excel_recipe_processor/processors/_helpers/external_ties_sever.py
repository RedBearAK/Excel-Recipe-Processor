"""
Sever surgery over an xlsx package: resolve every [N] carrier, then
remove the external-link plumbing, writing a NEW package.

excel_recipe_processor/processors/_helpers/external_ties_sever.py

The source file is read once with zipfile. Every part is held as
bytes; the XML parts that carry ties are rewritten as text; the result
goes through xlsx_package_write.write_and_verify, which proves the
surgery touched only the parts it claimed before naming the result -
as a new file, or in place of the original (temp + verify + backup +
atomic replace). Refusal at any point means NO output - the report
still records what would have happened.

RESOLUTION ORDER, per carrier (2026-09-14 design):
  1. retarget - when every sheet the reference names exists in this
     workbook (and [N]!Name names a local defined name), the [N] is
     removed and the reference becomes local. Applies to formulas,
     defined names, CF and DV rules, chart series.
  2. the carrier's unresolved policy:
       formulas               freeze (drop <f>, keep cached <v>) | refuse
       defined names          drop | refuse
       conditional formatting drop the rule | refuse
       data validation        drop the rule | refuse
  3. forced refusals v1 cannot handle: array formulas, shared masters
     or slaves with no cached value under freeze, chart series that
     do not retarget, pivot caches sourced from an external link,
     any [N] in a carrier the classifier does not name.

ALL-OR-NOTHING: the plumbing (externalReferences block, link rels,
link parts, content-type overrides) is removed only when zero [N]
survives in the whole package. Partial removal would require
renumbering every surviving index - the exact surgery that creates
phantom links - so it is not attempted.

calcChain.xml is removed and fullCalcOnLoad set: a frozen cell that
is still listed in calcChain makes Excel offer a repair on open, and
retargeted formulas need a recalculation anyway.
"""

import zipfile

from excel_recipe_processor.processors._helpers.xlsx_package_rgx import (
    calc_pr_rgx,
    attr_name_rgx,
    attr_count_rgx,
    attr_sqref_rgx,
    chart_part_rgx,
    cell_element_rgx,
    cell_ref_attr_rgx,
    chart_formula_rgx,
    value_element_rgx,
    calc_completed_rgx,
    workbook_close_rgx,
    worksheet_part_rgx,
    cf_rule_element_rgx,
    formula_element_rgx,
    formula_si_attr_rgx,
    calc_mode_manual_rgx,
    formula_type_attr_rgx,
    full_calc_on_load_rgx,
    calc_chain_override_rgx,
    calc_chain_rel_type_rgx,
    relationship_element_rgx,
    inline_string_element_rgx,
    workbook_defined_name_rgx,
    data_validation_element_rgx,
    data_validations_element_rgx,
    pivot_cache_definition_part_rgx,
    conditional_formatting_element_rgx,
)
from excel_recipe_processor.processors._helpers.external_ties_rgx import (
    external_index_rgx,
    external_name_ref_rgx,
    external_sheet_ref_rgx,
    external_link_override_rgx,
    external_link_rel_type_rgx,
    external_references_block_rgx,
)
from excel_recipe_processor.processors._helpers.xlsx_package_write import (
    write_and_verify,
)
from excel_recipe_processor.processors._helpers.external_ties_inventory import (
    sheet_part_map,
    inventory_external_ties,
)


POLICY_FREEZE = 'freeze'
POLICY_DROP = 'drop'
POLICY_REFUSE = 'refuse'

FORMULA_POLICIES = (POLICY_FREEZE, POLICY_REFUSE)
DROPPABLE_POLICIES = (POLICY_DROP, POLICY_REFUSE)

# Inventory sections the sever step leaves in the new file on purpose.
LIMBO_SECTIONS = ('file_hyperlinks', 'connections', 'query_tables', 'ole_objects')

SNIPPET_CAP = 120


class SeverRefusal(Exception):
    """Raised after the plan is complete when anything was refused."""

    def __init__(self, message: str, report: dict):
        super().__init__(message)
        self.report = report


def _snip(text: str) -> str:
    return text if len(text) <= SNIPPET_CAP else text[:SNIPPET_CAP] + '...'


class SeverPlan:
    """Rewrites parts in memory; nothing reaches disk until write()."""

    def __init__(self, source_path: str, policies: dict, retarget_if_possible: bool):
        self.source_path = source_path
        self.policies = policies
        self.retarget_if_possible = retarget_if_possible
        self.parts = {}
        self.order = []
        with zipfile.ZipFile(source_path, 'r') as archive:
            for name in archive.namelist():
                self.order.append(name)
                self.parts[name] = archive.read(name)
            self.sheets = sheet_part_map(archive)
        self.local_sheets = {name for name, _ in self.sheets}
        self.local_names = set()
        self.before = inventory_external_ties(source_path)
        # Retargeting is only trusted for a DECLARED link index whose
        # source sheet list (when the part has one) contains the named
        # sheet. An orphan [N], or a sheet the source never had, falls
        # through to the carrier policy and is reported as such.
        self.link_sheets = {link['index']: set(link['sheet_names'])
                            for link in self.before['external_links']}
        # What the surgery claims to touch; write_and_verify proves that
        # nothing outside these two sets differs from the original.
        self.changed_parts = set()
        self.removed_parts = set()
        self.fixed = {'retargeted': [], 'frozen': [], 'dropped': []}
        self.refused = []
        self.limbo = []

    # ---- text helpers ----------------------------------------------------------

    def _text(self, part: str) -> str:
        return self.parts[part].decode('utf-8')

    def _set_text(self, part: str, text: str) -> None:
        payload = text.encode('utf-8')
        if payload != self.parts[part]:
            self.changed_parts.add(part)
        self.parts[part] = payload

    def _retarget_text(self, text: str) -> str:
        """Local form of every [N] reference, or '' when any cannot resolve."""
        if not self.retarget_if_possible:
            return ''

        def local_name(match):
            index, name = int(match.group(1)), match.group(2)
            if index not in self.link_sheets or name not in self.local_names:
                raise LookupError(name)
            return name

        def local_sheet(match):
            quoted = match.group('qidx') is not None
            index = int(match.group('qidx') if quoted else match.group('idx'))
            sheet = match.group('qsheet') if quoted else match.group('sheet')
            if not sheet or index not in self.link_sheets:
                raise LookupError(sheet)
            unescaped = sheet.replace("''", "'")
            ends = unescaped.split(':') if ':' in unescaped else [unescaped]
            declared = self.link_sheets[index]
            for end in ends:
                if end not in self.local_sheets:
                    raise LookupError(end)
                if declared and end not in declared:
                    raise LookupError(end)
            return f"'{sheet}'!" if quoted else f"{sheet}!"

        try:
            candidate = external_name_ref_rgx.sub(local_name, text)
            candidate = external_sheet_ref_rgx.sub(local_sheet, candidate)
        except LookupError:
            return ''
        if external_index_rgx.search(candidate):
            return ''
        return candidate

    # ---- carriers --------------------------------------------------------------

    def _collect_local_names(self) -> None:
        workbook = self._text('xl/workbook.xml')
        for match in workbook_defined_name_rgx.finditer(workbook):
            if not external_index_rgx.search(match.group('text')):
                self.local_names.add(_first_name(match.group('attrs')))

    def resolve_defined_names(self) -> None:
        workbook = self._text('xl/workbook.xml')
        policy = self.policies['defined_name']

        def rewrite(match):
            attrs, text = match.group('attrs'), match.group('text')
            if not external_index_rgx.search(text):
                return match.group(0)
            name = _first_name(attrs)
            local = self._retarget_text(text)
            if local:
                self.fixed['retargeted'].append(
                    {'kind': 'defined_name', 'where': name,
                     'before': _snip(text), 'after': _snip(local)})
                return f'<definedName{attrs}>{local}</definedName>'
            if policy == POLICY_DROP:
                self.fixed['dropped'].append(
                    {'kind': 'defined_name', 'where': name, 'before': _snip(text)})
                return ''
            self.refused.append({'kind': 'defined_name', 'where': name,
                                 'reason': 'policy refuse', 'text': _snip(text)})
            return match.group(0)

        self._set_text('xl/workbook.xml', workbook_defined_name_rgx.sub(rewrite, workbook))

    def resolve_worksheets(self, heartbeat=None) -> None:
        for position, (sheet_name, part) in enumerate(self.sheets, start=1):
            if heartbeat is not None:
                heartbeat(position, len(self.sheets), sheet_name)
            if part not in self.parts or not worksheet_part_rgx.match(part):
                continue
            xml = self._text(part)
            if '[' in xml:
                xml = self._resolve_sheet_formulas(sheet_name, xml)
                xml = self._resolve_sheet_features(sheet_name, xml)
            self._set_text(part, xml)

    def _resolve_sheet_formulas(self, sheet_name: str, xml: str) -> str:
        policy = self.policies['formula']
        frozen_groups = set()

        def rewrite(match):
            body = match.group('body')
            if not body or '<f' not in body or '[' not in body:
                return match.group(0)
            attrs = match.group('attrs')
            formula = formula_element_rgx.search(body)
            if not formula:
                return match.group(0)
            text = formula.group('text') or ''
            if not external_index_rgx.search(text):
                return match.group(0)
            cell = f"{sheet_name}!{_first_ref(attrs)}"
            f_type = formula_type_attr_rgx.search(formula.group('attrs'))
            kind = f_type.group(1) if f_type else ''

            local = self._retarget_text(text)
            if local:
                self.fixed['retargeted'].append(
                    {'kind': 'formula', 'where': cell,
                     'before': _snip(text), 'after': _snip(local)})
                new_formula = f"<f{formula.group('attrs')}>{local}</f>"
                return f"<c{attrs}>{body.replace(formula.group(0), new_formula, 1)}</c>"

            if kind == 'array' or kind == 'dataTable':
                self.refused.append({'kind': 'formula', 'where': cell,
                                     'reason': f'{kind} formula; v1 cannot freeze it',
                                     'text': _snip(text)})
                return match.group(0)
            if policy != POLICY_FREEZE:
                self.refused.append({'kind': 'formula', 'where': cell,
                                     'reason': 'policy refuse', 'text': _snip(text)})
                return match.group(0)
            if not value_element_rgx.search(body):
                self.refused.append({'kind': 'formula', 'where': cell,
                                     'reason': 'no cached value to freeze to',
                                     'text': _snip(text)})
                return match.group(0)
            if kind == 'shared':
                si = formula_si_attr_rgx.search(formula.group('attrs'))
                if si:
                    frozen_groups.add(si.group(1))
            self.fixed['frozen'].append(
                {'kind': 'formula', 'where': cell, 'before': _snip(text),
                 'after': _snip(_value_text(body))})
            return f"<c{attrs}>{body.replace(formula.group(0), '', 1)}</c>"

        xml = cell_element_rgx.sub(rewrite, xml)
        if frozen_groups:
            xml = self._freeze_shared_slaves(sheet_name, xml, frozen_groups)
        return xml

    def _freeze_shared_slaves(self, sheet_name: str, xml: str, groups: set) -> str:
        def rewrite(match):
            body = match.group('body')
            if not body or '<f' not in body:
                return match.group(0)
            formula = formula_element_rgx.search(body)
            if not formula or formula.group('text'):
                return match.group(0)
            si = formula_si_attr_rgx.search(formula.group('attrs'))
            if not si or si.group(1) not in groups:
                return match.group(0)
            attrs = match.group('attrs')
            cell = f"{sheet_name}!{_first_ref(attrs)}"
            if not value_element_rgx.search(body):
                self.refused.append({'kind': 'formula', 'where': cell,
                                     'reason': 'shared slave with no cached value',
                                     'text': f'shared si={si.group(1)}'})
                return match.group(0)
            self.fixed['frozen'].append(
                {'kind': 'formula', 'where': cell,
                 'before': f'shared si={si.group(1)}',
                 'after': _snip(_value_text(body))})
            return f"<c{attrs}>{body.replace(formula.group(0), '', 1)}</c>"

        return cell_element_rgx.sub(rewrite, xml)

    def _resolve_sheet_features(self, sheet_name: str, xml: str) -> str:
        cf_policy = self.policies['conditional_formatting']
        dv_policy = self.policies['data_validation']

        def rewrite_cf_block(block):
            sqref = _first_sqref(block.group(0))
            where = f"{sheet_name}!{sqref}"

            def rewrite_rule(rule):
                text = rule.group(0)
                if not external_index_rgx.search(text):
                    return text
                local = self._retarget_text(text)
                if local:
                    self.fixed['retargeted'].append(
                        {'kind': 'conditional_formatting', 'where': where,
                         'before': _snip(_inner_formulas(text)),
                         'after': _snip(_inner_formulas(local))})
                    return local
                if cf_policy == POLICY_DROP:
                    self.fixed['dropped'].append(
                        {'kind': 'conditional_formatting', 'where': where,
                         'before': _snip(_inner_formulas(text))})
                    return ''
                self.refused.append({'kind': 'conditional_formatting', 'where': where,
                                     'reason': 'policy refuse',
                                     'text': _snip(_inner_formulas(text))})
                return text

            body = cf_rule_element_rgx.sub(rewrite_rule, block.group('body'))
            if '<cfRule' not in body:
                return ''
            return block.group(0).replace(block.group('body'), body, 1)

        xml = conditional_formatting_element_rgx.sub(rewrite_cf_block, xml)

        def rewrite_dv_block(block):
            def rewrite_rule(rule):
                text = rule.group(0)
                if not external_index_rgx.search(text):
                    return text
                where = f"{sheet_name}!{_first_sqref(text)}"
                local = self._retarget_text(text)
                if local:
                    self.fixed['retargeted'].append(
                        {'kind': 'data_validation', 'where': where,
                         'before': _snip(_inner_formulas(text)),
                         'after': _snip(_inner_formulas(local))})
                    return local
                if dv_policy == POLICY_DROP:
                    self.fixed['dropped'].append(
                        {'kind': 'data_validation', 'where': where,
                         'before': _snip(_inner_formulas(text))})
                    return ''
                self.refused.append({'kind': 'data_validation', 'where': where,
                                     'reason': 'policy refuse',
                                     'text': _snip(_inner_formulas(text))})
                return text

            body = data_validation_element_rgx.sub(rewrite_rule, block.group('body'))
            remaining = len(data_validation_element_rgx.findall(body))
            if remaining == 0:
                return ''
            attrs = attr_count_rgx.sub(f'count="{remaining}"', block.group('attrs'))
            return f'<dataValidations{attrs}>{body}</dataValidations>'

        return data_validations_element_rgx.sub(rewrite_dv_block, xml)

    def resolve_charts(self) -> None:
        for part in self.order:
            if not chart_part_rgx.match(part):
                continue
            xml = self._text(part)
            if '[' not in xml:
                continue

            def rewrite(match):
                text = match.group(1)
                if not external_index_rgx.search(text):
                    return match.group(0)
                local = self._retarget_text(text)
                if local:
                    self.fixed['retargeted'].append(
                        {'kind': 'chart_series', 'where': part,
                         'before': _snip(text), 'after': _snip(local)})
                    return f'<c:f>{local}</c:f>'
                self.refused.append({'kind': 'chart_series', 'where': part,
                                     'reason': 'chart series does not retarget; '
                                               'v1 cannot freeze charts',
                                     'text': _snip(text)})
                return match.group(0)

            self._set_text(part, chart_formula_rgx.sub(rewrite, xml))

    def refuse_forced(self) -> None:
        """Carriers the surgery never rewrites but cannot leave behind."""
        for pivot in self.before['pivots']:
            if pivot['external_rid']:
                self.refused.append({
                    'kind': 'pivot_source', 'where': f"{pivot['on_sheet']}!{pivot['location']}",
                    'reason': 'pivot cache sourced from an external link',
                    'text': pivot['external_part']})
        for feature in self.before['sheet_features']:
            if feature['kind'] in ('conditional_formatting', 'data_validation'):
                continue
            self.refused.append({
                'kind': feature['kind'], 'where': f"{feature['sheet']}!{feature['sqref'] or '?'}",
                'reason': f"[N] inside <{feature['element']}>; no v1 rule for this carrier",
                'text': feature['snippet']})
        for section in LIMBO_SECTIONS:
            for entry in self.before[section]:
                self.limbo.append({'kind': section, 'detail': entry})

    def survivors(self) -> list:
        """Every carrier part still holding [N] after resolution - must be
        empty. Literal data is not a carrier: cached values and inline
        strings are blanked before the scan, and parts that hold only
        user text (shared strings, comments, docProps) are not scanned."""
        found = []
        for part in self.order:
            is_carrier = (part == 'xl/workbook.xml' or worksheet_part_rgx.match(part)
                          or chart_part_rgx.match(part)
                          or pivot_cache_definition_part_rgx.match(part))
            if not is_carrier:
                continue
            text = self._text(part)
            if worksheet_part_rgx.match(part):
                text = value_element_rgx.sub('', text)
                text = inline_string_element_rgx.sub('', text)
            for match in external_index_rgx.finditer(text):
                start = text.rfind('<', 0, match.start())
                found.append({'part': part, 'context': _snip(text[start:match.end() + 40])})
                if len(found) > 50:
                    return found
        return found

    # ---- plumbing --------------------------------------------------------------

    def remove_plumbing(self) -> dict:
        removed = {'parts': [], 'rels': 0, 'overrides': 0, 'calc_chain': False}
        workbook = self._text('xl/workbook.xml')
        workbook = external_references_block_rgx.sub('', workbook)
        workbook = self._force_recalc(workbook)
        self._set_text('xl/workbook.xml', workbook)

        rels_part = 'xl/_rels/workbook.xml.rels'
        rels = self._text(rels_part)

        def drop_link_rel(match):
            element = match.group(0)
            if external_link_rel_type_rgx.search(element) or calc_chain_rel_type_rgx.search(element):
                removed['rels'] += 1
                return ''
            return element

        self._set_text(rels_part, relationship_element_rgx.sub(drop_link_rel, rels))

        types = self._text('[Content_Types].xml')
        types, count = external_link_override_rgx.subn('', types)
        removed['overrides'] += count
        types, count = calc_chain_override_rgx.subn('', types)
        removed['overrides'] += count
        self._set_text('[Content_Types].xml', types)

        for part in list(self.order):
            if part.startswith('xl/externalLinks/') or part == 'xl/calcChain.xml':
                removed['parts'].append(part)
                if part == 'xl/calcChain.xml':
                    removed['calc_chain'] = True
                self.order.remove(part)
                del self.parts[part]
                self.removed_parts.add(part)
        return removed

    def _force_recalc(self, workbook: str) -> str:
        calc = calc_pr_rgx.search(workbook)
        if not calc:
            return workbook_close_rgx.sub(
                '<calcPr fullCalcOnLoad="1" calcCompleted="0"/></workbook>', workbook, 1)
        element = calc.group(0)
        element = calc_mode_manual_rgx.sub('calcMode="auto"', element)
        element = full_calc_on_load_rgx.sub('', element)
        element = calc_completed_rgx.sub('', element)
        element = element.replace('<calcPr', '<calcPr fullCalcOnLoad="1" calcCompleted="0"', 1)
        return workbook.replace(calc.group(0), element, 1)

    # ---- driver ----------------------------------------------------------------

    def run(self, heartbeat=None) -> dict:
        self._collect_local_names()
        self.resolve_defined_names()
        self.resolve_worksheets(heartbeat)
        self.resolve_charts()
        self.refuse_forced()
        survivors = [] if self.refused else self.survivors()
        for survivor in survivors:
            self.refused.append({'kind': 'unclassified', 'where': survivor['part'],
                                 'reason': '[N] survived every rule',
                                 'text': survivor['context']})
        removed = {} if self.refused else self.remove_plumbing()
        return {
            'source': self.source_path,
            'fixed': self.fixed,
            'refused': self.refused,
            'limbo': self.limbo,
            'plumbing_removed': removed,
            'before_summary': self.before['summary'],
        }

    def write(self, output_path: str, in_place: bool, backup_suffix: str,
              verify_output=None, verify_with_openpyxl: bool = False) -> dict:
        """Hand the plan to the shared write-and-verify path."""
        return write_and_verify(
            self.source_path, self.order, self.parts,
            self.changed_parts, self.removed_parts, output_path,
            in_place, backup_suffix, verify_output, verify_with_openpyxl)


# ---- small pulls ----------------------------------------------------------------

def _first_name(attrs: str) -> str:
    match = attr_name_rgx.search(attrs)
    return match.group(1) if match else '?'


def _first_ref(attrs: str) -> str:
    match = cell_ref_attr_rgx.search(attrs)
    return match.group(1) if match else '?'


def _first_sqref(text: str) -> str:
    match = attr_sqref_rgx.search(text)
    return match.group(1) if match else '?'


def _value_text(body: str) -> str:
    match = value_element_rgx.search(body)
    if not match:
        return ''
    inner = match.group(0)
    start, end = inner.find('>') + 1, inner.rfind('<')
    return inner[start:end] if 0 < start <= end else ''


def _inner_formulas(element: str) -> str:
    """The formula texts of a CF/DV element, for the report."""
    texts = []
    position = 0
    while True:
        open_at = element.find('<formula', position)
        if open_at < 0:
            break
        text_at = element.find('>', open_at) + 1
        close_at = element.find('</formula', text_at)
        if close_at < 0:
            break
        texts.append(element[text_at:close_at])
        position = close_at
    return ' | '.join(texts)


def validate_policies(policies: dict, step_name: str, error_class) -> None:
    """Refuse a policy word outside each carrier's vocabulary."""
    allowed = {'formula': FORMULA_POLICIES, 'defined_name': DROPPABLE_POLICIES,
               'conditional_formatting': DROPPABLE_POLICIES,
               'data_validation': DROPPABLE_POLICIES}
    for carrier, choices in allowed.items():
        value = policies.get(carrier)
        if value not in choices:
            raise error_class(
                f"Step '{step_name}': unresolved_{carrier}_policy must be one of "
                f"{list(choices)}, got {value!r}")

# End of file #
