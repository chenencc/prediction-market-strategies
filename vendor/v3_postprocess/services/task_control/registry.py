"""One pinned definition/cadence registry, with fail-closed runtime defaults."""
from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
import json
from pathlib import Path
from .contracts import require, integer, digest, verified, seal

OPENING = 'opening_research'
PAIR = 'cross_platform_pair'
COMBINATION = 'same_platform_combination'
CROSS_SECTIONAL = 'cross_sectional'
KINDS = (OPENING, PAIR, COMBINATION, CROSS_SECTIONAL)
MYRIAD_PAIR = 'polymarket_myriad_pair'
TASK5 = MYRIAD_PAIR
SUPPORTED_KINDS = (*KINDS,MYRIAD_PAIR)
ROOT = Path(__file__).resolve().parents[2]
_DEFINITION_SCOPE=ContextVar('validated_task_definition_scope',default=None)


def load_definitions():
    scoped=_DEFINITION_SCOPE.get()
    if scoped is not None:return deepcopy(scoped)
    result = verified(json.loads((ROOT/'config/task-definitions.v1.json').read_text(encoding='utf8')), 'four-task-definitions/1')
    require(set(result['tasks']) == set(KINDS), 'four_task_definitions_required')
    for index, kind in enumerate(KINDS, 1):
        definition = result['tasks'][kind]
        require(definition['priority'] == index and definition['number'] == index, 'task_priority_changed')
        expected = 1_000_000_000 if kind == COMBINATION else 4_000_000_000
        require(definition['state_view_interval_ns'] == expected, 'task_cadence_definition_mismatch')
        require(definition['requires_intelligence'] is (kind == OPENING), 'intelligence_definition_mismatch')
    for path, expected in result['policy_hashes'].items():
        p = ROOT/path
        require(p.resolve().is_relative_to(ROOT.resolve()), 'policy_path_outside_repository')
        require(digest(json.loads(p.read_text(encoding='utf8'))) == expected, 'selection_policy_changed_without_version')
    return result


@contextmanager
def definition_scope():
    """Reuse one verified immutable registry only within a pure CPU operation."""
    token=_DEFINITION_SCOPE.set(load_definitions())
    try:yield
    finally:_DEFINITION_SCOPE.reset(token)


def definition(kind):
    if kind==MYRIAD_PAIR:return deepcopy(task5_definition()['definition'])
    require(kind in KINDS, 'unsupported_task_type')
    scoped=_DEFINITION_SCOPE.get()
    if scoped is not None:return deepcopy(scoped['tasks'][kind])
    return deepcopy(load_definitions()['tasks'][kind])


def task5_definition():
    """Separate extension hash: existing four-task ledgers never change."""
    value=deepcopy(load_definitions()['tasks'][PAIR])
    value.update(number=5,priority=5,required_venues=['polymarket','myriad'],default_max_concurrent_groups=0)
    return seal('task5-definition/1',task_type=MYRIAD_PAIR,definition=value,
        inherits_filter_definition_hash=load_definitions()['contract_hash'],inherits_filter_task=PAIR,
        native_mapping_is_candidate_hint_only=True,recording_enabled_by_default=False)


def definition_hash(kind):
    if kind==MYRIAD_PAIR:return task5_definition()['contract_hash']
    scoped=_DEFINITION_SCOPE.get()
    return scoped['contract_hash'] if scoped is not None else load_definitions()['contract_hash']


def validate_members(kind, values):
    d = definition(kind)
    venues = {v.split(':', 1)[0] for v in values}
    require(len(values) >= d['minimum_members'], 'insufficient_task_members')
    require(d['maximum_members'] is None or len(values) <= d['maximum_members'], 'too_many_task_members')
    require(venues == set(d['required_venues']), 'task_venue_mismatch')


def load_config(value=None):
    if value is None:
        value = json.loads((ROOT/'config/task-system.v1.json').read_text(encoding='utf8'))
    value = deepcopy(value)
    prior_hash = value.pop('config_hash', None)
    if prior_hash is not None:
        require(prior_hash == digest(value), 'configuration_hash_mismatch')
    require(set(value) == {'schema','definitions_hash','enabled','task_limits','max_distinct_markets',
                          'predict_key_slots','request_budgets','segment_seconds','directory_timezone'},
            'unsupported_configuration_fields')
    require(value.get('schema') == 'task-system-config/1', 'configuration_schema_mismatch')
    definitions = load_definitions()
    require(value['definitions_hash'] == definitions['contract_hash'], 'configuration_definitions_mismatch')
    require(type(value['enabled']) is bool, 'enabled_must_be_boolean')
    require(set(value['task_limits']) == set(KINDS), 'four_task_limits_required')
    for row in value['task_limits'].values():
        require(set(row) == {'enabled','max_concurrent_groups'} and type(row['enabled']) is bool, 'invalid_task_limit')
        integer(row['max_concurrent_groups'], 'invalid_group_limit')
        require(not row['enabled'] or row['max_concurrent_groups'] > 0, 'enabled_task_needs_explicit_capacity')
    integer(value['max_distinct_markets'], 'invalid_market_limit')
    integer(value['predict_key_slots'], 'invalid_key_slots', 1)
    require(value['predict_key_slots'] <= 4, 'at_most_four_predict_keys')
    require(value['directory_timezone'] == 'Asia/Shanghai', 'directory_timezone_mismatch')
    integer(value['segment_seconds'], 'invalid_segment_duration', 1)
    require(value['segment_seconds'] <= 3600, 'segment_duration_too_long')
    budgets = value['request_budgets']
    require(set(budgets) == {'polymarket_shared_rpm','predict_per_key_rpm','predict_shared_host_rpm'}, 'explicit_request_budgets_required')
    for amount in budgets.values():
        require(amount is None or type(amount) is int and amount > 0, 'invalid_request_budget')
    if value['enabled']:
        require(value['max_distinct_markets'] > 0 and any(r['enabled'] for r in value['task_limits'].values()), 'runtime_capacity_required')
        require(budgets['polymarket_shared_rpm'] is not None, 'runtime_request_budgets_required')
        if value['task_limits'][PAIR]['enabled']:
            require(budgets['predict_per_key_rpm'] is not None and budgets['predict_shared_host_rpm'] is not None,
                    'runtime_predict_request_budgets_required')
    value['config_hash'] = digest(value)
    return value
