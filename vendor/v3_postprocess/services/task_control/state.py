"""Independent progress axes. Pure transitions are not durable runtime writes.

Evidence references are mandatory for delivery milestones; their bodies and
external effects must be checked by the later publisher/runtime implementation.
"""
from .contracts import require, integer, text, evidence, seal, verified
from .requests import validate_request

EDGES = {
 'lifecycle': {'open':{'window_closed','official_terminal','blocked'},'window_closed':set(),'official_terminal':set(),'blocked':set()},
 'recording': {'pending':{'active','failed'},'active':{'closed','failed'},'closed':set(),'failed':set()},
 'intelligence': {'not_required':set(),'pending':{'running','failed'},'running':{'sealed','failed'},'sealed':set(),'failed':{'running'}},
 'trades': {'pending':{'live','failed'},'live':{'tail_wait','failed'},'tail_wait':{'tail_running','failed'},'tail_running':{'closed','failed'},'closed':set(),'failed':set()},
 'trade_quality': {'unchecked':{'quality_pending','observed_reference_pass'},'quality_pending':{'quality_pending','observed_reference_pass'},'observed_reference_pass':{'quality_pending','observed_reference_pass'}},
 'delivery': {'pending':{'building','failed'},'building':{'sealed','failed'},'sealed':{'uploaded','failed'},'uploaded':{'verified','failed'},'verified':{'retired','failed'},'retired':set(),'failed':{'building'}}
}


def initial_state(request, *, at_ns):
    request = validate_request(request);integer(at_ns, 'invalid_state_clock')
    require(at_ns >= request['selected_at_ns'], 'state_before_request_selection')
    return seal('task-progress/1', request_hash=request['contract_hash'], requires_intelligence=request['requires_intelligence'],
        lifecycle='open', end_policy=request['end_policy'], mode=request['mode'],
        recording='pending', intelligence='pending' if request['requires_intelligence'] else 'not_required',
        trades='pending', trade_quality='unchecked', delivery={k:'pending' for k in ('context','orderbook','trades')},
        updated_at_ns=at_ns, last_event=None)


def transition(state, *, axis, to, at_ns, reason, evidence_refs=()):
    state = verified(state, 'task-progress/1');integer(at_ns, 'invalid_state_clock')
    require(at_ns >= state['updated_at_ns'], 'state_clock_regressed');text(reason, 'transition_reason_required')
    family = 'delivery' if axis.startswith('delivery.') else axis
    require(family in EDGES, 'unsupported_progress_axis')
    if family == 'delivery':
        name = axis.split('.',1)[1];require(name in state['delivery'], 'unsupported_delivery_component')
        old = state['delivery'][name]
    else:
        old = state[axis]
    require(to in EDGES[family].get(old, set()), 'invalid_progress_transition')
    refs = evidence(list(evidence_refs)) if evidence_refs else []
    if family == 'delivery' and to in {'sealed','uploaded','verified','retired'}:
        require(bool(refs), 'delivery_milestone_evidence_required')
    if family == 'delivery' and to == 'retired':
        require(any(r['kind'] == 'guarded_cleanup_receipt' for r in refs), 'cleanup_receipt_required')
    if family == 'delivery' and to == 'verified':
        require(any(r['kind'] == 'exact_revision_readback_receipt' for r in refs), 'exact_readback_receipt_required')
    if family == 'delivery' and to == 'building':
        if name == 'orderbook':require(state['recording'] == 'closed', 'book_segment_not_closed')
        if name == 'trades':require(state['trades'] == 'closed', 'trade_walk_not_closed')
    if axis == 'intelligence' and to == 'sealed':
        require(any(r['kind'] == 'intelligence_seal' for r in refs), 'intelligence_seal_required')
    if axis == 'trade_quality' and to == 'observed_reference_pass':
        require(any(r['kind'] == 'trade_quality_receipt' for r in refs), 'trade_quality_receipt_required')
    if axis == 'lifecycle' and to == 'official_terminal':
        require(state['mode'] == 'live' and any(r['kind'] == 'official_terminal_receipt' for r in refs), 'official_terminal_receipt_required')
    if axis == 'lifecycle' and to == 'window_closed':
        require(state['end_policy'] == 'explicit_window', 'continuous_lifecycle_not_finished_by_segment')
    if family == 'delivery':state['delivery'][name] = to
    else:state[axis] = to
    state.pop('schema');state.pop('contract_hash');state['updated_at_ns'] = at_ns
    state['last_event'] = dict(axis=axis, before=old, after=to, reason=reason, evidence_refs=refs, at_ns=at_ns)
    return seal('task-progress/1', **state)


def completion(state):
    state = verified(state, 'task-progress/1')
    base = all(v == 'retired' for v in state['delivery'].values())
    intelligence = state['intelligence'] in {'sealed','not_required'}
    return dict(base_payloads_delivered_and_retired=base,
        intelligence_complete=intelligence, trade_walk_complete=state['trades'] == 'closed',
        trade_quality=state['trade_quality'],
        segment_payloads_complete=base and intelligence and state['recording'] == state['trades'] == 'closed',
        full_task_complete=base and intelligence and state['recording'] == state['trades'] == 'closed' and
            state['lifecycle'] in {'window_closed','official_terminal'},
        source_trade_completeness_certified=False,
        runtime_side_effects_verified_by_this_contract=False)
