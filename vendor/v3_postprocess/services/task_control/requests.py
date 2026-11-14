"""Immutable draft requests and half-open segments, never execution permission."""
from copy import deepcopy
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from .contracts import (require, integer, text, evidence, digest, seal, verified, sha)
from .registry import OPENING, PAIR, CROSS_SECTIONAL, definition, load_config
from .selection import validate_candidate

SECOND = 1_000_000_000
FILES = ['context.parquet', 'orderbook.parquet', 'trades.parquet']


def market_inputs(values, expected_members, selected_ns):
    require(isinstance(values, list) and bool(values), 'native_market_inputs_required')
    result = deepcopy(values);keys = []
    for row in result:
        require(set(row) == {'member','metadata_ref','instruments'}, 'invalid_native_market_input')
        keys.append(row['member']);evidence([row['metadata_ref']])
        at = row['metadata_ref'].get('observed_at_ns')
        require(type(at) is int and at <= selected_ns, 'metadata_clock_missing_or_future')
        instruments = row['instruments']
        require(isinstance(instruments, list) and len(instruments) >= 2, 'complete_outcome_mapping_required')
        seen = set();assets = set()
        for item in instruments:
            allowed={'outcome_id','asset_id','index_set','book_origin','derived_from_asset_id'}
            if row['member'].startswith('myriad:'):allowed.add('native_outcome_id')
            require(set(item) <= allowed, 'invalid_instrument_fields')
            if row['member'].startswith('myriad:'):
                require(type(item.get('native_outcome_id')) is int and item['native_outcome_id'] in (0,1) and
                    'index_set' not in item and 'derived_from_asset_id' not in item and item.get('book_origin')=='native','myriad_native_outcome_identity_required')
            outcome = text(item.get('outcome_id'), 'outcome_identity_required')
            asset = text(item.get('asset_id'), 'native_asset_id_required')
            require(asset.isascii() and asset.isdecimal(), 'native_asset_id_must_be_decimal_string')
            require(outcome not in seen and asset not in assets, 'duplicate_outcome_or_asset')
            seen.add(outcome);assets.add(asset)
            require(item.get('book_origin') in {'native','derived_complement'}, 'book_origin_required')
            if 'index_set' in item:
                integer(item['index_set'], 'invalid_outcome_index_set', 1)
            if item['book_origin'] == 'derived_complement':
                require(row['member'].startswith('predictfun:'), 'derived_complement_requires_predict')
                text(item.get('derived_from_asset_id'), 'derived_book_parent_required')
        for item in instruments:
            if item['book_origin'] == 'derived_complement':
                require(any(p['asset_id'] == item['derived_from_asset_id'] and p['book_origin'] == 'native' for p in instruments), 'derived_book_parent_not_native')
        if row['member'].startswith('myriad:'):
            require(len(instruments)==2 and {item['native_outcome_id'] for item in instruments}=={0,1},'complete_myriad_native_outcomes_required')
        row['instruments'] = sorted(instruments, key=lambda i:i['outcome_id'])
    require(len(keys) == len(set(keys)) and sorted(keys) == expected_members, 'request_members_do_not_match_candidate')
    return sorted(result, key=lambda r:r['member'])


def recording_request(selection, *, config, markets, selected_at_ns, start_ns,
                      end_ns=None, mode='live', opening=None):
    c = validate_candidate(selection);cfg = load_config(config)
    require(c['decision'] == 'include', 'only_included_candidate_can_request_recording')
    require(mode in {'live','debug','historical_validation'}, 'invalid_recording_mode')
    selected = integer(selected_at_ns, 'request_selection_clock_required')
    start = integer(start_ns, 'recording_start_required')
    require(c['observed_at_ns'] <= selected, 'candidate_observation_in_future')
    d = definition(c['task_type']);clock_role = 'recording_admission_not_market_open'
    if mode == 'live' and c['task_type'] == OPENING:
        require(isinstance(opening, dict) and set(opening) == {'bound_ns','exchange_proof_ref','history_ref'}, 'opening_boundary_and_history_required')
        evidence([opening['exchange_proof_ref'],opening['history_ref']])
        require(start == integer(opening['bound_ns'], 'invalid_opening_bound') and start <= selected, 'opening_window_must_anchor_to_supplied_bound')
        require(end_ns == start + 3600*SECOND, 'opening_first_hour_required')
        clock_role = 'supplied_exchange_opening_bound_not_exact_open_claim'
    elif mode == 'live' and c['task_type'] == CROSS_SECTIONAL:
        require(end_ns is None and start >= selected, 'task4_requires_lifecycle_not_predicted_end')
    elif mode == 'live' and c['task_type'] == PAIR and end_ns is None:
        require(start >= selected and opening is None, 'pair_continuation_clock_invalid')
    else:
        require(opening is None, 'opening_proof_not_applicable')
        require(start >= selected, 'recording_start_before_selection')
    if end_ns is not None:
        integer(end_ns, 'invalid_recording_end')
        require(start < end_ns <= start+86400*SECOND, 'explicit_window_must_be_1ns_to_24h')
    require(end_ns is not None or c['task_type'] in {PAIR,CROSS_SECTIONAL} and mode == 'live', 'finite_window_required')
    inputs = market_inputs(markets, c['members'], selected)
    identity = digest(inputs)
    return seal('recording-request/1', task_type=c['task_type'], group_id=c['group_id'],
        candidate=c, config=cfg, markets=inputs, market_identity_hash=identity,
        selected_at_ns=selected, start_ns=start, end_ns=end_ns, mode=mode,
        opening=deepcopy(opening), clock_role=clock_role,
        end_policy=('either_leg_official_terminal' if c['task_type']==PAIR else 'official_terminal') if end_ns is None else 'explicit_window',
        state_view_interval_ns=d['state_view_interval_ns'], native_capture='all_received_events',
        official_REST_frequency_guaranteed=False, requires_intelligence=d['requires_intelligence'],
        final_data_files=FILES, recording_authorized=False, execution_authorized=False)


def validate_request(value):
    value = verified(value, 'recording-request/1')
    args = {k:value[k] for k in ('config','markets','selected_at_ns','start_ns','end_ns','mode','opening')}
    rebuilt = recording_request(value['candidate'], **args)
    require(rebuilt == value, 'recording_request_fields_inconsistent')
    return value


def footprint(request):
    request = validate_request(request)
    return dict(group_count=1, distinct_markets=len(request['markets']),
        predict_markets=sum(r['member'].startswith('predictfun:') for r in request['markets']),
        outcome_streams=sum(len(r['instruments']) for r in request['markets']),
        native_book_streams=sum(i['book_origin'] == 'native' for r in request['markets'] for i in r['instruments']),
        derived_book_streams=sum(i['book_origin'] != 'native' for r in request['markets'] for i in r['instruments']))


def segment(request, *, sequence, start_ns, end_ns, previous=None):
    request = validate_request(request)
    integer(sequence, 'invalid_segment_sequence');integer(start_ns, 'invalid_segment_start');integer(end_ns, 'invalid_segment_end')
    require(request['start_ns'] <= start_ns < end_ns, 'segment_outside_recording_start')
    require(request['end_ns'] is None or end_ns <= request['end_ns'], 'segment_after_recording_end')
    require(end_ns-start_ns <= request['config']['segment_seconds']*SECOND, 'segment_duration_exceeds_config')
    if previous is None:
        require(sequence == 0, 'first_segment_sequence_must_be_zero')
        predecessor = None;expected_start = request['start_ns']
    else:
        previous = validate_segment(previous, request)
        require(previous['request_hash'] == request['contract_hash'] and previous['group_id'] == request['group_id'], 'segment_predecessor_identity_changed')
        require(sequence == previous['sequence']+1, 'segment_sequence_gap')
        predecessor = previous['contract_hash'];expected_start = previous['end_ns']
    require(start_ns >= expected_start, 'segment_overlap_or_time_reversal')
    result = seal('recording-segment/1', request_hash=request['contract_hash'], group_id=request['group_id'],
        sequence=sequence, start_ns=start_ns, end_ns=end_ns, predecessor_hash=predecessor,
        boundary='[start_ns,end_ns)', continuation_gap=None if start_ns == expected_start else
        dict(start_ns=expected_start, end_ns=start_ns, reason='initial_or_continuation_gap'),
        final_data_files=FILES)
    return validate_segment(result, request)


def validate_segment(value, request):
    request = validate_request(request);value = verified(value, 'recording-segment/1')
    require(set(value) == {'schema','contract_hash','request_hash','group_id','sequence','start_ns','end_ns',
                           'predecessor_hash','boundary','continuation_gap','final_data_files'}, 'invalid_segment_fields')
    require(value['request_hash'] == request['contract_hash'] and value['group_id'] == request['group_id'], 'segment_request_binding_mismatch')
    sequence = integer(value['sequence'], 'invalid_segment_sequence')
    start = integer(value['start_ns'], 'invalid_segment_start');end = integer(value['end_ns'], 'invalid_segment_end')
    require(request['start_ns'] <= start < end and end-start <= request['config']['segment_seconds']*SECOND,
            'invalid_segment_time_bounds')
    require(request['end_ns'] is None or end <= request['end_ns'], 'segment_after_recording_end')
    require(value['boundary'] == '[start_ns,end_ns)' and value['final_data_files'] == FILES, 'segment_format_mismatch')
    if sequence == 0:require(value['predecessor_hash'] is None, 'first_segment_cannot_have_predecessor')
    else:sha(value['predecessor_hash'])
    gap = value['continuation_gap']
    if gap is not None:
        require(isinstance(gap, dict) and set(gap) == {'start_ns','end_ns','reason'}, 'invalid_continuation_gap')
        integer(gap['start_ns'], 'invalid_gap_start');integer(gap['end_ns'], 'invalid_gap_end')
        require(request['start_ns'] <= gap['start_ns'] < gap['end_ns'] == start and
                gap['reason'] == 'initial_or_continuation_gap', 'invalid_continuation_gap')
    if sequence == 0:
        require((start == request['start_ns'] and gap is None) or
                (gap is not None and gap['start_ns'] == request['start_ns']), 'initial_gap_must_be_preserved')
    return value


def storage_route(request, *, actual_started_at_ns, segment_sequence):
    request = validate_request(request)
    integer(actual_started_at_ns, 'actual_start_clock_required');integer(segment_sequence, 'invalid_segment_sequence')
    require(actual_started_at_ns >= request['start_ns'], 'actual_start_before_requested_boundary')
    require(request['end_ns'] is None or actual_started_at_ns < request['end_ns'], 'actual_start_after_recording_window')
    day = datetime.fromtimestamp(actual_started_at_ns//SECOND, timezone.utc).astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()
    return dict(record_date=day, record_date_timezone='Asia/Shanghai',
        clock_role='actual_recording_start_not_opening_or_upload',
        actual_started_at_ns=actual_started_at_ns,
        prefix=f"v1/record_date={day}/task_type={request['task_type']}/group={request['group_id']}/run={request['contract_hash']}/segment={segment_sequence:06}",
        data_files=FILES, control_files=['manifest.json'])
