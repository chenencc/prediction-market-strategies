"""Balanced whole-group admission, distinct from legacy priority arbitration.

Resource estimates are reservations, not measured throughput guarantees. Current
owners remain sticky. Failed/pending exports never create a second physical feed.
"""
from copy import deepcopy
from task_control.contracts import require, integer, seal
from task_control.registry import PAIR, COMBINATION, CROSS_SECTIONAL,MYRIAD_PAIR
from task_control.requests import market_inputs
from task_control.selection import validate_candidate
from .shared_feeds import merge,conflict

TASKS = (PAIR, COMBINATION, CROSS_SECTIONAL)
VENUES = ('polymarket', 'predictfun')


def task5_enabled(capacity):return capacity.get('task5',{}).get('enabled',False)
def task_types(capacity):return (*TASKS,MYRIAD_PAIR) if task5_enabled(capacity) else TASKS
def venue_types(capacity):return (*VENUES,'myriad') if task5_enabled(capacity) else VENUES


def limits(value):
    value = deepcopy(value)
    sharing=value.pop('shared_polymarket',None)
    if sharing is not None:require(sharing is True,'explicit_shared_polymarket_opt_in_required')
    extension=value.pop('task5',None)
    if extension is not None:
        require(isinstance(extension,dict) and extension=={'enabled':True} and type(extension['enabled']) is bool,'task5_requires_explicit_opt_in')
        value['task5']=extension
    required={'groups_per_task', 'physical_markets', 'markets_by_venue','outcome_streams','request_rpm_by_venue','candidate_max_age_ns'}
    if extension:required.add('task5')
    require(set(value) in (required,required|{'group_caps_by_task','total_groups'}),
            'explicit_mixed_resource_limits_required')
    for key in ('groups_per_task', 'physical_markets', 'outcome_streams', 'candidate_max_age_ns'):
        integer(value[key], 'positive_mixed_limit_required', 1)
    for key in ('markets_by_venue', 'request_rpm_by_venue'):
        require(set(value[key]) == set(venue_types(value)), 'both_venue_limits_required')
        for amount in value[key].values():integer(amount, 'invalid_venue_limit')
    if 'total_groups' in value:
        integer(value['total_groups'],'positive_group_pool_required',1)
        require(set(value['group_caps_by_task'])==set(task_types(value)),'all_task_group_caps_required')
        for amount in value['group_caps_by_task'].values():
            integer(amount,'nonnegative_task_group_cap_required')
            require(amount<=value['total_groups'],'task_cap_exceeds_group_pool')
        require(value['group_caps_by_task'][PAIR]<=value['groups_per_task'],'pair_cannot_borrow_polymarket_slots')
        require(value['total_groups']<=sum(value['group_caps_by_task'].values()),'insufficient_task_caps_for_group_pool')
    if extension:
        require('total_groups' in value and value['group_caps_by_task'][MYRIAD_PAIR]>0 and
            value['markets_by_venue']['myriad']>0 and value['request_rpm_by_venue']['myriad']>0,
            'task5_requires_explicit_group_pool_and_venue_budgets')
    if sharing:
        require(value.get('group_caps_by_task',{}).get(COMBINATION)==0 and
                value.get('group_caps_by_task',{}).get(CROSS_SECTIONAL)==0,'sharing_requires_only_two_pair_classes')
        value['shared_polymarket']=True
    return value


def group_caps(capacity):
    return dict(capacity.get('group_caps_by_task') or {t:capacity['groups_per_task'] for t in TASKS})


def group_total(capacity):return capacity.get('total_groups',capacity['groups_per_task']*len(TASKS))


def discovery_targets(capacity,counts,*,baseline=False,usage=None):
    caps=group_caps(capacity);remaining=max(0,group_total(capacity)-sum(counts.values()))
    if baseline:caps={t:min(n,capacity['groups_per_task']) for t,n in caps.items()}
    result={t:min(remaining,max(0,caps[t]-counts.get(t,0))) for t in task_types(capacity)}
    if usage is None:return result
    # Lower bounds only: complete real offers still pass the authoritative plan.
    # No candidate requests are useful if even this task's smallest whole group
    # cannot fit the existing physical/outcome/venue/request reservations.
    minimum={PAIR:dict(polymarket=1,predictfun=1),COMBINATION:dict(polymarket=2),
        CROSS_SECTIONAL:dict(polymarket=1),MYRIAD_PAIR:dict(polymarket=1,myriad=1)}
    for task,target in result.items():
        needed=minimum[task];markets=sum(needed.values());bounds=[target,
            max(0,capacity['physical_markets']-usage['physical_markets'])//markets,
            max(0,capacity['outcome_streams']-usage['outcome_streams'])//(2*markets)]
        for venue,n in needed.items():
            rpm=5*n if venue=='myriad' else n
            bounds.extend((max(0,capacity['markets_by_venue'][venue]-usage['markets_by_venue'].get(venue,0))//n,
                max(0,capacity['request_rpm_by_venue'][venue]-usage['request_rpm_by_venue'].get(venue,0))//rpm))
        result[task]=min(bounds)
    return result


def offer(selection, *, markets, request_rpm_by_member, selected_at_ns):
    c = validate_candidate(selection)
    require(c['decision'] == 'include' and c['task_type'] in (*TASKS,MYRIAD_PAIR), 'included_task234_candidate_required')
    integer(selected_at_ns, 'invalid_offer_clock')
    require(c['observed_at_ns'] <= selected_at_ns, 'future_candidate')
    inputs = market_inputs(markets, c['members'], selected_at_ns)
    require(set(request_rpm_by_member) == set(c['members']), 'all_member_request_reservations_required')
    for amount in request_rpm_by_member.values():integer(amount, 'positive_member_rpm_required', 1)
    return seal('mixed-recording-offer/1', candidate=c, markets=inputs,
                selected_at_ns=selected_at_ns, request_rpm_by_member=request_rpm_by_member)


def validate_offer(value):
    from task_control.contracts import verified
    value = verified(value, 'mixed-recording-offer/1')
    rebuilt = offer(value['candidate'], **{k:value[k] for k in (
        'markets', 'request_rpm_by_member', 'selected_at_ns')})
    require(value == rebuilt, 'mixed_offer_fields_changed')
    return value


def footprint(offers):
    return _validated_footprint([validate_offer(value) for value in offers])


def _validated_footprint(offers,*,tasks=None,venues=None,shared_polymarket=False):
    """Internal only: plan validates contracts once before capacity arithmetic."""
    offers=list(offers);extra=any(o['candidate']['task_type']==MYRIAD_PAIR for o in offers)
    tasks=tasks or ((*TASKS,MYRIAD_PAIR) if extra else TASKS);venues=venues or ((*VENUES,'myriad') if extra else VENUES)
    feeds = {}; counts = {task:0 for task in tasks}
    for value in offers:
        c = value['candidate']; counts[c['task_type']] += 1
        for market in value['markets']:
            member = market['member']
            merge(feeds,market,shared_polymarket)
    by_venue = {v:sum(m.startswith(v+':') for m in feeds) for v in venues}
    reserved={}
    for o in offers:
        for m,n in o['request_rpm_by_member'].items():reserved[m]=max(reserved.get(m,0),n)
    rpm = {v:sum(n for m,n in reserved.items() if m.startswith(v+':')) for v in venues}
    return dict(groups_by_task=counts, physical_markets=len(feeds), markets_by_venue=by_venue,
                outcome_streams=sum(len(m['instruments']) for m in feeds.values()),
                request_rpm_by_venue=rpm)


def plan(candidates, *, active=(), draining=(), capacity, at_ns, allow_new=True,tail_rpm_by_venue=None):
    # Validate policy files once per pure admission operation, not thousands
    # of times while validating already-owned contracts. The scope is reset
    # before the next operation, so an on-disk policy change still fails closed.
    from task_control.registry import definition_scope
    with definition_scope():
        return _plan(candidates,active=active,draining=draining,capacity=capacity,at_ns=at_ns,
            allow_new=allow_new,tail_rpm_by_venue=tail_rpm_by_venue)


def _plan(candidates, *, active=(), draining=(), capacity, at_ns, allow_new=True,tail_rpm_by_venue=None):
    """Whole-group resource admission, with optional Task3/4 slot borrowing.

    Draining groups no longer consume a logical slot but retain physical/resource
    ownership until the adapter acknowledges release. Metadata absence is not a
    closure and does not appear here as permission to replace individual legs.
    """
    capacity = limits(capacity); integer(at_ns, 'invalid_planning_clock')
    tasks=task_types(capacity);venues=venue_types(capacity)
    tail_rpm_by_venue=tail_rpm_by_venue or {v:0 for v in venues}
    require(set(tail_rpm_by_venue)==set(venues),'both_tail_venue_reservations_required')
    for n in tail_rpm_by_venue.values():integer(n,'nonnegative_tail_reservation_required')
    def reserved_footprint(rows):
        value=_validated_footprint(rows,tasks=tasks,venues=venues,shared_polymarket=capacity.get('shared_polymarket',False))
        for v in venues:value['request_rpm_by_venue'][v]+=tail_rpm_by_venue[v]
        return value
    active = [validate_offer(x) for x in active]; draining = [validate_offer(x) for x in draining]
    require(all(x['candidate']['task_type'] in tasks for x in active+draining),'active_task_requires_explicit_capacity')
    reserved = active + draining; usage = reserved_footprint(reserved)
    counts = _validated_footprint(active,tasks=tasks,venues=venues,shared_polymarket=capacity.get('shared_polymarket',False))['groups_by_task']; owned = {m for x in reserved for m in x['candidate']['members']}
    ids = {x['candidate']['group_id'] for x in reserved}
    rows = [validate_offer(x) for x in candidates]
    rows.sort(key=lambda x:(x['candidate']['group_id'], -x['candidate']['observed_at_ns'],
                            -x['selected_at_ns'], x['contract_hash']))
    buckets = {task:[] for task in tasks}; decisions = []; admitted = []
    for row in rows:
        c = row['candidate']; reason = None
        if c['task_type'] not in tasks:reason = 'task5_disabled_by_default'
        elif c['group_id'] in ids:reason = 'already_owned_or_duplicate_candidate'
        elif c['observed_at_ns'] > at_ns or row['selected_at_ns'] > at_ns:reason = 'future_candidate'
        elif at_ns - c['observed_at_ns'] > capacity['candidate_max_age_ns']:reason = 'candidate_requires_refresh'
        if reason:decisions.append(dict(offer_hash=row['contract_hash'], reason=reason))
        else:
            buckets[c['task_type']].append(row); ids.add(c['group_id'])
    while any(buckets.values()):
        task = min((k for k in tasks if buckets[k]), key=lambda k:(counts[k], tasks.index(k)))
        row = buckets[task].pop(0); c = row['candidate']; reason = None
        if not allow_new:reason = 'new_admission_paused'
        elif sum(counts.values())>=group_total(capacity):reason='logical_group_pool_full'
        elif counts[task] >= group_caps(capacity)[task]:reason = 'logical_group_target_full'
        elif conflict(c['members'],owned,capacity.get('shared_polymarket',False)):reason = 'whole_group_owner_conflict'
        else:
            trial = reserved_footprint(reserved + [row])
            if trial['physical_markets'] > capacity['physical_markets']:reason = 'physical_market_cap'
            elif trial['outcome_streams'] > capacity['outcome_streams']:reason = 'outcome_stream_cap'
            elif any(trial['markets_by_venue'][v] > capacity['markets_by_venue'][v] for v in venues):reason = 'venue_market_cap'
            elif any(trial['request_rpm_by_venue'][v] > capacity['request_rpm_by_venue'][v] for v in venues):reason = 'request_reservation_cap'
        if reason is None:
            admitted.append(row); reserved.append(row); counts[task] += 1; owned.update(c['members']); usage = trial
        decisions.append(dict(offer_hash=row['contract_hash'], reason=reason or 'admitted_whole_group'))
    return seal('mixed-admission-plan/1', at_ns=at_ns, weights={k:1 for k in tasks}, capacity=capacity,
                allocation_policy='elastic_task3_task4' if 'total_groups' in capacity else 'equal_class_caps',
                admitted=admitted, decisions=decisions, usage=usage,
                active_groups_by_task=counts, source_quality_certified=False,
                resource_estimates_are_not_live_throughput_proof=True)
