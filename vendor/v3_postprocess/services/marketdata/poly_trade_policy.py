"""Shared public-trade window/cache policy; all physical calls remain quota gated.

The collector cache bucket is a client refresh key, not a server-side time
filter. Acceptance still requires measured HTTP age and strict WS identities.
"""
POLICY='polymarket-trades/2'
OVERLAP_NS=300*10**9
INDEX_GRACE_NS=30*10**9
PRECISION_NS=10**9
CLOCK_DISAGREEMENT_NS=5*10**9

def query(conditions,at_ns,refresh_seconds=15,bucket=None):
    conditions=sorted(set(str(c).lower() for c in conditions))
    if not conditions or len(conditions)>20:raise ValueError('bounded_condition_query_required')
    return dict(condition=','.join(conditions),taker_only='true',limit=200,
        _collector_cache_bucket=str(bucket if bucket is not None else at_ns//(refresh_seconds*10**9)))

def window(execution_ns,lower_ns,upper_ns):
    """Seconds are an interval, never silently fabricate a subsecond execution."""
    included=execution_ns<upper_ns and execution_ns+PRECISION_NS>lower_ns
    membership='inside' if lower_ns<=execution_ns and execution_ns+PRECISION_NS<=upper_ns else 'boundary_candidate'
    return included,membership

def linked_window(key,execution_ns,lower_ns,upper_ns,witness,asof_ns):
    """Keep exact-identity WS/API clock disagreements explicit, never shift clocks."""
    included,membership=window(execution_ns,lower_ns,upper_ns)
    if included:return included,membership,None
    g=witness.groups.get(key)
    if not g or g['available_at_ns']>asof_ns:return False,membership,None
    times=sorted(t for t in g['source_times'] if lower_ns<=t<upper_ns and
                 abs(t-execution_ns)<=CLOCK_DISAGREEMENT_NS)
    if not times:return False,membership,None
    evidence=dict(key=list(key),API_source_ns=execution_ns,WS_source_ns=times,
        first_WS_received_ns=g['available_at_ns'],native_reference=g.get('native_reference'),
        max_clock_disagreement_ns=CLOCK_DISAGREEMENT_NS)
    return True,'ws_linked_clock_disagreement',evidence
