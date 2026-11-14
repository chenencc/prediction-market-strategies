"""Explicit physical market end flags in the same raw-shard HF commit."""
from task_control.contracts import verified,require,digest,seal
from task5.selection import native_terminal


def market_index(snapshot):
    s=verified(snapshot,'native-lifecycle-registry-snapshot/1');markets={}
    for session in s['sessions']:
        start=session['actual_started_at_ns'];stop=session['closed_at_ns'] or s['recording_stopped_at_ns']
        require(type(start) is int and (stop is None or start<=stop<=s['at_ns']),'raw_registry_interval_changed')
        for member in session['offer']['candidate']['members']:
            row=markets.setdefault(member,dict(market_closed=False,market_status='unproven',terminal_evidence=None,recordings={}))
            row['recordings'][session['session_id']]=dict(start_ns=start,end_ns=stop)
    for member,row in markets.items():
        row['recording_status']='recording' if any(x['end_ns'] is None for x in row['recordings'].values()) else 'stopped'
        terminal=s['metadata'].get(member)
        if terminal:
            raw=terminal['raw'];ref=terminal['metadata_ref'];venue,mid=member.split(':',1)
            require(ref['kind']=='native_market_metadata' and ref['sha256']==digest(raw) and ref['observed_at_ns']<=s['at_ns'],
                    'raw_registry_terminal_evidence_changed')
            if venue=='myriad':
                from myriad.protocol import identity
                require(identity(raw)==mid,'raw_registry_terminal_member_changed')
            else:require(str(raw.get('id'))==mid,'raw_registry_terminal_member_changed')
            require(native_terminal(venue,raw),'raw_registry_official_terminal_required')
            row.update(market_closed=True,market_status='official_terminal',terminal_evidence=ref)
        row['async_trade_history_status']=('eligible_after_this_manifest_HF_verification'
            if row['market_closed'] and row['recording_status']=='stopped' else 'not_eligible')
    return seal('raw-physical-market-status/1',controller_id=s['controller_id'],source_lifecycle_hash=s['contract_hash'],
        markets=markets,service_recovery=s.get('service_recovery'),archive_coverage_certified=False,
        complete_exchange_fills_certified=False)
