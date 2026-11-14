"""Explicit books-only physical Poly sharing between independent Pair consumers."""
from task_control.contracts import digest,require

def identity(market):
    return digest(dict(member=market['member'],instruments=sorted(market['instruments'],key=lambda r:r['asset_id'])))

def conflict(members,owned,enabled=False):
    return any(m in owned and not (enabled and m.startswith('polymarket:')) for m in members)

def merge(feeds,market,enabled=False):
    member=market['member']
    if member in feeds:
        require(enabled and member.startswith('polymarket:'),'duplicate_physical_owner')
        require(identity(feeds[member])==identity(market),'shared_market_instrument_identity_changed')
    else:feeds[member]=market

def releasable_members(state,session_id):
    """A logical close releases only members without another live consumer."""
    s=state['sessions'][session_id]
    needed={m for sid,row in state['sessions'].items() if sid!=session_id and row['lifecycle']!='closed'
            for m in row['offer']['candidate']['members']}
    return set(s['offer']['candidate']['members'])-needed
