"""Portable compatibility primitives for the unchanged Pair rules."""
from dataclasses import dataclass
from datetime import datetime
import re
from task_control.contracts import digest

PAIR='cross_platform_pair'
CONDITION=re.compile(r'0x[0-9a-f]{64}',re.I)

def instant(value):
    if not isinstance(value,str):raise ValueError('timestamp_required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.utcoffset() is None:raise ValueError('timezone_required')
    return result

def receipt(kind,decision,reason,**fields):
    return dict(schema='study-selection/1',task_type=kind,decision=decision,reason=reason,
                recording_authorized=False,requires_intelligence=False,**fields)

def valid_id(value):return not isinstance(value,bool) and isinstance(value,(str,int)) and bool(str(value).strip())

def validate_native_outcomes(venue,raw):
    import json
    if venue=='polymarket':
        names,ids=raw.get('outcomes'),raw.get('clobTokenIds')
        names=json.loads(names) if isinstance(names,str) else names;ids=json.loads(ids) if isinstance(ids,str) else ids
    else:
        rows=raw.get('outcomes')
        if not isinstance(rows,list) or not all(isinstance(row,dict) for row in rows):raise ValueError('invalid_outcome_rows')
        names,ids=[row.get('name') for row in rows],[row.get('onChainId') for row in rows]
    if not isinstance(names,list) or not isinstance(ids,list) or len(names)!=len(ids):raise ValueError('invalid_outcome_arrays')
    if not all(isinstance(n,str) and n.strip() for n in names) or not all(valid_id(i) for i in ids):raise ValueError('invalid_outcome_identity')

@dataclass(frozen=True)
class PairPolicy:
    max_metadata_age_seconds:int=60
    excluded_categories:tuple[str,...]=()
    # Recording a native-mapped relation is not a payout-equivalence verdict.
    # Historical selector replay can explicitly retain its original strict gate.
    require_semantic_match:bool=False

@dataclass(frozen=True)
class MarketFacts:
    venue:str
    market_id:str
    condition_id:str
    question:str
    rules:str
    open_for_orders:bool
    outcomes:dict
    raw:dict
    @property
    def key(self):return self.venue+':'+self.market_id
