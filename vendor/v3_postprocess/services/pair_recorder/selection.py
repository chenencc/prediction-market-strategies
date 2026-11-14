"""Only the second task: native-map Pair selection and identity pinning."""
import json
from .rules.contracts import MarketFacts,PairPolicy
from .rules.pairs import select_pairs as original_select_pairs
from task_control import from_legacy,evidence_ref,digest

def decode_market(venue,raw):
    if venue=='polymarket':
        names=raw.get('outcomes',[]);ids=raw.get('clobTokenIds',[])
        names=json.loads(names) if isinstance(names,str) else names;ids=json.loads(ids) if isinstance(ids,str) else ids
        opened=raw.get('active') is True and raw.get('acceptingOrders') is True and raw.get('enableOrderBook') is True and not raw.get('closed') and not raw.get('archived')
        question=raw.get('question','');condition=str(raw.get('conditionId','')).lower()
    elif venue=='predictfun':
        rows=raw.get('outcomes',[]);names=[r.get('name','') for r in rows];ids=[r.get('onChainId','') for r in rows]
        opened=raw.get('tradingStatus')=='OPEN' and raw.get('resolution') is None
        question=raw.get('question') or raw.get('title','');condition=str(raw.get('conditionId','')).lower()
    else:raise ValueError('unsupported_venue')
    labels=[str(n).lower() for n in names]
    if not labels or len(labels)!=len(ids) or len(set(labels))!=len(labels) or len(set(map(str,ids)))!=len(ids):raise ValueError('invalid_outcome_mapping')
    return MarketFacts(venue,str(raw['id']),condition,str(question),str(raw.get('description','')),bool(opened),dict(zip(labels,map(str,ids))),raw)

def select(catalog,*,at,policy=PairPolicy()):return original_select_pairs(catalog,at=at,decode_market=decode_market,policy=policy)

def identity(venue,raw):
    m=decode_market(venue,raw)
    return digest(dict(venue=venue,market_id=m.market_id,condition_id=m.condition_id,outcomes=m.outcomes,protocol_version=raw.get('version'),
                       index_sets={str(r['onChainId']):r.get('indexSet') for r in raw.get('outcomes',[]) if isinstance(r,dict)} if venue=='predictfun' else None))

def native_terminal(venue,raw):
    # A failed read, absent directory row or temporary trading pause is not terminal.
    if venue=='polymarket':return raw.get('closed') is True or raw.get('umaResolutionStatus')=='resolved'
    resolution=raw.get('resolution')
    return isinstance(resolution,dict) and type(resolution.get('indexSet')) is int and resolution['indexSet'] in {1,2}

def normalize(receipt,at_ns):
    return from_legacy(receipt,observed_at_ns=at_ns,evidence_refs=[evidence_ref('pair_selection_receipt',digest(receipt),observed_at_ns=at_ns)])
