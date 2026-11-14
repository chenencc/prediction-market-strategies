"""Shared, versioned market metadata and Pair links; no duplicated market bodies."""
from copy import deepcopy
from .fees import exact_value,digest,fee_version,canonical,number
from .layout import venue,timestamp,pair_folder
from .objects import reference

VOLATILE={'bestBid','bestAsk','spread','lastTradePrice','liquidity','liquidityNum','liquidityClob',
    'volume','volumeNum','volumeClob','volume24hr','volume1wk','volume1mo','volume1yr',
    'outcomePrices','stats','updatedAt','oneHourPriceChange','oneDayPriceChange','oneWeekPriceChange','oneMonthPriceChange','competitive'}

def market_version(platform,raw,*,observed_at_ns,source_ref):
    venue(platform);timestamp(observed_at_ns)
    if 'id' not in raw:raise ValueError('native_market_id_required')
    if platform=='myriad':
        from myriad.protocol import identity,metadata
        raw=metadata(raw)['native'];mid=identity(raw)
    else:mid=str(raw['id'])
    stable=exact_value({k:v for k,v in raw.items() if k not in VOLATILE});objects={}
    fees=fee_version(platform,mid,raw,observed_at_ns=observed_at_ns,source_ref=source_ref)
    if platform=='polymarket':
        import json
        names=raw.get('outcomes',[]);assets=raw.get('clobTokenIds',[])
        names=json.loads(names) if isinstance(names,str) else names;assets=json.loads(assets) if isinstance(assets,str) else assets
        if len(names)!=len(assets):raise ValueError('native_outcome_mapping_lengths_differ')
        instruments=[dict(label=n,asset_id=str(a),book_origin='native') for n,a in zip(names,assets)]
    elif platform=='myriad':
        instruments=[dict(label=o.get('title'),asset_id=str(o['tokenId']),index_set=None,
            native_outcome_id=o['id'],book_origin='native',derived_from_asset_id=None,
            reported_closing_price=number(o.get('closingPrice')),
            reported_closing_price_unit='native_closing_price' if o.get('closingPrice') is not None else None)
            for o in raw['outcomes']]
    else:
        native=raw.get('outcomes') or [];first=next((str(o['onChainId']) for o in native if o.get('indexSet')==1),None)
        binary=len(native)==2 and {o.get('indexSet') for o in native}=={1,2}
        instruments=[]
        for outcome in native:
            derived=binary and outcome.get('indexSet')==2
            instruments.append(dict(label=outcome.get('name'),asset_id=str(outcome['onChainId']),index_set=outcome.get('indexSet'),
                book_origin='derived_complement' if derived else 'native' if outcome.get('indexSet')==1 else 'native_mapping_only',
                derived_from_asset_id=first if derived else None,
                extra={k:v for k,v in outcome.items() if k not in {'name','onChainId','indexSet'}}))
    parents=[]
    for parent in raw.get('events') or []:
        if not isinstance(parent,dict):raise ValueError('parent_event_metadata_object_required')
        item={k:v for k,v in parent.items() if k not in VOLATILE and k!='markets'}
        if isinstance(parent.get('markets'),list):item['market_ids']=[str(m['id']) for m in parent['markets'] if isinstance(m,dict) and 'id' in m]
        parents.append(reference(item,objects,available_at_ns=observed_at_ns))
    typed={'id','conditionId','question','title','description','outcomes','clobTokenIds','events','orderPriceMinTickSize','orderMinSize','decimalPrecision','negRisk','isNegRisk'}
    extra={k:v for k,v in stable.items() if k not in typed and 'fee' not in k.lower()}
    if platform=='myriad':
        extra.update(nativePriceScale=18,nativeAmountDecimals=raw['token']['decimals'])
    row=dict(schema='shared-market-version/2',venue=platform,market_id=str(raw['id']),
        condition_id=raw.get('conditionId'),question=raw.get('question',raw.get('title')),
        title=raw.get('title'),description=raw.get('description'),
        instrument_set_ref=reference(instruments,objects,available_at_ns=observed_at_ns),
        parent_event_set_ref=reference(parents,objects,available_at_ns=observed_at_ns),
        price_increment=number(raw.get('orderPriceMinTickSize')),
        minimum_order_size=number(raw.get('orderMinSize')),
        price_decimals=raw.get('decimalPrecision'),is_neg_risk=raw.get('negRisk',raw.get('isNegRisk')),
        extra_metadata_ref=reference(extra,objects,available_at_ns=observed_at_ns))
    row['market_id']=mid
    row['market_version_id']=digest(row);row.update(fee_version_id=fees['fee_version_id'],available_at_ns=observed_at_ns,source_ref=source_ref,source_effective_at_ns=None)
    row['_objects']=list(objects.values())
    return row,fees

def version_observation(market,fees):
    """Retain A->B->A assignments even though each semantic version is stored once."""
    if market['venue']!=fees['venue'] or market['market_id']!=fees['market_id'] or market['fee_version_id']!=fees['fee_version_id']:raise ValueError('market_fee_version_mismatch')
    if market['available_at_ns']!=fees['available_at_ns'] or market['source_ref']!=fees['source_ref']:raise ValueError('version_observation_source_mismatch')
    row=dict(schema='market-version-observation/1',venue=market['venue'],market_id=market['market_id'],
        market_version_id=market['market_version_id'],fee_version_id=fees['fee_version_id'],
        available_at_ns=market['available_at_ns'],source_effective_at_ns=fees['source_effective_at_ns'],source_ref=market['source_ref'])
    row['observation_id']=digest(row);return row

def pair_descriptor(pair,*,actual_started_at_ns,available_at_ns,market_versions,fee_versions):
    timestamp(actual_started_at_ns);timestamp(available_at_ns)
    if available_at_ns<actual_started_at_ns:raise ValueError('Pair_relation_available_before_actual_start')
    members=pair['members'];venues={m.split(':',1)[0] for m in members}
    task5=pair.get('task_type')=='polymarket_myriad_pair'
    expected={'polymarket','myriad'} if task5 else {'polymarket','predictfun'}
    if len(members)!=2 or venues!=expected:raise ValueError('one_market_per_Pair_venue_required')
    if set(market_versions)!=set(members) or set(fee_versions)!=set(members):raise ValueError('all_Pair_legs_require_shared_versions')
    selection=pair.get('selection',{});request=pair.get('request',{})
    instruments={r['member']:{i['outcome_id'].lower():i for i in r['instruments']} for r in request.get('markets',[])}
    if set(instruments)!=set(members) or not instruments[members[0]] or set(instruments[members[0]])!=set(instruments[members[1]]):raise ValueError('explicit_Pair_outcome_bindings_required')
    bindings=[dict(label=label,legs=[dict(member=m,**deepcopy(instruments[m][label])) for m in sorted(members)]) for label in sorted(instruments[members[0]])]
    row=dict(schema='shared-pair-relation/2',pair_id=pair['group_id'],members=sorted(members),
        actual_started_at_ns=actual_started_at_ns,start_date_timezone='Asia/Shanghai',
        available_at_ns=available_at_ns,
        admitted_at_ns=pair.get('admitted_at_ns'),market_versions=deepcopy(market_versions),fee_versions=deepcopy(fee_versions),
        selection_receipt_hash=digest(selection),selection_reason=selection.get('reason'),selection_policy=selection.get('source_policy'),
        recording_request_hash=request.get('contract_hash'),
        outcome_bindings=bindings,
        atomic_cross_venue_snapshot_certified=False,payoff_equivalence_certified=False)
    if task5:row['task_type']='polymarket_myriad_pair'
    row['relation_version_id']=digest(row)
    return row
