"""Compact API observations: shared metadata/fees, no full response-page copies."""
from copy import deepcopy
import re

from marketdata.core import exact,iso_to_ns
from marketdata.trade_quality import poly_execution_ns
from marketdata.poly_trade_policy import window as poly_window
from .fees import digest,number,exact_value,reported_trade_fees
from .objects import reference
from .layout import timestamp,venue


def api_observation(platform,raw,*,market_id,available_at_ns,source_ref,page_sha256,row_index,
                    market_version_id,window_start_ns,window_end_ns,market_metadata=None):
    venue(platform);timestamp(available_at_ns)
    trade=exact(raw) if isinstance(raw,(str,bytes)) else deepcopy(raw)
    objects={};observation_id=digest([platform,str(market_id),source_ref,available_at_ns,row_index])
    if platform=='myriad':
        from myriad.trades import fields
        f=fields(trade,market_metadata)
        if f['market_id']!=str(market_id):raise ValueError('native_trade_market_mismatch')
        executed=f['execution_ns'];precision=f['source_precision_ns'];index=None;asset=f['asset_id'];side=f['side']
        price=f['price_native'];size=f['size_native'];price_unit=f['price_unit'];size_unit=f['size_unit']
        settlement=None;transaction=f['transaction_hash'];taker_ref=makers_ref=None
        membership='inside' if window_start_ns<=executed<window_end_ns else 'outside'
        known=set(trade) # Exact financial fields are projected, no wallet/profile copies.
    elif platform=='predictfun':
        if str(trade.get('market',{}).get('id'))!=str(market_id):raise ValueError('native_settlement_market_mismatch')
        executed=iso_to_ns(trade['executedAt']) if trade.get('executedAt') else None
        fraction=re.search(r'\.([0-9]+)(?:Z|[+-][0-9]{2}:[0-9]{2})$',trade.get('executedAt',''))
        precision=10**(9-len(fraction[1])) if fraction else 10**9 if executed is not None else None
        taker=trade.get('taker') or {};outcome=taker.get('outcome') or {}
        index=outcome.get('indexSet');asset=outcome.get('onChainId');side=taker.get('quoteType')
        price=trade.get('priceExecuted');size=trade.get('amountFilled')
        for value in (price,size):
            if value is not None and (not isinstance(value,str) or not re.fullmatch('[0-9]+',value)):
                raise ValueError('native_execution_integer_string_required')
        def leg(raw_leg):
            result={k:v for k,v in raw_leg.items() if k not in {'fee','outcome'}}
            o=raw_leg.get('outcome') or {}
            result['outcome']={k:v for k,v in o.items() if k not in {'name','status','bestBid','bestAsk'}}
            return result
        taker_ref=reference(leg(taker),objects,available_at_ns=available_at_ns)
        makers_ref=reference([leg(m) for m in trade.get('makers') or []],objects,available_at_ns=available_at_ns)
        known={'market','taker','makers','priceExecuted','amountFilled','executedAt','settlementId','transactionHash'}
        price_unit=size_unit='native_wei'
        settlement=trade.get('settlementId');transaction=trade.get('transactionHash')
        membership='inside' if executed is not None and window_start_ns<=executed<window_end_ns else 'outside' if executed is not None else 'unknown'
    else:
        executed=poly_execution_ns(trade);precision=10**9 if executed is not None else None
        index=trade.get('outcome_index');asset=trade.get('token_id');side=trade.get('side')
        price=number(trade.get('price'));size=number(trade.get('size'));price_unit='probability';size_unit='shares'
        settlement=None;transaction=trade.get('transaction_hash');taker_ref=makers_ref=None
        # Market presentation comes from shared metadata. User profile display
        # fields are not execution evidence; retain native proxy wallet separately.
        known={'price','size','timestamp','outcome_index','token_id','side','transaction_hash','condition_id',
               'title','slug','icon','event_slug','outcome','name','pseudonym','bio','profile_image','profile_image_optimized'}
        membership=poly_window(executed,window_start_ns,window_end_ns)[1] if executed is not None and poly_window(executed,window_start_ns,window_end_ns)[0] else 'outside' if executed is not None else 'unknown'
    if index is not None and (type(index) is not int or not 0<=index<2**31):raise ValueError('native_outcome_index_required')
    extra={k:v for k,v in trade.items() if k not in known}
    args=dict(market_metadata=market_metadata) if platform=='myriad' else {}
    fees=reported_trade_fees(platform,market_id,trade,observation_id=observation_id,available_at_ns=available_at_ns,source_ref=source_ref,**args)
    row=dict(schema='api-trade-observation/1',venue=platform,market_id=str(market_id),observation_id=observation_id,
        available_at_ns=available_at_ns,execution_ns=executed,source_precision_ns=precision,source_ref=source_ref,
        source_page_sha256=page_sha256,source_row_index=row_index,source_market_version_id=market_version_id,
        settlement_id=settlement,transaction_hash=transaction,asset_id=str(asset) if asset is not None else None,index_set=index,
        price_native=price,size_native=size,price_unit=price_unit,size_unit=size_unit,side=side,window_membership=membership,
        taker_ref=taker_ref,makers_ref=makers_ref,extra_trade_ref=reference(extra,objects,available_at_ns=available_at_ns),
        unique_fill_certified=False,_objects=list(objects.values()))
    return row,fees


def quality_observation(platform,market_id,detail,*,kind,available_at_ns,source_ref):
    venue(platform);timestamp(available_at_ns);objects={}
    row=dict(schema='archive-quality-observation/1',venue=platform,market_id=str(market_id),
        available_at_ns=available_at_ns,source_ref=source_ref,quality_kind=kind,
        detail_ref=reference(detail,objects,available_at_ns=available_at_ns))
    row['observation_id']=digest(row);row['_objects']=list(objects.values())
    return row
