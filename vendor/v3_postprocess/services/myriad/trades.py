"""Exact Myriad REST trade fields for the shared archive, never WS quotes.

The official integer base-unit example differs from observed decimal-display
responses. Keep an explicit encoding; never scale a decimal response as wei or
invent an indexSet, taker role, exchange fill ID or chain fee confirmation.
"""
from decimal import Decimal,localcontext
import re

from marketdata.l2 import scaled_integer
from native_archive.fees import number,digest
from native_archive.objects import reference
from . import protocol


def fields(raw,metadata):
    if metadata is None:raise ValueError('myriad_trade_metadata_required')
    mid=protocol.identity(metadata);outcome=raw.get('outcome')
    allowed={'price','priceAfterFees','amount','side','outcome','txHash','timestamp','fees','user','wallet','address'}
    if set(raw)-allowed:raise ValueError('myriad_unprojected_trade_fields_require_review')
    if type(outcome) is not int or outcome not in (0,1):raise ValueError('myriad_native_trade_outcome_required')
    side=raw.get('side')
    if side not in ('buy','sell','split','merge'):raise ValueError('myriad_native_trade_action_unknown')
    executed=protocol.trade_time_ns(raw)
    if executed is None:raise ValueError('myriad_native_trade_clock_required')
    price=number(raw.get('price'));amount=number(raw.get('amount'));after=number(raw.get('priceAfterFees'))
    if price is None or amount is None or not 0<=Decimal(price)<=1 or Decimal(amount)<0:
        raise ValueError('myriad_REST_probability_and_nonnegative_amount_required')
    if after is not None and Decimal(after)<0:raise ValueError('myriad_effective_price_invalid')
    # An explicit decimal point cannot be a documented uint base-unit string.
    hint=metadata.get('__trade_amount_encoding')
    if hint=='decimal_display' and isinstance(raw['amount'],str) and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',raw['amount']):encoding=hint
    elif hint not in (None,'documented_uint_base_units'):raise ValueError('myriad_trade_amount_encoding_unknown')
    elif isinstance(raw['amount'],str) and re.fullmatch(r'[0-9]+\.[0-9]+',raw['amount']):encoding='decimal_display'
    elif isinstance(raw['amount'],str) and re.fullmatch('[0-9]+',raw['amount']):encoding='documented_uint_base_units'
    else:raise ValueError('myriad_trade_amount_encoding_unknown')
    asset=next(str(o['tokenId']) for o in metadata['outcomes'] if o['id']==outcome)
    clock=str(raw['timestamp']);fraction=re.search(r'\.([0-9]+)(?:Z|[+-][0-9]{2}:[0-9]{2})$',clock)
    precision=10**(9-len(fraction[1])) if fraction else 10**9
    return dict(market_id=mid,execution_ns=executed,source_precision_ns=precision,asset_id=asset,
        index_set=None,price_native=price,size_native=amount,price_unit='probability',
        size_unit='shares' if encoding=='decimal_display' else 'native_wei',side=side,
        settlement_id=None,transaction_hash=raw.get('txHash'),native_outcome_id=outcome,
        price_after_fees=after,amount_encoding=encoding,
        action_class='execution_record_not_unique_fill' if side in ('buy','sell') else 'supply_action_not_TradeTick')


def fees(raw,metadata,common):
    f=fields(raw,metadata);native=raw.get('fees')
    if native is not None and not isinstance(native,dict):raise ValueError('myriad_fee_components_object_required')
    native=native or {};parts={}
    for key,value in native.items():
        amount=number(value)
        if amount is not None and Decimal(amount)<0:raise ValueError('myriad_fee_negative')
        if f['amount_encoding']=='documented_uint_base_units' and amount is not None and not re.fullmatch('[0-9]+',str(value)):
            raise ValueError('myriad_base_unit_fee_requires_uint')
        parts[key]=amount
    total=parts.get('total');converted=total
    decimals=metadata['token']['decimals']
    if total is not None and f['amount_encoding']=='documented_uint_base_units':converted=scaled_integer(raw['fees']['total'],decimals)
    component_match=None
    if total is not None and all(parts.get(k) is not None for k in ('lp','treasury','distributor')):
        with localcontext() as context:
            context.prec=160
            component_match=Decimal(total)==sum(Decimal(parts[k]) for k in ('lp','treasury','distributor'))
    objects={};extra=dict(components_native=parts,price_after_fees=f['price_after_fees'],
        native_outcome_id=f['native_outcome_id'],amount_encoding=f['amount_encoding'],
        action_class=f['action_class'],components_match_reported_total=component_match,
        chain_payment_verified=False)
    row=common|dict(leg='reported_total',leg_index=None,execution_reference=raw.get('txHash'),
        fee_rate_bps=None,fee_amount_native=total,
        fee_unit='collateral_token' if f['amount_encoding']=='decimal_display' else 'collateral_base_units',
        amount_decimals=None if f['amount_encoding']=='decimal_display' else decimals,
        amount_converted_to_cash=converted,extra_fee_ref=reference(extra,objects,available_at_ns=common['available_at_ns']),
        fee_status='API_amount_reported_not_chain_verified' if total is not None else 'not_reported',_objects=list(objects.values()))
    row['fee_record_id']=digest({k:v for k,v in row.items() if k!='_objects'});return [row]
