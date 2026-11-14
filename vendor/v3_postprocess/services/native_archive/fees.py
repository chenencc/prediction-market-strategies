"""Keep reported policy, observed fee rate and actually reported charge separate.

No fee formula is guessed; native SHARES are never silently treated as cash.
Polymarket's legacy base-fee numbers are not interpreted as basis points.
"""
import hashlib,json,re
from decimal import Decimal
from .layout import venue,timestamp

def exact_value(value):
    if isinstance(value,float):raise ValueError('binary_float_not_allowed_in_fee_evidence')
    if isinstance(value,Decimal):
        if not value.is_finite():raise ValueError('nonfinite_fee_value')
        return str(value)
    if isinstance(value,dict):return {k:exact_value(v) for k,v in value.items()}
    if isinstance(value,list):return [exact_value(v) for v in value]
    return value

def canonical(value):return json.dumps(exact_value(value),sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()

def number(value):
    if value is None:return None
    if isinstance(value,bool) or not isinstance(value,(int,str,Decimal)):raise ValueError('exact_fee_number_required')
    n=Decimal(value)
    if not n.is_finite():raise ValueError('nonfinite_fee_value')
    return str(value)

def fee_version(platform,market,raw,*,observed_at_ns,source_ref,source_effective_at_ns=None):
    from .objects import reference
    venue(platform);timestamp(observed_at_ns)
    if source_effective_at_ns is not None:timestamp(source_effective_at_ns)
    policy={k:exact_value(v) for k,v in raw.items() if 'fee' in k.lower()};objects={}
    schedule=raw.get('feeSchedule') or {}
    if not isinstance(schedule,dict):raise ValueError('fee_schedule_object_required')
    known={'feesEnabled','feeType','makerFeeBps','feeRateBps','makerBaseFee','takerBaseFee','feeSchedule'}
    extra={k:v for k,v in policy.items() if k not in known}
    extra_schedule={k:v for k,v in schedule.items() if k not in {'rate','exponent','takerOnly','rebateRate'}}
    if extra_schedule:extra['feeSchedule']=extra_schedule
    row=dict(schema='market-fee-version/2',venue=platform,market_id=str(market),
        fees_enabled=raw.get('feesEnabled') if isinstance(raw.get('feesEnabled'),bool) else None,
        fee_type=raw.get('feeType'),maker_fee_bps=number(raw.get('makerFeeBps')) if platform=='predictfun' else None,
        taker_fee_bps=number(raw.get('feeRateBps')) if platform=='predictfun' else None,
        legacy_maker_base_fee=number(raw.get('makerBaseFee')),legacy_taker_base_fee=number(raw.get('takerBaseFee')),
        schedule_rate=number(schedule.get('rate')),schedule_exponent=number(schedule.get('exponent')),
        schedule_taker_only=schedule.get('takerOnly'),schedule_rebate_rate=number(schedule.get('rebateRate')),
        extra_policy_ref=reference(extra,objects,available_at_ns=observed_at_ns),configuration_status='reported' if policy else 'unknown',
        actual_paid_fee_certified=False)
    # A policy is shared across markets; the observation timeline binds a market
    # to it. Repeating a fee configuration must not copy its entire payload.
    row['fee_version_id']=digest({k:v for k,v in row.items() if k!='market_id'})
    row.update(available_at_ns=observed_at_ns,source_effective_at_ns=source_effective_at_ns,
        source_ref=source_ref,effective_time_basis='source_reported' if source_effective_at_ns is not None else 'first_observation_only')
    row['_objects']=list(objects.values())
    return row

def reported_trade_fees(platform,market,trade,*,observation_id,available_at_ns,source_ref,market_metadata=None):
    from .objects import reference
    venue(platform);timestamp(available_at_ns)
    common=dict(schema='reported-trade-fee/2',venue=platform,market_id=str(market),observation_id=observation_id,
        available_at_ns=available_at_ns,source_ref=source_ref,amount_decimals=None,
        amount_converted_to_cash=None,amount_is_estimate=False,unique_fill_certified=False)
    if platform=='myriad':
        from myriad.trades import fees
        return fees(trade,market_metadata,common)
    if platform=='polymarket':
        rate=number(trade.get('fee_rate_bps'))
        row=common|dict(leg='taker_rate',leg_index=None,execution_reference=trade.get('transaction_hash',trade.get('transactionHash')),
            fee_rate_bps=rate,fee_amount_native=None,fee_unit=None,extra_fee_ref=None,
            fee_status='rate_reported_amount_unknown' if rate is not None else 'not_reported')
        row['fee_record_id']=digest(row);return [row]
    rows=[]
    for role,legs in (('taker',[trade.get('taker')]),('maker',trade.get('makers') or [])):
        for index,leg in enumerate(legs):
            leg=leg or {};fee=leg.get('fee');fee=fee if isinstance(fee,dict) else None
            amount=fee.get('amount') if fee else None
            if amount is not None and (isinstance(amount,bool) or not re.fullmatch('-?[0-9]+',str(amount))):raise ValueError('native_fee_integer_required')
            objects={};extra={k:v for k,v in (fee or {}).items() if k not in {'amount','type'}}
            rows.append(common|dict(leg=role,leg_index=index,execution_reference=trade.get('settlementId'),
                fee_rate_bps=None,fee_amount_native=str(amount) if amount is not None else None,
                fee_unit=fee.get('type') if fee else None,extra_fee_ref=reference(extra,objects,available_at_ns=available_at_ns),
                fee_status='amount_reported' if amount is not None else 'not_reported',_objects=list(objects.values())))
    for row in rows:row['fee_record_id']=digest({k:v for k,v in row.items() if k!='_objects'})
    return rows
