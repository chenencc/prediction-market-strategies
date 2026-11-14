"""Exact lightweight financial projection, without raw order/object copies."""
import re
from marketdata.core import iso_to_ns
from marketdata.trade_quality import poly_execution_ns
from native_archive.fees import number

FINANCIAL_KEYS={
 'minimumOrderSize','nativePriceScale','nativeAmountDecimals','chainId','chain_id','networkId',
 'resolutionIndexSet','minimumTick','pricePrecision','amountDecimals','positionIds','questionID',
 'questionIndex','negRiskMarketID','negRiskRequestID','negRiskOther','fallbackResolutionType',
 'umaResolutionStatus','umaResolutionStatuses','resolvedBy','resolvedOutcomeId','voided',
 'winning_asset_id','winning_outcome','payout','payouts','payoutNumerators','payoutDenominator',
 'executionMode','tradingModel','secondsDelay','enableOrderBook','acceptingOrdersTimestamp',
 'endDateIso','startDateIso','gameStartTime','eventStartTime','expiresAt','resolvesAt',
 'openPrice','closePrice','strikePrice','priceToBeat','openingPrice','closingPrice',
 'sportsMarketType','variantData','variantDetails','groupItemThreshold','marketOutcomes',
 'token','oracle','state'}

def merge_patch(previous,patch):
 """Omitted keys survive. Explicit null survives as unknown/removal evidence.

Outcome arrays are keyed by a native stable identity; ambiguous arrays replace
in full. Other arrays are explicit replacements, never a guessed union.
 """
 result=dict(previous)
 for k,v in patch.items():
  old=result.get(k)
  if isinstance(v,dict) and isinstance(old,dict):result[k]=merge_patch(old,v)
  elif k in ('outcomes','marketOutcomes') and isinstance(v,list) and isinstance(old,list) and v and old:
   candidates=('onChainId','tokenId','assetId','id','indexSet')
   identity=next((name for name in candidates if all(isinstance(x,dict) and x.get(name) is not None for x in old+v)),None)
   if identity and len({str(x[identity]) for x in old})==len(old) and len({str(x[identity]) for x in v})==len(v):
    updates={str(x[identity]):x for x in v};result[k]=[merge_patch(x,updates.pop(str(x[identity]),{})) for x in old]+list(updates.values())
   else:result[k]=v
  else:result[k]=v
 return result

def economics(venue,raw,mid):
 if venue=='predictfun':
  if str(raw.get('market',{}).get('id'))!=str(mid):raise ValueError('native_settlement_market_mismatch')
  executed=iso_to_ns(raw['executedAt']) if raw.get('executedAt') else None
  fraction=re.search(r'\.([0-9]+)(?:Z|[+-][0-9]{2}:[0-9]{2})$',raw.get('executedAt',''))
  precision=10**(9-len(fraction[1])) if fraction else 10**9 if executed is not None else None
  taker=raw.get('taker') or {};outcome=taker.get('outcome') or {};index=outcome.get('indexSet');asset=outcome.get('onChainId')
  price,size=raw.get('priceExecuted'),raw.get('amountFilled')
  for value in (price,size):
   if value is not None and (not isinstance(value,str) or not re.fullmatch('[0-9]+',value)):raise ValueError('native_execution_integer_string_required')
  side=taker.get('quoteType');price_unit=size_unit='native_wei'
 else:
  stamp=raw.get('timestamp')
  if stamp is not None:
   from decimal import Decimal
   if isinstance(stamp,(bool,float)) or Decimal(str(stamp))!=Decimal(str(stamp)).to_integral_value():raise ValueError('native_trade_integer_seconds_required')
  executed=poly_execution_ns(raw);precision=10**9 if executed is not None else None
  index=raw.get('outcome_index');asset=raw.get('token_id');side=raw.get('side')
  price,size=number(raw.get('price')),number(raw.get('size'));price_unit,size_unit='probability','shares'
 if index is not None and (type(index) is not int or not 0<=index<2**31):raise ValueError('native_outcome_index_required')
 return dict(execution_ns=executed,source_precision_ns=precision,asset_id=str(asset) if asset is not None else None,index_set=index,
  price_native=price,size_native=size,price_unit=price_unit,size_unit=size_unit,side=side,
  settlement_id=raw.get('settlementId') if venue=='predictfun' else None,transaction_hash=raw.get('transactionHash') if venue=='predictfun' else raw.get('transaction_hash'))

def fees(venue,raw):
 if venue=='polymarket':
  rate=number(raw.get('fee_rate_bps'));return [['taker_rate',None,rate,None,None,'rate_reported_amount_unknown' if rate is not None else 'not_reported']]
 result=[]
 for role,legs in (('taker',[raw.get('taker')]),('maker',raw.get('makers') or [])):
  for index,leg in enumerate(legs):
   fee=(leg or {}).get('fee');fee=fee if isinstance(fee,dict) else {};amount=fee.get('amount')
   if amount is not None and (isinstance(amount,bool) or not re.fullmatch('-?[0-9]+',str(amount))):raise ValueError('native_fee_integer_required')
   result.append([role,index,None,str(amount) if amount is not None else None,fee.get('type'),'amount_reported' if amount is not None else 'not_reported'])
 return result

def occurrence_evidence(venue,raw):
 # Local hash input ONLY. Fee revisions and display changes do not alter identity.
 keys=('fill_id','fillId','trade_id','tradeId','log_index','logIndex','transaction_index','transactionIndex',
       'proxy_wallet','proxyWallet','wallet','walletAddress','order_id','orderId','orderHash','maker','taker')
 if venue=='polymarket':return {k:raw[k] for k in keys if k in raw}
 def leg(value):
  return {k:v for k,v in (value or {}).items() if k not in ('fee','outcome','name','status','bestBid','bestAsk')}
 return [leg(raw.get('taker')),[leg(x) for x in raw.get('makers') or []]]
