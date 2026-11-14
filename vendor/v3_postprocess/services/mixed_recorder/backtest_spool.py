"""Offline data-only archive: exact L2, executions, fees and time-qualified mappings.

No database, network, recorder or HF imports. Native originals are never changed.
Only ZIP + chunk.json are publication payloads; checkpoint/control files are local
restart dependencies. This profile is opt-in and not wired to the live publisher.
"""
import base64
from collections import Counter
from copy import deepcopy
from decimal import Decimal,localcontext
import hashlib,io,json,os,re,zipfile
from pathlib import Path
import zstandard
from marketdata.poly_trade_policy import window as poly_window
from .backtest_fields import FINANCIAL_KEYS,merge_patch,economics,fees,occurrence_evidence
from .backtest_index import TradeIndex
from native_archive.fees import canonical
from pair_recorder.supervisor import OwnerLock
from .store import durable_file,encoded,sha256_file,sync_directory

PROFILE='market-backtest/2'
ENCODING=PROFILE+'-delta-jsonl-zstd6'
MAX_LINE=64*1024**2
MAX_TRADES=2000000
CODE_SHA256=hashlib.sha256(b''.join((Path(__file__).parent/name).read_text(encoding='utf-8-sig').replace('\r\n','\n').encode() for name in ('backtest_spool.py','backtest_fields.py','backtest_index.py'))).hexdigest()
VENUES=('polymarket','predictfun')
COLUMNS={
 'm':['market','version','sequence','received_ns'],
 's':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','source_version','bids','asks'],
 'd':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','source_version','bids','asks','valid'],
 'u':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','source_version','bids','asks'],
 'h':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','source_version'],
 'q':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','bid','ask'],
 'l':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','price','size','side','fee_rate_bps'],
 'k':['instrument','sequence','position','received_ns','source_ns','source_precision_ns','epoch','old_tick','new_tick'],
 'r':['instruments','sequence','received_ns','epoch'],
 'g':['market','received_ns','ready','epoch'],
 't':['id','identity','market','instrument','sequence','position','available_at_ns','execution_ns','precision_ns','price','size','price_unit','size_unit','side','rank','identified','metadata_version','fees'],
 'f':['id','sequence','position','available_at_ns','fees'],
 'a':['market','instrument','sequence','position','available_at_ns','execution_ns','precision_ns','price','size','price_unit','size_unit','side'],
 'n':['market','sequence','received_ns','source_ns','epoch','settlement'],
 'e':['market','sequence','received_ns','source_ns','resolution'],
}
FEE_COLUMNS=('leg','leg_index','fee_rate_bps','fee_amount_native','fee_unit','fee_status')
META_KEYS=FINANCIAL_KEYS|{'id','conditionId','clobTokenIds','outcomes','decimalPrecision','orderPriceMinTickSize','orderMinSize',
 'minimum_tick_size','minimum_order_size','priceIncrement','sizeIncrement','minOrderSize','tickSize','sizePrecision','marketType','marketVariant',
 'isNegRisk','negRisk','isYieldBearing','status','tradingStatus','active','closed','acceptingOrders','resolution',
 'endDate','startDate','resolutionSource','oracleQuestionId','resolverAddress','winningOutcome','winningAssetId'}
OUTCOME_KEYS={'id','indexSet','onChainId','tokenId','assetId','name','status','closingPrice'}
IGNORED={'trade_quality','trade_coverage','trade_control','subscription_sent','ws_control','group_control','request_gap'}


def require(ok,code):
 if not ok:raise ValueError('backtest_'+code)
def dumps(value):return canonical(value)
def data(value):return (dumps(value)+'\n').encode()
def key(value):return base64.urlsafe_b64encode(hashlib.sha256(data(value)).digest()).decode().rstrip('=')
def decimal(value):
 require(not isinstance(value,(float,bool)) and isinstance(value,(str,int,Decimal)),'exact_decimal_required')
 with localcontext() as ctx:
  ctx.prec=160;n=Decimal(value);require(n.is_finite(),'finite_decimal_required')
  require(len(n.as_tuple().digits)+abs(n.as_tuple().exponent)<=1024,'decimal_expansion_bound')
  if not n:return '0'
  text=format(n,'f');return text.rstrip('0').rstrip('.') if '.' in text else text
def levels(items):
 out={};seen=set()
 for x in items:
  p,q=(x['price'],x['size']) if isinstance(x,dict) else x[:2];p,q=decimal(p),decimal(q)
  require(0<=Decimal(p)<=1 and Decimal(q)>=0 and p not in seen,'invalid_or_duplicate_level');seen.add(p)
  if Decimal(q):out[p]=q
 return out
def pairs(book,side):return [[p,q] for p,q in sorted(book[side].items(),key=lambda x:Decimal(x[0]),reverse=side=='bids')]
def ms(value):
 if value is None:return None,None
 with localcontext() as ctx:
  ctx.prec=160;n=Decimal(str(value))*10**6;require(n==n.to_integral_value(),'source_clock_precision')
 digits=max(0,-Decimal(str(value)).as_tuple().exponent)
 return int(n),10**max(0,6-digits)

def slim_metadata(raw):
 result={k:v for k,v in raw.items() if k in META_KEYS or 'fee' in k.lower() or any(x in k.lower() for x in ('collateral','currency','decimals'))}
 for field in ('outcomes','marketOutcomes'):
  if isinstance(result.get(field),list):
   result[field]=[{k:v for k,v in o.items() if k in OUTCOME_KEYS} if isinstance(o,dict) else o for o in result[field]]
 return json.loads(dumps(result))

def fresh(collection,first):
 return dict(schema='market-backtest-checkpoint/2',collection=collection,through=first-1,last_received_ns=None,
  markets={},instruments={},assets={},conditions={},epochs={},versions={},version_keys={},current_versions={},pairs={},windows={},
  books={},trades={},trade_index={},retired={},lanes={},next_market=1,next_instrument=1,next_version=1,next_pair=1,next_epoch=1,anchors={},settlements={},gap_seen={},next_trade=1,last_chunk=None,external_prefix=first!=1)


class Projector:
 def __init__(self,state,trade_index=None):
  self.state=state;self.trade_index=trade_index;self.fee_dependencies={};self.records=[];self.stats=Counter()
  self.start_sequence=state['through'];self.start_books=deepcopy({k:v for k,v in state['books'].items() if v['valid']});self.start_meta=dict(state['current_versions'])
  self.market_by_id={r[0]:r for r in state['markets'].values()};self.instrument_by_id={r[0]:r for r in state['instruments'].values()}
 def emit(self,tag,*args):
  require(tag in COLUMNS and len(args)==len(COLUMNS[tag]),'record_columns');self.records.append([tag,*args]);self.stats[tag]+=1
 def market(self,venue,mid):
  require(venue in VENUES and mid is not None,'market_identity');ident=venue+':'+str(mid);markets=self.state['markets']
  require(ident not in self.state['retired'],'retired_market_reappeared')
  if ident not in markets:
   markets[ident]=[self.state['next_market'],venue,str(mid)];self.state['next_market']+=1;self.market_by_id[markets[ident][0]]=markets[ident]
  return markets[ident][0]
 def instrument(self,venue,mid,asset,index=None,origin='native'):
  m=self.market(venue,mid);ident=venue+':'+str(asset);require(asset is not None,'asset_identity')
  if ident not in self.state['instruments']:
   row=[self.state['next_instrument'],m,str(asset),index,origin];self.state['next_instrument']+=1;self.state['instruments'][ident]=row;self.state['assets'][ident]=[str(mid),row[0]];self.instrument_by_id[row[0]]=row
  row=self.state['instruments'][ident];require(row[1]==m,'asset_market_changed')
  if index is not None:
   require(row[3] is None or row[3]==index,'asset_outcome_index_changed')
   if row[3] is None:row[3]=index
  return row[0]
 def epoch(self,value):
  if value is None:return 0
  if str(value) not in self.state['epochs']:self.state['epochs'][str(value)]=self.state['next_epoch'];self.state['next_epoch']+=1
  return self.state['epochs'][str(value)]
 def context(self,catalog):
  # Only identity and time-qualified Pair windows come from catalog. Never seed
  # current catalog fee/status values into earlier events (no look-ahead).
  for member,entry in catalog.get('metadata',{}).items():
   venue,mid=member.split(':',1)
   if venue not in VENUES or member in self.state['retired']:continue
   body=catalog['bodies'][entry['body_sha256']];body=body.get('data',body);self.identities(venue,mid,body)
  for sid,session in catalog.get('sessions',{}).items():
   start=session.get('actual_started_at_ns')
   if start is None:continue
   members=session['offer']['candidate']['members'];mids=[];labels={}
   if any(member in self.state['retired'] for member in members):continue
   for member in members:
    v,mid=member.split(':',1);mids.append(self.market(v,mid));windows=self.state['windows'].setdefault(member,{})
    windows[sid]=[start,session.get('closed_at_ns')]
   for market in session['offer'].get('markets',[]):
    v,mid=market['member'].split(':',1)
    for inst in market.get('instruments',[]):
     i=self.instrument(v,mid,inst['asset_id'],inst.get('index_set',inst.get('indexSet')),inst.get('book_origin','native'))
     labels.setdefault(str(inst['outcome_id']).lower(),[]).append(i)
   require(len(set(members))==len(members) and labels and all(len(ids)==len(members) and len(set(ids))==len(ids) and {self.instrument_by_id[i][1] for i in ids}==set(mids) for ids in labels.values()),'pair_outcome_mapping_required')
   pair=[self.state['next_pair'],mids,[[k,labels[k]] for k in sorted(labels)],start,session.get('closed_at_ns')]
   if sid in self.state['pairs']:pair[0]=self.state['pairs'][sid][0]
   else:self.state['next_pair']+=1
   self.state['pairs'][sid]=pair
 def identities(self,venue,mid,body):
  require(str(body.get('id'))==str(mid),'metadata_market_identity');self.market(venue,mid)
  if venue=='polymarket':
   assets=body.get('clobTokenIds',[]);assets=json.loads(assets) if isinstance(assets,str) else assets
   for asset in assets or []:self.instrument(venue,mid,str(asset))
   if body.get('conditionId') is not None:self.state['conditions'][str(body['conditionId'])]=str(mid)
  else:
   for o in body.get('outcomes') or []:
    if o.get('onChainId') is not None:self.instrument(venue,mid,str(o['onChainId']),o.get('indexSet'),'derived_complement' if o.get('indexSet')==2 else 'native')
 def metadata(self,event,body,partial=False):
  body=body.get('data',body) if isinstance(body,dict) else body;require(isinstance(body,dict),'metadata_shape')
  v=event['venue'];mid=str(event['native_market_id']);self.identities(v,mid,body);m=self.market(v,mid);native=slim_metadata(body);s=self.state
  if partial and str(m) in s['current_versions']:
   previous=s['versions'][str(s['current_versions'][str(m)])][3];native=merge_patch(previous,native)
  h=key([m,native])
  if h not in s['version_keys']:
   ident=s['next_version'];s['next_version']+=1;s['version_keys'][h]=ident;s['versions'][str(ident)]=[ident,m,event['timestamp_received_ns'],native]
  ident=s['version_keys'][h]
  if s['current_versions'].get(str(m))!=ident:
   s['current_versions'][str(m)]=ident;self.emit('m',m,ident,event['collector_sequence'],event['timestamp_received_ns'])
 def reset(self,venue,epoch,seq,at,market=None):
  indices=[];scope=None if market is None else {str(x) for x in market} if isinstance(market,(list,tuple,set)) else {str(market)}
  for k,b in self.state['books'].items():
   inst=self.instrument_by_id.get(int(k))
   market_row=self.market_by_id.get(inst[1]) if inst else None
   if market_row and market_row[1]==venue and (scope is None or market_row[2] in scope) and (not epoch or b['epoch']==epoch):
    b['valid']=False;indices.append(int(k))
  if indices:self.emit('r',indices,seq,at,epoch)
 def gap(self,row,seq):
  member=row['member'];v,mid=member.split(':',1);at=row['at_ns'];identity=key([member,at,row.get('epoch'),row.get('previous_epoch')])
  if identity in self.state['gap_seen']:return
  self.state['gap_seen'][identity]=at;epoch=self.epoch(row.get('epoch'));ready=row.get('epoch') is not None
  self.emit('g',self.market(v,mid),at,ready,epoch)
  if not ready:self.reset(v,0,seq,at,mid)
 def book(self,i,event,pos,stamp,precision,version,bids,asks,snapshot):
  seq=event['collector_sequence'];at=event['timestamp_received_ns'];epoch=self.epoch(event.get('connection_epoch'));s=self.state
  old=s['books'].get(str(i))
  version_regressed=False
  if old and version is not None and old.get('version') is not None:
   try:version_regressed=Decimal(str(version))<Decimal(str(old['version']))
   except Exception:version_regressed=False
  if snapshot:
   new_sides={side:levels(values) for side,values in (('bids',bids),('asks',asks))}
   if old and old['valid'] and old['epoch']==epoch and not version_regressed:
    delta={side:[[p,new_sides[side].get(p,'0')] for p in sorted(old[side].keys()|new_sides[side].keys(),key=Decimal,reverse=side=='bids') if old[side].get(p)!=new_sides[side].get(p)] for side in ('bids','asks')}
    if delta['bids'] or delta['asks']:self.emit('u',i,seq,pos,at,stamp,precision,epoch,version,delta['bids'],delta['asks'])
    else:self.emit('h',i,seq,pos,at,stamp,precision,epoch,version)
   else:self.emit('s',i,seq,pos,at,stamp,precision,epoch,version,pairs(new_sides,'bids'),pairs(new_sides,'asks'))
   book=dict(new_sides,valid=True)
  else:
   # Delta sizes are absolute. Explicit zero deletes a level; do not run them
   # through snapshot filtering, which intentionally omits zero levels.
   changes={side:[[decimal(x['price'] if isinstance(x,dict) else x[0]),decimal(x['size'] if isinstance(x,dict) else x[1])] for x in values] for side,values in (('bids',bids),('asks',asks))}
   valid=bool(old and old['valid'] and old['epoch']==epoch)
   self.emit('d',i,seq,pos,at,stamp,precision,epoch,version,changes['bids'],changes['asks'],valid)
   for side in ('bids','asks'):
    for p,q in changes[side]:require(0<=Decimal(p)<=1 and Decimal(q)>=0,'invalid_delta_level')
   book=old if valid else dict(bids={},asks={},valid=False)
   if valid:
    for side in ('bids','asks'):
     for p,q in changes[side]:
      if Decimal(q):book[side][p]=q
      else:book[side].pop(p,None)
  book.update(epoch=epoch,source_ns=stamp,precision=precision,received_ns=at,version=version,sequence=seq,position=pos);s['books'][str(i)]=book
 def poly(self,event,n,pos):
  kind=n.get('event_type');seq=event['collector_sequence'];at=event['timestamp_received_ns'];epoch=self.epoch(event.get('connection_epoch'));stamp,precision=ms(n.get('timestamp'))
  def instrument(asset):
   found=self.state['assets'].get('polymarket:'+str(asset))
   require(found is not None,'unknown_poly_asset');return found[1]
  if kind=='book':self.book(instrument(n['asset_id']),event,pos,stamp,precision,None,n['bids'],n['asks'],True)
  elif kind=='price_change':
   for j,c in enumerate(n.get('price_changes',[n])):
    require(c['side'] in ('BUY','SELL'),'book_side');i=instrument(c['asset_id']);self.book(i,event,[pos[0],j],stamp,precision,None,[c] if c['side']=='BUY' else [],[c] if c['side']=='SELL' else [],False)
    # Preserve official quote values only where the retained depth cannot supply
    # the same values. This is market data, not a later audit record.
    b=self.state['books'][str(i)]
    bid=max(b['bids'],key=Decimal) if b['valid'] and b['bids'] else None;ask=min(b['asks'],key=Decimal) if b['valid'] and b['asks'] else None
    nb=decimal(c['best_bid']) if c.get('best_bid') is not None else None;na=decimal(c['best_ask']) if c.get('best_ask') is not None else None
    if (nb is not None or na is not None) and (not b['valid'] or nb!=bid or na!=ask):self.emit('q',i,seq,[pos[0],j],at,stamp,precision,epoch,nb,na)
  elif kind=='best_bid_ask':self.emit('q',instrument(n['asset_id']),seq,pos,at,stamp,precision,epoch,decimal(n['best_bid']) if n.get('best_bid') is not None else None,decimal(n['best_ask']) if n.get('best_ask') is not None else None)
  elif kind=='last_trade_price':self.emit('l',instrument(n['asset_id']),seq,pos,at,stamp,precision,epoch,decimal(n['price']),decimal(n['size']) if n.get('size') is not None else None,n.get('side'),n.get('fee_rate_bps'))
  elif kind=='tick_size_change':self.emit('k',instrument(n['asset_id']),seq,pos,at,stamp,precision,epoch,decimal(n['old_tick_size']),decimal(n['new_tick_size']))
  elif kind in ('new_market','market_resolved'):
   mid=self.state['conditions'].get(str(n.get('market')))
   if mid is None:
    assets=n.get('assets_ids',[]);found=[self.state['assets'].get('polymarket:'+str(x)) for x in assets];mid=next((x[0] for x in found if x),None)
   if mid is not None and kind=='market_resolved':
    resolution={k:v for k,v in n.items() if k in ('winning_asset_id','winning_outcome','resolution','payout','status')};self.emit('e',self.market('polymarket',mid),seq,at,stamp,resolution)
   else:self.stats['untracked_market_announcement']+=1
  else:raise ValueError('backtest_unknown_poly_market_event:'+str(kind))
 def predict(self,event,raw):
  d=raw.get('data',{});mid=str(d.get('marketId',event['native_market_id']));require(mid==str(event['native_market_id']),'predict_book_market')
  row=next((r for r in self.state['instruments'].values() if r[1]==self.market('predictfun',mid) and r[3]==1),None);require(row is not None,'predict_native_indexSet1_required')
  stamp,precision=ms(d.get('updateTimestampMs',d.get('tsMs')));self.book(row[0],event,[0,0],stamp,precision,d.get('version'),d['bids'],d['asks'],True)
  settled=d.get('lastOrderSettled')
  if settled is not None:
   require(isinstance(settled,dict),'settlement_notification_shape');financial={k:v for k,v in settled.items() if k in ('kind','id','marketId','outcome','price','side','size','amount','amountFilled','executedAt','timestamp') or 'fee' in k.lower()}
   m=self.market('predictfun',mid);h=key(financial)
   if self.state['settlements'].get(str(m))!=h:
    self.state['settlements'][str(m)]=h;self.emit('n',m,event['collector_sequence'],event['timestamp_received_ns'],stamp,self.epoch(event.get('connection_epoch')),financial)
 def membership(self,member,executed,venue):
  if executed is None:return 'unknown'
  windows=list(self.state['windows'].get(member,{}).values());require(windows,'market_recording_window_required')
  for start,end in windows:
   if venue=='polymarket':
    inside,_=poly_window(executed,start,end or 2**63-1)
    if inside:return 'inside'
   elif start<=executed<(end or 2**63-1):return 'inside'
  return 'before' if executed<min(x[0] for x in windows) else 'outside'
 def trade(self,event,body):
  require(event.get('http_status')==200,'trade_HTTP_failure_requires_retry_not_archive')
  venue=event['venue'];mid=str(event['native_market_id']);rows=body.get('data')
  require(isinstance(rows,list),'trade_page_shape')
  if venue=='predictfun':require(body.get('success') is True,'predict_trade_success')
  ranks=Counter();last_market=None;m=self.market(venue,mid);member=venue+':'+mid;at=event['timestamp_received_ns'];seq=event['collector_sequence']
  for index,raw in enumerate(rows):
   # Embedded market fee/precision changes are observations at this page's
   # availability time too; do not lose them when discarding full trade wrappers.
   if venue=='predictfun' and isinstance(raw.get('market'),dict) and raw['market']!=last_market:
    self.metadata(event,raw['market'],partial=True);last_market=raw['market']
   if venue=='polymarket' and raw.get('condition_id') is not None:require(self.state['conditions'].get(str(raw['condition_id']))==mid,'trade_condition_identity')
   normalized=economics(venue,raw,mid)
   price,size=normalized['price_native'],normalized['size_native'];require(price is not None and size is not None and Decimal(price)>=0 and Decimal(size)>0,'trade_economic_values')
   asset=normalized['asset_id'];inst=self.instrument(venue,mid,asset,normalized['index_set']) if asset is not None else None
   membership=self.membership(member,normalized['execution_ns'],venue)
   core=[normalized[k] for k in ('execution_ns','source_precision_ns','price_native','size_native','price_unit','size_unit','side')]
   if membership=='before':
    ident=str(inst) if inst is not None else 'market:'+str(m);old=self.state['anchors'].get(ident);stamp=normalized['execution_ns']
    if old is None or stamp>old['execution_ns']:
     self.state['anchors'][ident]=dict(execution_ns=stamp);self.emit('a',m,inst,seq,[index],at,*core)
    self.stats['prewindow_observations_omitted']+=1;continue
   if membership=='outside':self.stats['postwindow_observations_omitted']+=1;continue
   native=normalized['settlement_id'] if venue=='predictfun' else normalized['transaction_hash'];identified=isinstance(native,(str,int)) and not isinstance(native,bool) and bool(str(native))
   # Native/economic/leg distinctions and same-page multiplicity survive.
   # Identical cross-page occurrences without fill IDs remain ambiguous.
   base=[venue,mid,str(native),asset,normalized['index_set'],*core,occurrence_evidence(venue,raw)];group=key(base);ranks[group]+=1;rank=ranks[group]
   fingerprint=key([base,rank]) if identified else key([base,seq,index])
   seen=self.trade_index.get(member,fingerprint) if self.trade_index is not None else self.state['trades'].get(fingerprint)
   fee_rows=fees(venue,raw);financial=key(fee_rows)
   if seen is None:
    require(len(self.trade_index if self.trade_index is not None else self.state['trades'])<MAX_TRADES,'trade_state_capacity')
    ident=self.state['next_trade'];self.state['next_trade']+=1
    self.emit('t',ident,None,m,inst,seq,[index],at,*core,rank,identified,self.state['current_versions'].get(str(m),0),fee_rows)
    version=self.state['current_versions'].get(str(m),0)
    seen=dict(id=ident,fees=financial,fee_values=fee_rows,fee_available_at_ns=at,definition=self.records[-1],metadata=self.state['versions'].get(str(version)),member=member)
    self.remember(member,fingerprint,seen)
   elif seen['fees']!=financial:
    if seen['definition'][5]<=self.start_sequence:
     self.fee_dependencies.setdefault(str(seen['id']),dict(record=seen['definition'],metadata=seen.get('metadata'),fees_at_start=seen['fee_values'],fees_available_at_ns=seen['fee_available_at_ns']))
    self.remember(member,fingerprint,dict(seen,fees=financial,fee_values=fee_rows,fee_available_at_ns=at));self.emit('f',seen['id'],seq,[index],at,fee_rows)
   else:self.stats['repeated_trade_observations_omitted']+=1
 def remember(self,member,fingerprint,value):
  if self.trade_index is not None:self.trade_index.put(member,fingerprint,value)
  else:self.state['trades'][fingerprint]=value
 def event(self,event):
  require(event['venue'] in VENUES,'venue_scope');seq=event['collector_sequence'];at=event['timestamp_received_ns'];kind=event['event_type'];v=event['venue'];text=event.get('raw_payload','')
  raw=json.loads(text,parse_float=Decimal) if text.lstrip().startswith(('{','[')) else None
  if kind in IGNORED:self.stats['diagnostic_records_omitted']+=1
  elif kind=='market_metadata':
   if event.get('http_status')==200:self.metadata(event,raw)
  elif kind=='trade_page':
   if event.get('http_status')==200:self.trade(event,raw)
   else:self.stats['failed_HTTP_observation_omitted']+=1
  elif v=='polymarket' and kind=='ws_frame':
   if raw is None:self.stats['heartbeat_omitted']+=1
   else:
    for i,n in enumerate(raw if isinstance(raw,list) else [raw]):require(isinstance(n,dict),'poly_frame_shape');self.poly(event,n,[i,0])
  elif v=='predictfun' and kind=='orderbook_snapshot':self.predict(event,raw)
  elif kind=='connection_opening':
   if v=='polymarket':self.reset(v,0,seq,at)
   else:
    require(isinstance(raw,dict) and isinstance(raw.get('market_ids'),list) and raw.get('shard') is not None,'opening_scope_required')
    lane=v+':'+str(raw['shard']);members={str(x) for x in raw['market_ids']}|set(self.state['lanes'].get(lane,[]))
    self.reset(v,0,seq,at,sorted(members))
    self.state['lanes'][lane]=[str(x) for x in raw['market_ids']]
  elif kind in ('connection_gap','connection_closed','disconnect','capture_closed'):
   self.reset(v,self.epoch(event.get('connection_epoch')),seq,at)
  elif kind in ('connection_opened',):self.stats['connection_ack_omitted']+=1
  elif kind=='admission_book':
   # Admission REST books seed identities only when native shapes are known;
   # they do not prove a synchronized join to later WS deltas.
   self.stats['admission_book_not_used_as_WS_basis']+=1
  elif v=='predictfun' and kind=='lifecycle':
   d=raw.get('data',{}) if isinstance(raw,dict) else {}
   if isinstance(d,dict) and any(k in d for k in ('resolution','status','tradingStatus')):
    fields={k:value for k,value in d.items() if k in ('status','tradingStatus','resolution','winningOutcome','winningAssetId')}
    require(event['native_market_id'] is not None,'lifecycle_market_identity');self.emit('e',self.market(v,event['native_market_id']),seq,at,None,fields)
   else:self.stats['nonfinancial_lifecycle_omitted']+=1
  else:raise ValueError('backtest_unknown_event:'+v+':'+kind)
  self.state['through']=seq;self.state['last_received_ns']=at
 def header(self):
  s=self.state
  required=set(self.start_meta.values())|set(s['current_versions'].values())
  required.update(r[2] for r in self.records if r[0]=='m')
  checkpoints=[]
  for ident,b in self.start_books.items():checkpoints.append([int(ident),b['epoch'],b['received_ns'],b['source_ns'],b['precision'],b['version'],pairs(b,'bids'),pairs(b,'asks')])
  return dict(schema=PROFILE,columns=COLUMNS,fee_columns=list(FEE_COLUMNS),markets=list(s['markets'].values()),instruments=list(s['instruments'].values()),
   metadata_versions=[v for k,v in s['versions'].items() if int(k) in required],metadata_asof_start=self.start_meta,pairs=list(s['pairs'].values()),book_checkpoints=checkpoints,
   collection=s['collection'],fee_trade_definitions=list(self.fee_dependencies.values()),record_encoding='delta-clocks-local-identities/1',
   external_prefix_requires_history=s['external_prefix'],trade_identity='conservative_native_plus_private_leg_hash_and_same_page_rank_not_certified_unique_fills',
   clocks='source_ns_and_first_available_ns; source sequence is local receive order',native_trade_ids_replaced_by_recovery_identity=True)


def convert_events(events,catalog,state,trade_index=None,owned=False):
 # run_batch owns a newly loaded checkpoint. No copy of historical trade IDs.
 state=state if owned else {k:(dict(v) if k=='trades' else deepcopy(v)) for k,v in state.items()}
 projector=Projector(state,trade_index);projector.context(catalog)
 gap_rows=iter(sorted(catalog.get('connection_gaps',[]),key=lambda x:x['at_ns']));gap=next(gap_rows,None)
 start=state.get('last_received_ns');expected=state['through']+1
 for event in events:
  require(event['collector_sequence']==expected,'source_sequence_gap');expected+=1;at=event['timestamp_received_ns']
  if start is None:start=at
  while gap is not None and gap['at_ns']<=at:
   if gap['at_ns']>=start:projector.gap(gap,event['collector_sequence'])
   gap=next(gap_rows,None)
  projector.event(event)
 floor=(projector.state.get('last_received_ns') or 0)-300*10**9;projector.state['gap_seen']={k:v for k,v in projector.state['gap_seen'].items() if v>=floor}
 return projector

CLOCK_FIELDS={'sequence','received_ns','source_ns','available_at_ns','execution_ns'}

def wire_row(row,clocks,decode=False):
 row=list(row);tag=row[0]
 if decode and tag=='t':row.insert(2,None)
 for i,name in enumerate(COLUMNS[tag],1):
  if name in CLOCK_FIELDS and row[i] is not None:
   value=row[i];require(type(value) is int,'integer_clock')
   if decode:row[i]=value+clocks.get(name,0);clocks[name]=row[i]
   else:row[i]=value-clocks.get(name,0);clocks[name]=value
 if not decode and tag=='t':row.pop(2)
 return row


def write_archive(path,header,records):
 path=Path(path);temp=path.with_suffix('.part');count=0;clocks={}
 with zipfile.ZipFile(temp,'w',compression=zipfile.ZIP_STORED) as archive,archive.open('records.jsonl.zst','w',force_zip64=True) as target:
  with zstandard.ZstdCompressor(level=6).stream_writer(target,closefd=False) as codec:
   line=data(header);require(len(line)<=MAX_LINE,'header_size');codec.write(line)
   for record in records:
    line=data(wire_row(record,clocks));require(len(line)<=MAX_LINE,'record_size');codec.write(line);count+=1
 with temp.open('r+b') as f:f.flush();os.fsync(f.fileno())
 os.replace(temp,path);sync_directory(path.parent)
 return count


def iter_archive(path,manifest):
 require(manifest['schema']=='market-backtest-chunk/1' and manifest['encoding']==ENCODING,'archive_profile')
 require(Path(path).stat().st_size==manifest['bytes'] and sha256_file(path)==manifest['sha256'],'archive_hash')
 with zipfile.ZipFile(path) as archive:
  require(archive.namelist()==['records.jsonl.zst'] and archive.infolist()[0].compress_type==zipfile.ZIP_STORED,'archive_member')
  with archive.open('records.jsonl.zst') as packed,zstandard.ZstdDecompressor().stream_reader(packed) as raw:
   source=io.BufferedReader(raw);line=source.readline(MAX_LINE+1);require(len(line)<=MAX_LINE and line.endswith(b'\n'),'header_line')
   header=json.loads(line);require(header['schema']==PROFILE and header['columns']==COLUMNS and header.get('record_encoding')=='delta-clocks-local-identities/1','archive_columns');yield header
   count=0;clocks={}
   while True:
    line=source.readline(MAX_LINE+1)
    if not line:break
    require(len(line)<=MAX_LINE and line.endswith(b'\n'),'record_line');row=json.loads(line)
    require(isinstance(row,list) and row and row[0] in COLUMNS and len(row)==len(COLUMNS[row[0]])+(0 if row[0]=='t' else 1),'record_shape');count+=1;yield wire_row(row,clocks,decode=True)
   require(count==manifest['rows'],'archive_rows')


def initial_state(collection,first,allow_external_prefix=False):
 require(isinstance(collection,str) and collection and type(first) is int and first>0,'source_identity')
 require(first==1 or allow_external_prefix,'missing_initial_prefix');return fresh(collection,first)


def collect_recovery(output,head,index):
 """Validate both checkpoint hashes before removing any unreachable owned shard."""
 refs=[dict(checkpoint=head['checkpoint'],checkpoint_sha256=head['checkpoint_sha256'])]
 if head.get('previous_checkpoint'):refs.append(head['previous_checkpoint'])
 protected=[];keep=set()
 for ref in refs:
  require(re.fullmatch(r'recovery/[0-9]+(?:-[a-f0-9]{16})?\.json\.zst',ref['checkpoint']) is not None,'checkpoint_path')
  p=output/ref['checkpoint'];require(not p.is_symlink() and sha256_file(p)==ref['checkpoint_sha256'],'gc_checkpoint_hash')
  keep.add(p.name);protected.append(json.loads(zstandard.ZstdDecompressor().decompress(p.read_bytes()))['trade_index'])
 index.collect(protected)
 for p in (output/'recovery').glob('*.json.zst'):
  require(not p.is_symlink() and re.fullmatch(r'[0-9]+(?:-[a-f0-9]{16})?\.json\.zst',p.name) is not None,'checkpoint_scope')
  if p.name not in keep:p.unlink()


def run_batch(output,*,collection,identity,first,last,input_sha256,catalog,event_factory,allow_external_prefix=False,fail_before_head=False):
 output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True);
 for child in ('work','chunks','recovery','trade-index'):require(not (output/child).is_symlink(),'output_directory_symlink')
 lock=OwnerLock(output/'writer.lock')
 try:
  require(identity==f'{first:020d}-{last:020d}' and re.fullmatch('[a-f0-9]{64}',input_sha256),'input_binding')
  head_path=output/'head.json';head=json.loads(head_path.read_bytes()) if head_path.exists() else None
  if head:
   require(head['collection']==collection and head['profile']==PROFILE and head.get('codec_sha256')==CODE_SHA256,'output_source_or_codec_changed')
   if last<=head['through']:
    control=output/'chunks'/identity/'recovery.json';require(control.exists(),'before_retained_restart_boundary');saved=json.loads(control.read_bytes());require(saved['input_sha256']==input_sha256,'completed_input_changed')
    target=output/'chunks'/identity;manifest=json.loads((target/'chunk.json').read_bytes())
    require((target/'data.zip').is_file() and (target/'data.zip').stat().st_size==manifest['bytes'] and sha256_file(target/'data.zip')==manifest['sha256'],'completed_archive_missing_or_changed')
    require(saved.get('manifest_sha256')==sha256_file(target/'chunk.json'),'completed_manifest_changed')
    collect_recovery(output,head,TradeIndex(output/'trade-index'))
    return dict(status='already_compacted',identity=identity,through=head['through'],new_rows=0)
   require(first==head['through']+1,'source_prefix_gap')
   require(re.fullmatch(r'recovery/[0-9]+(?:-[a-f0-9]{16})?\.json\.zst',head['checkpoint']) is not None,'checkpoint_path');checkpoint=output/head['checkpoint'];require(not checkpoint.is_symlink(),'checkpoint_symlink');require(sha256_file(checkpoint)==head['checkpoint_sha256'],'checkpoint_hash')
   state=json.loads(zstandard.ZstdDecompressor().decompress(checkpoint.read_bytes()))
  else:state=initial_state(collection,first,allow_external_prefix)
  index=TradeIndex(output/'trade-index',state['trade_index'])
  target=output/'chunks'/identity;require(not target.is_symlink(),'chunk_directory_symlink')
  if target.exists():
   saved=json.loads((target/'recovery.json').read_bytes());require(saved['input_sha256']==input_sha256,'recovery_input_changed');manifest=json.loads((target/'chunk.json').read_bytes())
   require(saved.get('manifest_sha256')==sha256_file(target/'chunk.json'),'recovery_manifest_changed');require((target/'data.zip').stat().st_size==manifest['bytes'] and sha256_file(target/'data.zip')==manifest['sha256'],'recovery_archive_changed');require(re.fullmatch(r'recovery/[0-9]+(?:-[a-f0-9]{16})?\.json\.zst',saved['checkpoint']) is not None,'checkpoint_path');checkpoint=output/saved['checkpoint'];require(not checkpoint.is_symlink(),'checkpoint_symlink');require(sha256_file(checkpoint)==saved['checkpoint_sha256'],'recovery_checkpoint_changed')
   new_head=saved['head'];stats=saved['stats']
   require(new_head.get('profile')==PROFILE and new_head.get('codec_sha256')==CODE_SHA256 and new_head.get('collection')==collection and new_head.get('identity')==identity and new_head.get('through')==last,'recovery_head_binding')
   require(new_head.get('checkpoint')==saved['checkpoint'] and new_head.get('checkpoint_sha256')==saved['checkpoint_sha256'],'recovery_checkpoint_binding')
   parent={k:head[k] for k in ('checkpoint','checkpoint_sha256')} if head else None
   require(new_head.get('previous_checkpoint')==parent,'recovery_parent_changed')
   resumed=json.loads(zstandard.ZstdDecompressor().decompress(checkpoint.read_bytes()));TradeIndex(output/'trade-index',resumed['trade_index']).verify()
  else:
   require(state['through']==first-1 and state['collection']==collection and state['schema']=='market-backtest-checkpoint/2','checkpoint_sequence');projector=convert_events(event_factory(),catalog,state,index,owned=True);state=projector.state;require(state['through']==last,'source_last_sequence')
   target.parent.mkdir(exist_ok=True);work=output/'work';work.mkdir(exist_ok=True);temp=work/identity
   if temp.exists():
    # Only replace this tool's incomplete staging; source archives stay intact.
    require(not temp.is_symlink() and temp.resolve().parent==work.resolve(),'work_scope')
    for p in temp.iterdir():require(p.name in ('data.zip','data.part','chunk.json','recovery.json') and p.is_file() and not p.is_symlink(),'unexpected_staging_dependency');p.unlink()
   else:temp.mkdir()
   rows=write_archive(temp/'data.zip',projector.header(),projector.records)
   state['trade_index']=index.flush()
   active_epochs={b['epoch'] for b in state['books'].values()}
   state['epochs']={k:v for k,v in state['epochs'].items() if v in active_epochs}
   # Past metadata is already in immutable chunks or sparse fee definitions.
   state['versions']={k:v for k,v in state['versions'].items() if int(k) in set(state['current_versions'].values())}
   state['version_keys']={k:v for k,v in state['version_keys'].items() if str(v) in state['versions']}
   manifest=dict(schema='market-backtest-chunk/1',encoding=ENCODING,identity=identity,first=first,last=last,rows=rows,file='data.zip',bytes=(temp/'data.zip').stat().st_size,sha256=sha256_file(temp/'data.zip'))
   durable_file(temp/'chunk.json',encoded(manifest));recovery=output/'recovery';recovery.mkdir(exist_ok=True);checkpoint=recovery/(str(last)+'.json.zst')
   checkpoint_bytes=encoded(state);require(len(checkpoint_bytes)<=16*1024**2,'control_checkpoint_capacity')
   durable_file(checkpoint,zstandard.ZstdCompressor(level=3).compress(checkpoint_bytes))
   new_head=dict(profile=PROFILE,codec_sha256=CODE_SHA256,collection=collection,through=last,identity=identity,checkpoint=checkpoint.relative_to(output).as_posix(),checkpoint_sha256=sha256_file(checkpoint))
   if head:new_head['previous_checkpoint']={k:head[k] for k in ('checkpoint','checkpoint_sha256')}
   stats=dict(projector.stats);control=dict(input_sha256=input_sha256,manifest_sha256=sha256_file(temp/'chunk.json'),checkpoint=new_head['checkpoint'],checkpoint_sha256=new_head['checkpoint_sha256'],head=new_head,stats=stats)
   durable_file(temp/'recovery.json',encoded(control));os.replace(temp,target);sync_directory(target.parent)
  if fail_before_head:raise RuntimeError('injected_before_head')
  durable_file(head_path,encoded(new_head),replace=True)
  collect_recovery(output,new_head,index)
  return dict(status='compacted',identity=identity,through=last,rows=manifest['rows'],payload_bytes=manifest['bytes']+(target/'chunk.json').stat().st_size,stats=stats,chunk=str(target),raw_sources_retained=True,uploaded=False)
 finally:lock.close()

def retire_finished_market(output,member,tail_receipt):
 """Explicit OFFLINE retirement, never infers closure from age or silence.

The trusted tail producer must bind a final recording boundary and source sequence.
Every already emitted chunk must still have a verified local payload. This narrow
path does NOT accept a boolean HF claim or delete any source/archive. Production
HF-only retirement needs its own authenticated exact-revision receipt integration.
A later source event for this member fails closed rather than silently duplicating.
 """
 output=Path(output);lock=OwnerLock(output/'writer.lock')
 try:
  head=json.loads((output/'head.json').read_bytes());require(head['codec_sha256']==CODE_SHA256,'retirement_codec')
  require(re.fullmatch(r'recovery/[0-9]+(?:-[a-f0-9]{16})?\.json\.zst',head['checkpoint']) is not None,'checkpoint_path')
  checkpoint=output/head['checkpoint'];require(sha256_file(checkpoint)==head['checkpoint_sha256'],'checkpoint_hash')
  state=json.loads(zstandard.ZstdDecompressor().decompress(checkpoint.read_bytes()))
  receipt_path=Path(tail_receipt);proof=json.loads(receipt_path.read_bytes());digest=sha256_file(receipt_path)
  require(proof.get('schema')=='compact-tail-complete/1' and proof.get('collection')==state['collection'] and proof.get('member')==member,'retirement_tail_binding')
  if member in state['retired']:
   require(state['retired'][member]['tail_sha256']==digest,'retirement_changed');collect_recovery(output,head,TradeIndex(output/'trade-index'));return {'status':'already_retired'}
  require(member in state['markets'],'retirement_market')
  windows=state['windows'].get(member,{})
  require(windows and all(end is not None for start,end in windows.values()),'retirement_open_window')
  end=max(x[1] for x in windows.values())
  require(proof.get('final_recording_end_ns')==end and proof.get('through_sequence')==state['through'] and proof.get('tail_complete') is True and proof.get('no_more_source_events') is True,'retirement_tail_incomplete')
  chunks=[]
  for target in (output/'chunks').iterdir():
   require(target.is_dir() and not target.is_symlink(),'retirement_chunk_scope')
   manifest=json.loads((target/'chunk.json').read_bytes());control=json.loads((target/'recovery.json').read_bytes())
   require(control['manifest_sha256']==sha256_file(target/'chunk.json'),'retirement_manifest')
   require((target/'data.zip').is_file() and (target/'data.zip').stat().st_size==manifest['bytes'] and sha256_file(target/'data.zip')==manifest['sha256'],'retirement_payload_required')
   require(manifest['last']<=state['through'],'retirement_pending_chunk');chunks.append(manifest['identity'])
  require(head['identity'] in chunks,'retirement_head_payload')
  m=state['markets'][member][0];instruments={r[0] for r in state['instruments'].values() if r[1]==m}
  state['markets'].pop(member);state['windows'].pop(member,None);state['current_versions'].pop(str(m),None);state['settlements'].pop(str(m),None)
  state['instruments']={k:v for k,v in state['instruments'].items() if v[1]!=m}
  state['assets']={k:v for k,v in state['assets'].items() if v[1] not in instruments}
  state['books']={k:v for k,v in state['books'].items() if int(k) not in instruments}
  state['anchors']={k:v for k,v in state['anchors'].items() if k not in {str(i) for i in instruments}|{'market:'+str(m)}}
  state['pairs']={k:v for k,v in state['pairs'].items() if m not in v[1]}
  state['versions']={k:v for k,v in state['versions'].items() if v[1]!=m}
  state['version_keys']={k:v for k,v in state['version_keys'].items() if str(v) in state['versions']}
  v,mid=member.split(':',1)
  if v=='polymarket':state['conditions']={k:value for k,value in state['conditions'].items() if value!=mid}
  for lane,markets in state['lanes'].items():
   if lane.startswith(v+':'):state['lanes'][lane]=[x for x in markets if x!=mid]
  state['retired'][member]=dict(tail_sha256=digest,end_ns=end,through=state['through'])
  index=TradeIndex(output/'trade-index',state['trade_index']);index.retire(member);state['trade_index']=index.flush()
  blob=encoded(state);require(len(blob)<=16*1024**2,'control_checkpoint_capacity')
  checkpoint=output/'recovery'/(str(state['through'])+'-'+hashlib.sha256(blob).hexdigest()[:16]+'.json.zst')
  if not checkpoint.exists():durable_file(checkpoint,zstandard.ZstdCompressor(level=3).compress(blob))
  head['previous_checkpoint']={k:head[k] for k in ('checkpoint','checkpoint_sha256')}
  head.update(checkpoint=checkpoint.relative_to(output).as_posix(),checkpoint_sha256=sha256_file(checkpoint));durable_file(output/'head.json',encoded(head),replace=True)
  collect_recovery(output,head,index)
  return dict(status='retired_index_only',member=member,payloads_preserved=len(chunks),tail_sha256=digest)
 finally:lock.close()

class BookReplay:
 """Cold reconstruction of retained full depth; checkpoints are not new ticks."""
 def __init__(self,header):
  self.books={}
  for ident,epoch,received,source,precision,version,bids,asks in header['book_checkpoints']:
   self.books[ident]=dict(epoch=epoch,valid=True,bids=levels(bids),asks=levels(asks))
 def apply(self,record):
  tag=record[0];r=dict(zip(COLUMNS[tag],record[1:]))
  if tag=='r':
   for i in r['instruments']:
    if i in self.books:self.books[i]['valid']=False
  elif tag in ('s','d','u','h'):
   i=r['instrument'];old=self.books.get(i);valid=bool(old and old['valid'] and old['epoch']==r['epoch'])
   if tag=='s':self.books[i]=dict(epoch=r['epoch'],valid=True,bids=levels(r['bids']),asks=levels(r['asks']))
   elif tag=='d':
    require(r['valid']==valid,'delta_basis_changed')
    if not valid:self.books[i]=dict(epoch=r['epoch'],valid=False,bids={},asks={});return
    self.update(i,r)
   elif tag=='u':require(valid,'snapshot_delta_basis_missing');self.update(i,r)
   else:require(valid,'refresh_basis_missing')
 def update(self,i,row):
  for side in ('bids','asks'):
   for p,q in row[side]:
    if Decimal(q):self.books[i][side][p]=q
    else:self.books[i][side].pop(p,None)
 def view(self,i):
  b=self.books.get(i)
  return None if not b or not b['valid'] else {side:pairs(b,side) for side in ('bids','asks')}


def expanded(header,record):
 """Small explicit adapter for future consumers; no native order engine needed."""
 tag=record[0];row=dict(zip(COLUMNS[tag],record[1:]));row['type']=tag
 markets={r[0]:r for r in header['markets']};instruments={r[0]:r for r in header['instruments']}
 if row.get('instrument') is not None:
  instrument=instruments[row['instrument']];row.update(asset_id=instrument[2],index_set=instrument[3],book_origin=instrument[4]);mid=instrument[1]
 else:mid=row.get('market')
 if mid is not None:row.update(venue=markets[mid][1],market_id=markets[mid][2])
 if tag=='t':row['fees']=[dict(zip(FEE_COLUMNS,f)) for f in row['fees']]
 return row
