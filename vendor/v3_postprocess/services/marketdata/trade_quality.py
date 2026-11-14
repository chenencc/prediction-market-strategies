"""Bounded public-API evidence gates, shared across venues.

Quiet markets are not stale merely because their last trade is old. HTTP
response age, comparable HEAD regressions and known WS overlap are independent
checks. Passing these checks never certifies complete exchange fills.
"""
import hashlib,json,re,math
from collections import Counter
from datetime import timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from .core import iso_to_ns
from .poly_trade_policy import INDEX_GRACE_NS

HTTP_FIELDS={'date','age','cache-control','etag','last-modified','cf-cache-status','x-cache','via','retry-after'}

def evidence_headers(headers):
    return {str(k).lower():str(v) for k,v in headers.items() if str(k).lower() in HTTP_FIELDS or str(k).lower().startswith('ratelimit')}

def http_freshness(headers,received_ns,max_age_seconds=20,clock_tolerance_seconds=5):
    h=evidence_headers(headers);reasons=[];age=None;date_ns=None;cache_ttl=None
    if 'age' in h:
        try:
            age=float(h['age'])
            if age<0 or not math.isfinite(age):raise ValueError()
        except (ValueError,OverflowError):age=None;reasons.append('invalid_age_header')
    if 'date' in h:
        try:
            dt=parsedate_to_datetime(h['date'])
            if dt.tzinfo is None:raise ValueError()
            date_ns=int(dt.timestamp()*10**9);apparent=(received_ns-date_ns)/1e9
            if apparent < -clock_tolerance_seconds:reasons.append('server_clock_in_future')
            age=max(age or 0,max(0,apparent))
        except (ValueError,TypeError,OverflowError):reasons.append('invalid_date_header')
    match=re.search(r'(?:^|,)\s*(?:s-maxage|max-age)\s*=\s*"?(\d+)',h.get('cache-control',''),re.I)
    if match:cache_ttl=int(match.group(1))
    if age is None:reasons.append('http_freshness_evidence_missing')
    elif age>max_age_seconds:reasons.append('http_response_too_old')
    return {'acceptable':not reasons,'reasons':reasons,'response_age_seconds':age,'server_date_ns':date_ns,'cache_max_age_seconds':cache_ttl,'headers':h,'not_a_source_generation_clock':True}

def predict_identity(row):
    sid=row.get('settlementId');mid=str(row.get('market',{}).get('id'))
    return ('predictfun',mid,str(sid)) if sid else None

def poly_overlap_key(row,wire=False):
    """A relaxed observation group, NOT an execution/fill deduplication key."""
    tx=row.get('transaction_hash',row.get('transactionHash'));asset=row.get('asset_id',row.get('assetId')) if wire else row.get('token_id',row.get('asset_id',row.get('assetId')))
    market=row.get('market',row.get('condition_id',row.get('conditionId')))
    side=row.get('side')
    if not tx or not asset or not market or not side:return None
    return (str(market).lower(),str(tx).lower(),str(asset),str(side).upper())

def poly_execution_ns(row,wire=False):
    value=row.get('timestamp')
    if value is None:return None
    if isinstance(value,bool):raise ValueError('boolean_source_timestamp')
    return int(value)*(10**6 if wire else 10**9)

class KnownWSTrades:
    """Bounded receive-time witness; never joins observations on tx hash alone."""
    def __init__(self,max_groups=100000):
        self.max_groups=max_groups;self.groups={};self.uncomparable=[];self.overflow=False
    def observe(self,event,available_at_ns):
        kind=event.get('event_type',event.get('type'))
        if kind!='last_trade_price':return False
        row=event.get('payload',event);key=poly_overlap_key(row,wire=True)
        try:execution=poly_execution_ns(row,wire=True)
        except (ValueError,TypeError,OverflowError):execution=None
        if key is None or execution is None:
            self.uncomparable.append({'available_at_ns':available_at_ns,'execution_ns':execution,'condition':str(row.get('market','')).lower() or None});self.overflow|=len(self.uncomparable)>self.max_groups
            if len(self.uncomparable)>self.max_groups:self.uncomparable=self.uncomparable[-self.max_groups:]
            return False
        if key not in self.groups:
            if len(self.groups)>=self.max_groups:self.overflow=True;return False
            self.groups[key]={'source_times':set(),'available_at_ns':available_at_ns,'observations':0}
        g=self.groups[key];g['source_times'].add(execution);g['available_at_ns']=min(g['available_at_ns'],available_at_ns);g['observations']+=1
        if event.get('_collector_native_reference'):g.setdefault('native_reference',event['_collector_native_reference'])
        if row.get('size') is not None:g.setdefault('observed_size',str(row['size']))
        return True
    def required(self,lower_ns,upper_ns,asof_ns,conditions=None):
        result=set()
        for key,g in self.groups.items():
            if conditions and key[0] not in conditions:continue
            if g['available_at_ns']<=asof_ns and any(lower_ns<=t<upper_ns for t in g['source_times']):result.add(key)
        return result
    def unknown_in_window(self,lower_ns,upper_ns,asof_ns,conditions=None):
        return sum(x['available_at_ns']<=asof_ns and (not conditions or not x.get('condition') or x['condition'] in conditions) and (x['execution_ns'] is None or lower_ns<=x['execution_ns']<upper_ns) for x in self.uncomparable)

class TradeQuality:
    def __init__(self,max_response_age_seconds=20,witness=None,max_observed_groups=1000000):
        from .myriad_trade_quality import MyriadKnownWSTrades
        self.myriad_witness=MyriadKnownWSTrades();self.myriad_ws_check=False
        self.max_age=max_response_age_seconds;self.heads={};self.witness=witness or KnownWSTrades();self.union={};self.max_groups=max_observed_groups;self.union_overflow=False
    def remember(self,identity,execution_ns):
        if identity is None:return
        if identity not in self.union and len(self.union)>=self.max_groups:self.union_overflow=True;return
        self.union[identity]=execution_ns
    def head(self,scope,source_times,headers,request_ns,received_ns,raw_sha256):
        fresh=http_freshness(headers,received_ns,self.max_age);reasons=list(fresh['reasons']);latest=max(source_times,default=None);previous=self.heads.get(scope);baseline=previous['latest_source_ns'] if previous else None
        ordered=previous is None or request_ns>=previous['request_ns']
        if previous and not ordered:reasons.append('out_of_order_head_response')
        if baseline is not None and ordered and (latest is None or latest<baseline):reasons.append('head_source_watermark_regressed')
        repeated=previous is not None and raw_sha256==previous['raw_sha256']
        # Never lower a prior source watermark or discard the observed union.
        if ordered:
            self.heads[scope]={'latest_source_ns':max([t for t in (baseline,latest) if t is not None],default=None),'request_ns':request_ns,'raw_sha256':raw_sha256}
        ttl=fresh['cache_max_age_seconds'];age=fresh['response_age_seconds']
        retry_seconds=max(1,ttl-age) if ttl is not None and age is not None else 1
        return {'scope':scope,'acceptable':not reasons,'reasons':reasons,'latest_source_ns':latest,'previous_highwater_ns':baseline,'body_repeated':repeated,'freshness':fresh,'retry_not_before_ns':received_ns+int(retry_seconds*10**9),'quiet_market_age_alone_not_failure':True,'request_ns':request_ns,'received_ns':received_ns}
    def finish(self,head,pagination_complete,observed_keys,lower_ns,upper_ns,asof_ns,conditions=None,check_ws=True):
        reasons=list((head or {}).get('reasons',[]))
        if head is None:reasons.append('head_quality_evidence_missing')
        if not pagination_complete:reasons.append('pagination_incomplete')
        expected=self.witness.required(lower_ns,upper_ns,asof_ns,conditions) if check_ws else set();missing=expected-set(observed_keys)
        if missing:reasons.append('known_WS_trade_absent_from_API')
        if check_ws and self.witness.unknown_in_window(lower_ns,upper_ns,asof_ns,conditions):reasons.append('WS_identity_incomplete')
        if self.witness.overflow and check_ws:reasons.append('WS_witness_capacity_exceeded')
        if self.union_overflow:reasons.append('observed_union_capacity_exceeded')
        reasons=list(dict.fromkeys(reasons));matched=len(expected)-len(missing)
        young=sum(asof_ns-self.witness.groups[k]['available_at_ns']<INDEX_GRACE_NS for k in missing)
        samples=[dict(key=list(k),first_WS_received_ns=self.witness.groups[k]['available_at_ns'],
            native_reference=self.witness.groups[k].get('native_reference'),observed_size=self.witness.groups[k].get('observed_size'),
            age_seconds=max(0,(asof_ns-self.witness.groups[k]['available_at_ns'])/1e9)) for k in sorted(missing)[:20]]
        return {'status':'quality_pending' if reasons else 'observed_reference_pass','acceptable':not reasons,'reasons':reasons,'pagination_complete':pagination_complete,'head':head,
                'known_WS_groups':len(expected),'known_WS_matched_groups':matched,'known_WS_missing_groups':len(missing),'match_basis':'transaction+market+asset+side observation groups, not unique fills' if check_ws else 'public_WS_trade_tape_unavailable_API_only',
                'pending_indexing_groups':young,'persistent_missing_groups':len(missing)-young,'missing_reference_samples':samples,
                'observed_union_groups':len(self.union),'source_truth_certified':False,'retry_not_before_ns':(head or {}).get('retry_not_before_ns',asof_ns+10**9),'must_not_block_book_context_delivery':True}
