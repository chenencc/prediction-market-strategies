"""Compact, explicitly projected source archive for book/trade backtests.

Not a Nautilus catalog exporter. Content IDs are local storage references, never
venue fill IDs. Ordered API page occurrences remain intact after content reuse.
No wallet, signer, profile, full HTTP headers or per-observation SHA is emitted.
"""
from collections import Counter, defaultdict
import hashlib
import json
import os
import re
from decimal import Decimal
from pathlib import Path
import shutil
import time

import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.core import atomic, exact, iso_to_ns
from .fees import canonical
from .stream_writer import arrow_digest

PROFILE='nautilus-source/1'
ROOT='v3'
ENCODING=dict(compression='zstd',compression_level=9,version='2.6',data_page_version='2.0',
              row_group_size=65536,dictionary_pagesize_limit=8*1024**2,write_page_checksum=True)
DICTIONARIES={'market_id','asset_id','transaction_hash','settlement_id','order_id','side',
              'price_unit','size_unit','market_version_id','source_market_version_id','fee_version_id',
              'fee_status','fee_unit','role','kind','venue'}
FEE_FIELDS=('leg','leg_index','fee_rate_bps','fee_amount_native','fee_unit','extra_fee_json',
            'amount_decimals','amount_converted_to_cash','amount_is_estimate','fee_status')
FEE_LEG=pa.struct([('leg',pa.string()),('leg_index',pa.int32()),('fee_rate_bps',pa.string()),
    ('fee_amount_native',pa.string()),('fee_unit',pa.string()),('extra_fee_json',pa.string()),
    ('amount_decimals',pa.int32()),('amount_converted_to_cash',pa.string()),
    ('amount_is_estimate',pa.bool_()),('fee_status',pa.string())])
ORDER_LEG=pa.struct([('role',pa.string()),('leg_index',pa.int32()),('order_id',pa.string()),
    ('asset_id',pa.string()),('index_set',pa.int32()),('price_native',pa.string()),
    ('size_native',pa.string()),('quote_type',pa.string())])
PAYLOAD_FIELDS=('market_id','execution_ns','source_precision_ns','settlement_id','transaction_hash',
    'asset_id','index_set','price_native','size_native','price_unit','size_unit','side',
    'window_membership','fee_bundle_id','legs')
TRADE_SCHEMA=pa.schema([('payload_id',pa.uint32()),('market_id',pa.string()),('execution_ns',pa.int64()),
    ('source_precision_ns',pa.int64()),('settlement_id',pa.string()),('transaction_hash',pa.string()),
    ('asset_id',pa.string()),('index_set',pa.int32()),('price_native',pa.string()),('size_native',pa.string()),
    ('price_unit',pa.string()),('size_unit',pa.string()),('side',pa.string()),('window_membership',pa.string()),
    ('fee_bundle_id',pa.uint32()),('legs',pa.list_(ORDER_LEG))])
FEE_SCHEMA=pa.schema([('fee_bundle_id',pa.uint32()),('legs',pa.list_(FEE_LEG))])
OCCURRENCE=pa.struct([('row_index',pa.uint32()),('payload_id',pa.uint32())])
PAGE_CONTENT_SCHEMA=pa.schema([('content_id',pa.uint32()),('occurrences',pa.list_(OCCURRENCE))])
PAGE_SCHEMA=pa.schema([('sequence',pa.uint64()),('market_id',pa.string()),('available_at_ns',pa.int64()),
    ('requested_at_ns',pa.int64()),('source_market_version_id',pa.string()),('http_status',pa.int16()),
    ('returned_rows',pa.uint32()),('content_id',pa.uint32())])
QUALITY_SCHEMA=pa.schema([('market_id',pa.string()),('available_at_ns',pa.int64()),('sequence',pa.uint64()),
    ('kind',pa.string()),('reasons',pa.list_(pa.string())),('acceptable',pa.bool_()),
    ('pagination_complete',pa.bool_()),('lower_ns',pa.int64()),('upper_ns',pa.int64()),
    ('known_WS_missing_groups',pa.uint32()),('persistent_missing_groups',pa.uint32())])
GAP_SCHEMA=pa.schema([('start_ns',pa.int64()),('end_ns',pa.int64()),('kind',pa.string()),
    ('reported_gap_seconds',pa.string()),('affected_assets',pa.uint32()),('asset_scope_known',pa.bool_())])
META_FIELDS={'sportsGameId','gameId','acceptingOrders','acceptingOrdersTimestamp','active','closed','enableOrderBook','createdAt',
    'endDate','endDateIso','startDate','startDateIso','gameStartTime','eventStartTime','secondsDelay',
    'resolutionSource','resolvedBy','resolution','resolverAddress','oracleQuestionId','questionID',
    'questionIndex','marketType','marketVariant','variantData','variantDetails','sportsMarketType',
    'groupItemThreshold','groupItemTitle','negRiskMarketID','negRiskRequestID','negRiskOther',
    'positionIds','version','status','tradingStatus','fallbackResolutionType','umaResolutionStatus',
    'umaResolutionStatuses','isYieldBearing','collateralToken','collateralTokenAddress','collateral',
    'quoteCurrency','quote_currency','chainId','chain_id','decimals','amountDecimals',
    'rewardsMaxSpread','rewardsMinSize','shareThreshold','spreadThreshold',
    'networkId','tradingModel','executionMode','eventId','outcomeIndex','externalSources',
    'token','nativePriceScale','nativeAmountDecimals','state','voided','resolvedOutcomeId',
    'publishedAt','expiresAt','resolvesAt','oracle'}
PATCH_FIELDS=META_FIELDS|{'orderPriceMinTickSize','orderMinSize','minimumOrderSize','decimalPrecision','tickSize',
    'priceIncrement','minimumTick','feeSchedule','feeRateBps','makerFeeBps','feesEnabled','feeType',
    'makerBaseFee','takerBaseFee','collateralDecimals','outcomes','marketOutcomes','resolutionIndexSet',
    'openPrice','closePrice','strikePrice','priceToBeat','openingPrice','closingPrice'}
DENIED_KEYS={'signer','proxy_wallet','wallet','walletAddress','marketMakerAddress','submitted_by',
             'profile_image','profile_image_optimized','image','imageUrl','icon','avatar','bio','pseudonym'}


def scrub(value):
    if isinstance(value,dict):return {k:scrub(v) for k,v in value.items() if k not in DENIED_KEYS}
    if isinstance(value,list):return [scrub(v) for v in value]
    return value


def write_pair_json(path,value):
    path=Path(path).resolve();path.parent.mkdir(parents=True,exist_ok=True)
    if os.name=='nt':
        text=str(path)
        if not text.startswith('\\\\?\\'):
            text=('\\\\?\\UNC\\'+text[2:]) if text.startswith('\\\\') else '\\\\?\\'+text
        path=Path(text)
    atomic(path,value)


def rows(path,columns=None):
    with pq.ParquetFile(path,page_checksum_verification=True) as reader:
        for batch in reader.iter_batches(batch_size=4096,columns=columns):
            yield from batch.to_pylist()


def seq(reference):
    value=str(reference).split(':')
    if len(value)<2 or not value[1].isdigit():raise ValueError('source_sequence_required')
    return int(value[1])


def actual_fee(row):
    return any(row.get(k) is not None for k in ('fee_rate_bps','fee_amount_native','fee_unit','extra_fee_ref'))


def financial_legs(row,objects):
    if row.get('venue')=='myriad':
        return [dict(role='native_action',leg_index=0,order_id=None,asset_id=row['asset_id'],index_set=None,
            price_native=row['price_native'],size_native=row['size_native'],quote_type=row['side'])]
    result=[]
    for role,key in (('taker','taker_ref'),('maker','makers_ref')):
        ref=row.get(key);value=objects[ref] if ref else None
        legs=(value or []) if role=='maker' else [value] if value else []
        for index,leg in enumerate(legs):
            outcome=leg.get('outcome') or {}
            result.append(dict(role=role,leg_index=index,order_id=leg.get('hash'),
                asset_id=str(outcome['onChainId']) if outcome.get('onChainId') is not None else None,
                index_set=outcome.get('indexSet'),price_native=leg.get('price'),size_native=leg.get('amount'),
                quote_type=leg.get('quoteType')))
    return result


class CompactTrades:
    def __init__(self,venue,objects):
        self.venue=venue;self.objects=objects;self.payloads=[];self.fees=[];self.contents=[];self.pages=[]
        self._payload={};self._fees={};self._content={};self.expected=hashlib.sha256();self.observations=0
        self.fee_observations=0;self.omitted_empty_fees=0

    def page(self,receipt,observations,fee_rows):
        occurrences=[];at=receipt['available_at_ns'];sequence=receipt['sequence'];version=None
        for row in observations:
            if row['available_at_ns']!=at or seq(row['source_ref'])!=sequence or row['market_id']!=receipt['market_id']:
                raise ValueError('API_page_observation_binding_changed')
            if version is not None and version!=row['source_market_version_id']:raise ValueError('API_page_metadata_assignment_changed')
            version=row['source_market_version_id'];charge=[]
            for fee in fee_rows.pop(row['observation_id'],[]):
                if fee['available_at_ns']!=at or fee['market_id']!=row['market_id']:raise ValueError('fee_observation_binding_changed')
                expected=row['transaction_hash'] if self.venue in ('polymarket','myriad') else row['settlement_id']
                if 'execution_reference' in fee and fee['execution_reference']!=expected:raise ValueError('fee_execution_binding_changed')
                if 'source_ref' in fee and fee['source_ref']!=row['source_ref']:raise ValueError('fee_page_binding_changed')
                if not actual_fee(fee):self.omitted_empty_fees+=1;continue
                payload={k:fee.get(k) for k in FEE_FIELDS};ref=fee.get('extra_fee_ref')
                payload['extra_fee_json']=canonical(scrub(self.objects[ref])) if ref else None
                charge.append(payload)
            self.fee_observations+=len(charge)
            fee_key=canonical(charge);fee_id=None
            if charge:
                fee_id=self._fees.get(fee_key)
                if fee_id is None:
                    fee_id=len(self.fees);self._fees[fee_key]=fee_id;self.fees.append(dict(fee_bundle_id=fee_id,legs=charge))
            payload={k:row.get(k) for k in PAYLOAD_FIELDS};payload['fee_bundle_id']=fee_id
            payload['legs']=financial_legs(row,self.objects)
            key=canonical(payload);ident=self._payload.get(key)
            if ident is None:
                ident=len(self.payloads);self._payload[key]=ident;self.payloads.append(dict(payload_id=ident,**payload))
            index=row['source_row_index'];occurrences.append(dict(row_index=index,payload_id=ident))
            self._update(self.expected,receipt,row['source_market_version_id'],index,payload,charge)
            self.observations+=1
        if len(occurrences)!=receipt['returned_rows']:raise ValueError('API_page_row_count_changed')
        if [o['row_index'] for o in occurrences]!=list(range(len(occurrences))):raise ValueError('API_page_order_or_multiplicity_changed')
        content_key=tuple((o['row_index'],o['payload_id']) for o in occurrences);content_id=self._content.get(content_key)
        if content_id is None:
            content_id=len(self.contents);self._content[content_key]=content_id
            self.contents.append(dict(content_id=content_id,occurrences=occurrences))
        self.pages.append(dict(**receipt,source_market_version_id=version,content_id=content_id))

    @staticmethod
    def _update(h,page,version,index,payload,charge):
        content={k:v for k,v in payload.items() if k!='fee_bundle_id'}
        h.update(canonical([page['sequence'],page['market_id'],page['available_at_ns'],page['requested_at_ns'],
                            page['http_status'],version,index,content,charge]).encode());h.update(b'\n')

    def verify(self,pages,payloads,fees,contents):
        h=hashlib.sha256();count=0;fee_count=0
        for page in pages:
            for occurrence in contents[page['content_id']]['occurrences']:
                payload=payloads[occurrence['payload_id']];fee_id=payload['fee_bundle_id']
                charge=fees[fee_id]['legs'] if fee_id is not None else []
                self._update(h,page,page['source_market_version_id'],occurrence['row_index'],
                             {k:payload[k] for k in PAYLOAD_FIELDS},charge)
                count+=1;fee_count+=len(charge)
        if count!=self.observations or fee_count!=self.fee_observations or h.digest()!=self.expected.digest():
            raise ValueError('compact_financial_roundtrip_changed')
        return dict(observations=count,reported_fee_legs=fee_count,pages=len(pages),payload_versions=len(payloads),
                    fee_bundles=len(fees),page_contents=len(contents),all_financial_fields_and_occurrences_verified=True)


class Output:
    def __init__(self,root):self.root=Path(root);self.receipts=[]

    def write(self,relative,table):
        target=self.root/relative;target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists():raise ValueError('immutable_output_already_exists')
        temp=target.with_suffix('.parquet.part');expected=[]
        for offset in range(0,len(table),65536):expected.append(arrow_digest(table.slice(offset,65536),table.schema))
        if not expected:expected.append(arrow_digest(table,table.schema))
        pq.write_table(table,temp,use_dictionary=[c for c in table.column_names if c in DICTIONARIES],**ENCODING)
        with pq.ParquetFile(temp,page_checksum_verification=True) as reader:
            if reader.num_row_groups!=len(expected):raise ValueError('compact_row_groups_changed')
            for index,h in enumerate(expected):
                if arrow_digest(reader.read_row_group(index),table.schema)!=h:raise ValueError('compact_cold_readback_changed')
        temp.rename(target)
        with target.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        self.receipts.append(dict(file=relative,rows=len(table),bytes=target.stat().st_size,sha256=sha,local_readback_verified=True))
        return target

    def dataset(self,name,venue,day,data,schema):
        return self.write(f'{ROOT}/{name}/venue={venue}/date={day}/part-core.parquet',pa.Table.from_pylist(data,schema=schema))

    def dated(self,name,venue,default_day,data,schema,clock='available_at_ns'):
        partitions=defaultdict(list)
        for row in data:
            day=time.strftime('%Y-%m-%d',time.gmtime(row[clock]//10**9))
            partitions[day].append(row)
        if not partitions:partitions[default_day]=[]
        return [self.dataset(name,venue,day,items,schema) for day,items in sorted(partitions.items())]


def _files(source,dataset,venue=None):
    base=Path(source)/'v2'/dataset
    if venue:base/=f'venue={venue}'
    return sorted(base.rglob('*.parquet'))


def _read(source,dataset,venue=None):
    for file in _files(source,dataset,venue):yield from rows(file)


def _as_table(rows_,schema):return pa.Table.from_pylist(list(rows_),schema=schema)


def reconnect_gaps(log):
    """Keep a conservative reconnect interval, not a complete asset gap tape."""
    gaps=[];affected={}
    for line in log.splitlines():
        connection=re.search(r'conn=(\d+)',line)
        if '[CONNECTION-DATA-GAP]' in line and connection:
            count=re.search(r'invalidated_assets=(\d+)',line)
            affected[connection[1]]=int(count[1]) if count else None
        if '[CONN-GAP]' not in line or connection is None:continue
        seconds=re.search(r'gap_seconds=([0-9.]+)',line)
        if seconds is None:continue
        stamp=line.split()[0];end=iso_to_ns(stamp)
        duration=int(Decimal(seconds[1])*10**9)
        gaps.append(dict(venue='polymarket',start_ns=end-duration,end_ns=end,kind='reported_reconnect_interval',
            reported_gap_seconds=seconds[1],affected_assets=affected.get(connection[1]),asset_scope_known=False))
    return gaps


def _project_archive(source,destination,*,window,source_gaps=()):
    """Project a closed v2 archive; verify before publishing the new manifest.

    The caller owns source retirement. This function never deletes inputs or
    invokes network services. It refuses to overwrite any output directory.
    """
    started=time.perf_counter();source=Path(source).resolve();destination=Path(destination).resolve()
    if destination==source or destination.is_relative_to(source):raise ValueError('distinct_output_root_required')
    if not (source/'v2/markets').is_dir() or not (source/'v2/market_observations').is_dir():
        raise ValueError('closed_native_archive_with_instruments_required')
    destination.mkdir(parents=True,exist_ok=True);out=Output(destination);source_files=[];source_inventory=[]
    for p in sorted(source.rglob('*')):
        if not p.is_file():continue
        if not p.resolve().is_relative_to(source):raise ValueError('source_file_path_escape')
        with p.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        source_inventory.append(dict(file=p.relative_to(source).as_posix(),sha256=sha))
    for p in source.rglob('*.parquet'):
        with p.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        source_files.append(dict(file=p.relative_to(source).as_posix(),sha256=sha,bytes=p.stat().st_size))
    # Objects are read only to extract allowlisted instrument/fee/order facts.
    # No object store or full raw payload is copied into the compact archive.
    objects={r['object_id']:exact(r['payload_json']) for r in _read(source,'objects')}
    markets_by_venue=defaultdict(list)
    for row in _read(source,'markets'):markets_by_venue[row['venue']].append(row)
    if not markets_by_venue:raise ValueError('empty_instrument_catalog')
    venues=sorted(markets_by_venue)
    assignments=list(_read(source,'market_observations'));known_markets=set();known_fees=set()
    meta_receipts=[];missing_fee=Counter();trade_proofs={};normal_quality_omitted=Counter()
    for venue in venues:
        day=time.strftime('%Y-%m-%d',time.gmtime(window['start_ns']//10**9))
        markets=[]
        for row in markets_by_venue[venue]:
            known_markets.add((venue,row['market_id'],row['market_version_id']))
            metadata=objects.get(row['extra_metadata_ref'],{})
            item={k:row.get(k) for k in ('venue','market_id','condition_id','question','description','market_version_id',
                'price_increment','minimum_order_size','price_decimals','is_neg_risk')}
            instrument_fields=('label','asset_id','index_set','book_origin','derived_from_asset_id')
            if venue=='myriad':instrument_fields+=('native_outcome_id','reported_closing_price','reported_closing_price_unit')
            item['instruments_json']=canonical([{k:o.get(k) for k in instrument_fields}
                for o in objects[row['instrument_set_ref']]])
            item['contract_metadata_json']=canonical(scrub({k:v for k,v in metadata.items() if k in META_FIELDS}))
            markets.append(item)
        from .writer import MARKET_SCHEMA,FEE_SCHEMA as OLD_FEE_SCHEMA
        schema=pa.schema([f for f in MARKET_SCHEMA if f.name in markets[0]]+[
            ('instruments_json',pa.string()),('contract_metadata_json',pa.large_string())]) if markets else pa.schema([])
        meta_receipts.append(out.dataset('instruments',venue,day,markets,schema))
        policies=[]
        for row in _read(source,'fees',venue):
            known_fees.add((venue,row['fee_version_id']))
            item={k:v for k,v in row.items() if k not in {'schema','extra_policy_ref'}};ref=row.get('extra_policy_ref')
            item['extra_policy_json']=canonical(scrub(objects[ref])) if ref else None;policies.append(item)
        schema=pa.schema([f for f in OLD_FEE_SCHEMA if f.name not in {'schema','extra_policy_ref'}]+[('extra_policy_json',pa.string())])
        meta_receipts.append(out.dataset('fee_policies',venue,day,policies,schema))
        assignments_=[dict(venue=venue,market_id=r['market_id'],market_version_id=r['market_version_id'],fee_version_id=r['fee_version_id'],
            available_at_ns=r['available_at_ns'],source_effective_at_ns=r['source_effective_at_ns'],sequence=seq(r['source_ref'])) for r in assignments if r['venue']==venue]
        schema=pa.schema([('venue',pa.string()),('market_id',pa.string()),('market_version_id',pa.string()),('fee_version_id',pa.string()),
            ('available_at_ns',pa.int64()),('source_effective_at_ns',pa.int64()),('sequence',pa.uint64())])
        for row in assignments_:
            if (venue,row['market_id'],row['market_version_id']) not in known_markets or (venue,row['fee_version_id']) not in known_fees:
                raise ValueError('unresolved_instrument_or_fee_assignment')
        meta_receipts.extend(out.dated('instrument_versions',venue,day,assignments_,schema))
        fee_rows=defaultdict(list)
        for fee in _read(source,'reported_fees',venue):
            if not actual_fee(fee):missing_fee[venue]+=1;continue
            fee_rows[fee['observation_id']].append(fee)
        receipts={};anomalies=[];coverage={}
        for row in _read(source,'quality',venue):
            detail=objects.get(row['detail_ref'],{});kind=row['quality_kind'];sequence=seq(row['source_ref'])
            if kind=='API_page_receipt':
                receipts[sequence]=dict(sequence=sequence,market_id=row['market_id'],available_at_ns=row['available_at_ns'],
                    requested_at_ns=detail.get('request_ns'),http_status=detail.get('status'),returned_rows=detail.get('rows',0))
                continue
            reasons=detail.get('reasons') or (detail.get('quality') or {}).get('reasons') or []
            quality=detail.get('quality') or detail
            if kind in {'trade_quality','trade_coverage','API_walk_quality'}:
                coverage[row['market_id']]=dict(available_at_ns=row['available_at_ns'],phase=detail.get('phase'),
                    acceptable=quality.get('acceptable'),pagination_complete=quality.get('pagination_complete'),reasons=reasons)
            bad=bool(reasons) or quality.get('acceptable') is False or any(k in kind for k in ('gap','cancelled','error'))
            if not bad:normal_quality_omitted[venue]+=1;continue
            anomalies.append(dict(market_id=row['market_id'],available_at_ns=row['available_at_ns'],sequence=sequence,kind=kind,
                reasons=[str(x) for x in reasons] or [kind],acceptable=quality.get('acceptable'),pagination_complete=quality.get('pagination_complete'),
                lower_ns=detail.get('lower_ns'),upper_ns=detail.get('upper_ns'),known_WS_missing_groups=quality.get('known_WS_missing_groups'),
                persistent_missing_groups=quality.get('persistent_missing_groups')))
        direct=(source/f'v2/compact_trades/venue={venue}/manifest.json').exists()
        stream=iter(_read(source,'api_trades',venue));current=next(stream,None);trades=CompactTrades(venue,objects)
        if direct:
            if current is not None or fee_rows or receipts:raise ValueError('mixed_direct_and_legacy_trade_sources')
            from .direct_trades import import_direct
            trade_proofs[venue]=import_direct(source,out,venue,day,known_markets)
        for sequence,receipt in sorted(receipts.items()):
            observations=[]
            while current is not None and seq(current['source_ref'])==sequence:
                if (venue,current['market_id'],current['source_market_version_id']) not in known_markets:
                    raise ValueError('unresolved_trade_instrument_version')
                observations.append(current);current=next(stream,None)
            if current is not None and seq(current['source_ref'])<sequence:raise ValueError('unmatched_API_observation')
            # Historical catalog parts were sorted by observation SHA within
            # one received page. Restore the original API row order explicitly.
            observations.sort(key=lambda row:row['source_row_index'])
            trades.page(receipt,observations,fee_rows)
        if current is not None or fee_rows:raise ValueError('unmatched_trade_or_fee_observations')
        data={}
        for name,value,schema in [('trade_content',trades.payloads,TRADE_SCHEMA),('trade_pages',trades.pages,PAGE_SCHEMA),
                                  ('trade_page_content',trades.contents,PAGE_CONTENT_SCHEMA),('trade_fees',trades.fees,FEE_SCHEMA)]:
            if direct:continue
            files=out.dated(name,venue,day,value,schema) if name=='trade_pages' else [out.dataset(name,venue,day,value,schema)]
            data[name]=[]
            for file in files:
                with pq.ParquetFile(file,page_checksum_verification=True) as reader:data[name].extend(reader.read().to_pylist())
            if name=='trade_pages':data[name].sort(key=lambda r:r['sequence'])
        if not direct:trade_proofs[venue]=trades.verify(data['trade_pages'],data['trade_content'],data['trade_fees'],data['trade_page_content'])
        trade_proofs[venue]['empty_fee_rows_omitted']=missing_fee[venue]
        out.dated('quality',venue,day,anomalies,QUALITY_SCHEMA)
        atomic(destination/f'{ROOT}/coverage-{venue}.json',coverage)
    # Native event order/depth/precision are preserved. Predict raw extras and
    # generic patches are removed; instrument/fee changes remain in versions.
    for p in _files(source,'events'):
        rel=p.relative_to(source).as_posix().replace('v2/','v3/',1)
        with pq.ParquetFile(p,page_checksum_verification=True) as reader:t=reader.read()
        if 'venue=predictfun' in rel or 'venue=myriad' in rel:
            if 'patch_ref' in t.column_names:
                patches=[]
                for ref in t['patch_ref'].to_pylist():
                    patch=objects[ref] if ref else {};patch=patch.get('market',patch) if isinstance(patch,dict) else {}
                    selected={k:v for k,v in patch.items() if k in PATCH_FIELDS} if isinstance(patch,dict) else {}
                    patches.append(canonical(scrub(selected)) if selected else None)
                t=t.append_column('instrument_patch_json',pa.array(patches,type=pa.string()))
            t=t.drop([c for c in ('extra_ref','patch_ref') if c in t.column_names])
        out.write(rel,t)
    pair_count=0
    for path in (source/'v2/pairs').rglob('relation-*.json'):
        old=exact(path.read_text());fields=('pair_id','members','actual_started_at_ns','start_date_timezone','available_at_ns','admitted_at_ns','task_type',
            'market_versions','fee_versions','outcome_bindings','atomic_cross_venue_snapshot_certified','payoff_equivalence_certified')
        pair={k:old[k] for k in fields if k in old};pair['schema']='backtest-pair/1'
        for member in pair['members']:
            venue,mid=member.split(':',1)
            if (venue,mid,pair['market_versions'][member]) not in known_markets or (venue,pair['fee_versions'][member]) not in known_fees:
                raise ValueError('unresolved_Pair_instrument_or_fee_reference')
        relative=path.relative_to(source).parent.as_posix().replace('v2/','v3/',1)+f'/r-{pair_count:08x}.json'
        write_pair_json(destination/relative,pair);pair_count+=1
    group_count=0
    for path in sorted((source/'v2/groups').rglob('*.json')):
        group=json.loads(path.read_text())
        for member in group['members']:
            venue,mid=member.split(':',1)
            if (venue,mid,group['market_versions'][member]) not in known_markets or (venue,group['fee_versions'][member]) not in known_fees:
                raise ValueError('unresolved_group_instrument_or_fee_reference')
        relative=path.relative_to(source).as_posix().replace('v2/','v3/',1)
        target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
        atomic(target,group);group_count+=1
    if source_gaps:
        day=time.strftime('%Y-%m-%d',time.gmtime(window['start_ns']//10**9))
        for venue in venues:
            selected=[{k:r.get(k) for k in GAP_SCHEMA.names} for r in source_gaps if r['venue']==venue]
            if selected:out.dataset('gaps',venue,day,selected,GAP_SCHEMA)
    for receipt in source_inventory:
        with (source/receipt['file']).open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        if sha!=receipt['sha256']:raise ValueError('source_changed_during_compaction')
    auxiliary=[]
    for p in sorted((destination/ROOT).rglob('*.json')):
        with p.open('rb') as stream:h=hashlib.file_digest(stream,'sha256').hexdigest()
        auxiliary.append(dict(file=p.relative_to(destination).as_posix(),sha256=h))
    proof=dict(schema=PROFILE,window=window,created_at_ns=time.time_ns(),files=out.receipts,auxiliary_files=auxiliary,pair_relations=pair_count,group_relations=group_count,
        trades=trade_proofs,empty_fee_rows_omitted=dict(missing_fee),normal_diagnostics_omitted=dict(normal_quality_omitted),
        dropped_information=['participant_wallets','signers','profile_fields','raw_objects','full_HTTP_headers_and_requests',
            'per_observation_SHA_identifiers','empty_fee_placeholder_rows','verbose_success_diagnostics'],
        fee_amount_missing_means='not_reported_not_zero',content_id_is_not_a_venue_fill_id=True,
        content_reference_scope='this_archive_manifest_and_venue',
        ordered_occurrences_and_revisions_preserved=True,all_financial_roundtrips_verified=True,
        full_source_coverage_certified=False,source_files=source_files,source_inventory=source_inventory,source_bytes=sum(x['bytes'] for x in source_files),
        parquet_bytes=sum(x['bytes'] for x in out.receipts),seconds=time.perf_counter()-started)
    proof['reduction_percent']=100*(1-proof['parquet_bytes']/proof['source_bytes'])
    atomic(destination/'manifest.json',proof)
    return proof


def source_signature(source):
    source=Path(source).resolve();inventory=[]
    for p in sorted(source.rglob('*')):
        if not p.is_file():continue
        if not p.resolve().is_relative_to(source):raise ValueError('source_file_path_escape')
        relative=p.relative_to(source)
        datasets={'events','objects','markets','fees','market_observations','reported_fees','api_trades','quality','pairs','groups','compact_trades','_control'}
        ordinary=len(relative.parts)>=3 and relative.parts[0]=='v2' and relative.parts[1] in datasets-{'_control'} and p.suffix in {'.parquet','.json'}
        lock=len(relative.parts)==3 and relative.parts[:2]==('v2','_control') and p.suffix=='.lock' and p.stem in datasets-{'events','pairs','_control'}
        if not (ordinary or lock):raise ValueError('unexpected_archive_input_file_no_retirement_allowed')
        with p.open('rb') as stream:h=hashlib.file_digest(stream,'sha256').hexdigest()
        inventory.append((p.relative_to(source).as_posix(),h))
    return hashlib.sha256(canonical(inventory).encode()).hexdigest()


def verify_publication(destination,proof):
    if proof.get('schema')=='nautilus-source/2':
        from .storage_profile import verify_storage
        return verify_storage(destination,proof)
    destination=Path(destination).resolve()
    if proof.get('schema')!=PROFILE or not proof.get('all_financial_roundtrips_verified'):
        raise ValueError('verified_compact_receipt_required')
    for receipt in proof['files']+proof.get('auxiliary_files',[]):
        path=(destination/receipt['file']).resolve()
        if not path.is_relative_to(destination):raise ValueError('compact_file_path_escape')
        with path.open('rb') as stream:h=hashlib.file_digest(stream,'sha256').hexdigest()
        if h!=receipt['sha256']:raise ValueError('compact_payload_changed_before_retirement')


def compact_archive(source,destination,*,window,source_gaps=()):
    """Transactional publication and same-input retry of this owned projection."""
    source=Path(source).resolve();destination=Path(destination).resolve()
    if destination==source or destination.is_relative_to(source):raise ValueError('distinct_output_root_required')
    if not source.exists() and (destination/'source-retirement-intent.json').exists():
        intent=json.loads((destination/'source-retirement-intent.json').read_text())
        if intent.get('source')!=str(source) or not intent.get('verified'):raise ValueError('retired_source_ownership_mismatch')
        proof=json.loads((destination/'manifest.json').read_text());verify_publication(destination,proof)
        if proof['window']!=window:raise ValueError('published_projection_input_changed')
        return proof
    if not (source/'v2/markets').is_dir():raise ValueError('closed_native_archive_with_instruments_required')
    signature=source_signature(source)
    if destination.exists():
        path=destination/'manifest.json'
        if not path.exists():raise ValueError('unowned_incomplete_destination')
        proof=json.loads(path.read_text())
        if proof.get('source_signature')!=signature or proof['window']!=window:raise ValueError('published_projection_input_changed')
        verify_publication(destination,proof);return proof
    work=destination.with_name('.c-'+hashlib.sha256(destination.name.encode()).hexdigest()[:10])
    intent=dict(profile=PROFILE,source=str(source),source_signature=signature,window=window)
    if work.exists():
        marker=work/'intent.json'
        if not marker.exists() or json.loads(marker.read_text())!=intent:raise ValueError('incomplete_projection_ownership_mismatch')
        # Only our tagged derived work directory is replaceable. Source inputs
        # still exist and their full signature matches the recorded intent.
        if work==source or not work.is_relative_to(destination.parent):raise ValueError('unsafe_projection_work_path')
        shutil.rmtree(work)
    work.mkdir(parents=True);atomic(work/'intent.json',intent)
    proof=_project_archive(source,work,window=window,source_gaps=source_gaps)
    proof['source_signature']=signature
    atomic(work/'manifest.json',proof);verify_publication(work,proof)
    (work/'intent.json').unlink();work.rename(destination)
    return proof


def retire_staging(source,destination,proof,*,allowed_parent):
    """Guarded retirement only of the exact verified converter input directory."""
    source=Path(source).resolve();destination=Path(destination).resolve();parent=Path(allowed_parent).resolve()
    if source==parent or not source.is_relative_to(parent) or not destination.is_relative_to(parent) or destination.is_relative_to(source):
        raise ValueError('retirement_path_outside_owned_run')
    saved=json.loads((destination/'manifest.json').read_text())
    if saved!=proof or not (proof.get('all_financial_roundtrips_verified') or proof.get('all_retained_financial_roundtrips_verified')):raise ValueError('verified_compact_receipt_required')
    verify_publication(destination,proof)
    if not source.exists():
        marker=destination/'source-retirement-intent.json'
        intent=json.loads(marker.read_text()) if marker.exists() else {}
        if intent.get('source')!=str(source) or not intent.get('verified'):raise ValueError('retired_source_ownership_mismatch')
        atomic(destination/'source-retirement.json',dict(at_ns=time.time_ns(),verified_source_directory=str(source),
            verified_destination=str(destination),status='verified_retirement_recovered_without_source_reaudit'))
        return
    inventory={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()}
    if inventory!={r['file'] for r in proof['source_inventory']}:raise ValueError('source_inventory_changed_before_retirement')
    if proof.get('source_signature') and source_signature(source)!=proof['source_signature']:
        raise ValueError('source_changed_before_retirement')
    for receipt in proof['source_inventory']:
        file=source/receipt['file']
        if not file.resolve().is_relative_to(source):raise ValueError('source_file_path_escape')
        with file.open('rb') as stream:h=hashlib.file_digest(stream,'sha256').hexdigest()
        if h!=receipt['sha256']:raise ValueError('source_changed_before_retirement')
    # Both absolute paths and every payload hash were checked above. No volumes,
    # old runtime, credentials, or other experiment folders are addressed here.
    atomic(destination/'source-retirement-intent.json',dict(at_ns=time.time_ns(),source=str(source),verified=True))
    shutil.rmtree(source)
    atomic(destination/'source-retirement.json',dict(at_ns=time.time_ns(),verified_source_directory=str(source),
        verified_destination=str(destination),source_Parquet_files=len(proof['source_files']),status='verified_legacy_projection_retired'))


def finish_publication(root,proof):
    """Ready receipt precedes unlink; a crash after unlink needs no native audit."""
    root=Path(root).resolve();destination=Path(proof['archive_path']).resolve()
    if destination!=root/'archive':raise ValueError('unexpected_final_archive_path')
    atomic(root/'finalization-ready.json',proof)
    retire_staging(root/'native-staging',destination,proof['compact_projection'],allowed_parent=root)
    now=time.time_ns();final=dict(proof,final_at_ns=now,legacy_Parquet_projection_retired=True)
    final['record_end_to_final_seconds']=(now-final['window']['end_ns'])/1e9
    atomic(root/'final.json',final)
    return final


def recover_publication(root):
    root=Path(root).resolve();ready=root/'finalization-ready.json'
    if not ready.exists():return None
    proof=json.loads(ready.read_text());proof['recovered_from_verified_publication']=True
    return finish_publication(root,proof)
