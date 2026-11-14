"""Offline instrument and fee evidence, with as-of versions and exact decimals."""
import json
from collections import Counter
from decimal import Decimal, localcontext
from pathlib import Path
from bisect import bisect_right
import pyarrow as pa
import pyarrow.parquet as pq
from native_archive.catalog import market_version, version_observation
from native_archive.fees import canonical, digest
from mixed_recorder.publication import bound_file, sha_file


def fee_policy(venue, raw):
    policy={k:v for k,v in raw.items() if 'fee' in k.lower()}
    result=dict(venue=venue, native_policy=policy, status='unknown', model=None,
                actual_paid_fee_certified=False, discounts_and_rebates_included=False)
    if raw.get('feesEnabled') is False:
        result.update(status='explicitly_disabled',model='zero')
    elif venue=='polymarket':
        s=raw.get('feeSchedule')
        if isinstance(s,dict) and s.get('rate') is not None:
            # Preserve the reported exponent. No category/default fee is substituted.
            result.update(status='reported_schedule',model='poly_schedule',rate=str(s['rate']),
                          exponent=str(s['exponent']) if s.get('exponent') is not None else None,
                          taker_only=s.get('takerOnly'))
    elif venue=='predictfun' and raw.get('feeRateBps') is not None:
        result.update(status='reported_bps',model='predict_min_price',taker_bps=str(raw['feeRateBps']),
                      maker_bps=str(raw['makerFeeBps']) if raw.get('makerFeeBps') is not None else None)
    elif venue=='myriad' and isinstance(raw.get('fees'),dict):
        f=raw['fees']; result.update(status='reported_bucket_schedule',model='myriad_buckets',
            maker_bps=str(f['maker_fee_bps']) if f.get('maker_fee_bps') is not None else None,
            taker_buckets=f.get('taker_fee_bps_array'))
    return result


def estimate_fee(policy, price, shares, *, role='taker', poly_model=None):
    """Unrounded cash-equivalent estimate. Never an actually paid token charge.

    Poly caller must select documented model explicitly: archived schedule
    parameters alone cannot distinguish historical fee-formula migrations.
    """
    if role not in ('maker','taker'): raise ValueError('fee_role_required')
    if any(isinstance(x,(float,bool)) for x in (price,shares)): raise ValueError('exact_fee_inputs_required')
    with localcontext() as c:
        c.prec=100; p,q=Decimal(price),Decimal(shares)
        if not p.is_finite() or not q.is_finite() or not 0<p<1 or q<0: raise ValueError('invalid_fee_inputs')
        model=policy['model']
        if model=='zero': return Decimal(0)
        if model=='poly_schedule':
            if role=='maker' and policy['taker_only'] is True:return Decimal(0)
            if role=='maker':raise ValueError('maker_policy_unconfirmed')
            rate=Decimal(policy['rate']); exponent=policy['exponent']
            if poly_model=='current_quadratic' and exponent=='1': return q*rate*p*(1-p)
            if poly_model=='legacy_price_weighted' and exponent is not None:
                n=Decimal(exponent)
                if n==n.to_integral_value() and 0<=n<=16:return q*p*rate*(p*(1-p))**int(n)
            raise ValueError('explicit_poly_formula_version_required')
        if model=='predict_min_price':
            bps=policy['maker_bps'] if role=='maker' else policy['taker_bps']
            if bps is None:raise ValueError('maker_policy_unconfirmed')
            if role=='maker' and Decimal(bps)!=0:raise ValueError('nonzero_maker_formula_unconfirmed')
            return q*Decimal(bps)/10000*min(p,1-p)
        if model=='myriad_buckets':
            if role=='maker':
                if policy['maker_bps'] is None: raise ValueError('maker_policy_unconfirmed')
                bps=Decimal(policy['maker_bps'])
                if bps!=0:raise ValueError('nonzero_maker_formula_unconfirmed')
            else:
                i=p*100; buckets=policy['taker_buckets']
                if i!=i.to_integral_value() or not isinstance(buckets,list) or len(buckets)!=100:
                    raise ValueError('exact_cent_bucket_required')
                bps=Decimal(buckets[int(i)-1])
            return q*p*bps/10000
        raise ValueError('fee_policy_unavailable')


def rows(root):
    for folder in sorted(Path(root).glob('segment-*')):
        for f in sorted(folder.glob('accept_*.parquet')):
            with pq.ParquetFile(f,page_checksum_verification=True) as reader:
                for batch in reader.iter_batches(batch_size=2048):yield from batch.to_pylist()


def build(root, output):
    """Produce immutable definitions + complete availability observations locally."""
    root,output=Path(root),Path(output); output.mkdir(parents=True,exist_ok=False)
    source_manifest=json.loads((root/'manifest.json').read_bytes())
    if source_manifest.get('schema')!='local-backtest-source/1' or not source_manifest.get('lossless_native_roundtrip'):
        raise ValueError('verified_local_source_required')
    for segment in source_manifest['segments']:
        for f in segment['files']:
            # First Windows source manifests used native separators; normalize
            # only local relative names, then apply the existing path guard.
            path=bound_file(root,f['file'].replace('\\','/'))
            if path.stat().st_size!=f['bytes'] or sha_file(path)!=f['sha256']:raise ValueError('local_source_file_changed')
    versions={}; fees={}; fee_versions={}; objects={}; observations=[]; instruments={}; latest={}; explicit_free={}
    for r in rows(root):
        if r['event_type']!='market_metadata':continue
        e=json.loads(r['data'],parse_float=Decimal)
        if e.get('http_status')!=200:continue
        raw=json.loads(e['raw_payload'],parse_float=Decimal);raw=raw.get('data',raw)
        v=r['venue'];mid=r['market_id'];ref='accept_extra_v3:'+str(r['sequence'])
        market,fee=market_version(v,raw,observed_at_ns=r['received_ns'],source_ref=ref)
        obs=version_observation(market,fee); observations.append(obs)
        for o in market.pop('_objects')+fee.pop('_objects'):objects[o['object_id']]=o
        versions[market['market_version_id']]=market
        fee_versions[fee['fee_version_id']]=fee
        policy=fee_policy(v,raw); pid=digest(policy);fees[pid]=policy
        latest[(v,mid)]=dict(fee_policy=policy,question=market['question'])
        if v=='polymarket' and policy['status']=='explicitly_disabled':
            explicit_free[mid]=dict(market_id=mid,question=market['question'],feesEnabled=False,
                                   source_ref=ref,available_at_ns=r['received_ns'])
        definitions=json.loads(objects[market['instrument_set_ref']]['payload_json'])
        for d in definitions:
            asset=d['asset_id']; key=digest([v,mid,asset,market['market_version_id'],pid])
            tick=market['price_increment']
            token=raw.get('token') or {}
            instruments[key]=dict(instrument_id=v+'-'+digest([v,mid,asset])[:32],venue=v,market_id=mid,
                asset_id=asset,label=d.get('label'),condition_id=market['condition_id'],
                source_instrument=('predictfun:'+mid+':indexSet=1' if v=='predictfun' and d.get('index_set')==1
                                   else 'myriad:'+mid+':'+str(d['native_outcome_id']) if v=='myriad' else asset),
                book_origin=d['book_origin'],derived_from_asset_id=d.get('derived_from_asset_id'),
                market_version_id=market['market_version_id'],fee_version_id=fee['fee_version_id'],fee_policy_id=pid,
                price_increment=tick,price_precision=(-Decimal(tick).normalize().as_tuple().exponent if tick else None),
                declared_decimal_precision=market['price_decimals'],
                tick_basis='market_metadata' if tick else 'not_reported',
                documented_order_tick='0.01' if v=='myriad' else None,
                amount_wire_decimals=token.get('decimals') if v=='myriad' else None,
                quote_currency=token.get('symbol') or raw.get('quoteCurrency'),
                quote_token_address=token.get('address') or raw.get('collateralTokenAddress'),
                minimum_order_size=market['minimum_order_size'],size_increment=None,
                venue_precision_is_not_size_increment=True)
    # Aggregate observed precision only. Never infer an exchange lot/tick from a sample.
    precision={}; mapped=Counter(); unmapped=Counter(); temporal_missing=Counter(); bindings={}
    by_source={}
    for i in instruments.values():
        if i['book_origin']=='native':by_source[(i['venue'],i['source_instrument'])]=i
        bindings[(i['venue'],i['source_instrument'],i['market_version_id'],i['fee_version_id'])]=i
    timelines={}
    for obs in observations:timelines.setdefault((obs['venue'],obs['market_id']),[]).append(obs)
    for values in timelines.values():values.sort(key=lambda x:x['available_at_ns'])
    with pq.ParquetFile(root/'books.parquet',page_checksum_verification=True) as pf:
        for batch in pf.iter_batches(batch_size=2048):
            for row in batch.to_pylist():
                k=(row['venue'],row['instrument']); item=by_source.get(k)
                if item: mapped[row['venue']]+=1
                else:unmapped[row['venue']]+=1
                if item:
                    timeline=timelines[(item['venue'],item['market_id'])]
                    idx=bisect_right([o['available_at_ns'] for o in timeline],row['received_ns'])-1
                    if idx<0 or (item['venue'],item['source_instrument'],timeline[idx]['market_version_id'],timeline[idx]['fee_version_id']) not in bindings:
                        temporal_missing[row['venue']]+=1
                p=precision.setdefault(k,dict(venue=k[0],source_instrument=k[1],price_decimals=0,size_decimals=0,rows=0))
                p['rows']+=1
                for level in row['bids']+row['asks']:
                    for name in ('price','size'):
                        p[name+'_decimals']=max(p[name+'_decimals'],max(0,-Decimal(level[name]).as_tuple().exponent))
    for name,values in [('market_versions',versions.values()),('fee_versions',fee_versions.values()),('objects',objects.values()),('fee_policies',fees.values()),
                        ('observations',observations),('instruments',instruments.values()),('observed_precision',precision.values())]:
        with (output/(name+'.jsonl')).open('w',encoding='utf8',newline='\n') as stream:
            for value in values:stream.write(canonical(value)+'\n')
    # Typed lookup table; nulls remain null. Full definitions and provenance above.
    if instruments:
        schema=pa.schema([(k,pa.bool_() if k=='venue_precision_is_not_size_increment' else pa.int32() if k in
            ('price_precision','declared_decimal_precision','amount_wire_decimals') else pa.string()) for k in next(iter(instruments.values()))])
        pq.write_table(pa.Table.from_pylist(list(instruments.values()),schema=schema),output/'instruments.parquet',compression='zstd',compression_level=9,write_page_checksum=True)
        with pq.ParquetFile(output/'instruments.parquet',page_checksum_verification=True) as check:
            restored=check.read().to_pylist()
        if restored!=list(instruments.values()): raise ValueError('instrument_parquet_readback_changed')
    summary=dict(schema='local-instrument-adaptation/1',markets_by_venue=dict(Counter(v for v,m in latest)),
        definitions=len(instruments),observations=len(observations),mapped_book_operations=dict(mapped),unmapped_book_operations=dict(unmapped),
        book_operations_missing_asof_definition=dict(temporal_missing),
        fee_statuses=dict(Counter(v+':'+x['fee_policy']['status'] for (v,m),x in latest.items())),
        poly_previously_missing_resolved=list(explicit_free.values()),
        unresolved_constraints=['Predict decimalPrecision is not a certified tick; size increment absent.',
           'Myriad documented order tick is separate from observed market tick; currency identity comes from saved token metadata.',
           'Poly fee formula version must be explicitly selected; legacy base fee fields never become bps.',
           'Missing quote currency and lot size remain null; explicit engine configuration required before order simulation.'],
        source_data_only=True,all_instruments_execution_ready=False)
    (output/'report.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf8')
    import hashlib
    proof={p.name:dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in output.iterdir() if p.is_file()}
    (output/'manifest.json').write_text(json.dumps(dict(schema='local-instrument-files/1',files=proof,
        source_manifest_sha256=sha_file(root/'manifest.json'),books_sha256=sha_file(root/'books.parquet')),indent=2),encoding='utf8')
    return summary
