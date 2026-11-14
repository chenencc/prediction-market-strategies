"""Normalize distinct native API rows once, retaining every page occurrence.

Archive-only: no provider client, credentials, or capture mutation. The output
uses the existing compact schemas and financial replay, not a second format.
"""
from collections import OrderedDict,Counter
import hashlib,json
from pathlib import Path
import pyarrow as pa
from marketdata.core import exact,atomic
from .trades import api_observation
from .backtest_profile import (CompactTrades,Output,TRADE_SCHEMA,PAGE_SCHEMA,
    PAGE_CONTENT_SCHEMA,FEE_SCHEMA,PAYLOAD_FIELDS,FEE_FIELDS,financial_legs,actual_fee,scrub)
from .fees import canonical


class DirectTrades:
    def __init__(self,venue,window,cache_pages=4096,*,metadata_by_market=None):
        self.venue=venue;self.window=window;self.trades=CompactTrades(venue,{})
        self.cache_pages=cache_pages;self.page_cache=OrderedDict();self.row_cache={}
        self.normalized_rows=0;self.inside=self.outside=self.unknown=0
        self.content_hashes={};self.source_digest=hashlib.sha256()
        self.page_problems=Counter();self.success_decode_failures=0;self.last_problem=None
        self.metadata_by_market=metadata_by_market or {}

    def _payload(self,raw,mid):
        key=canonical([mid,raw]);cached=self.row_cache.get(key)
        if cached is not None:return cached
        args=dict(market_metadata=self.metadata_by_market.get(mid)) if self.venue=='myriad' else {}
        row,fees=api_observation(self.venue,raw,market_id=mid,available_at_ns=1,
            source_ref='direct:0:0',page_sha256='0'*64,row_index=0,market_version_id='direct',
            window_start_ns=self.window['start_ns'],window_end_ns=self.window['end_ns'],**args)
        objects={o['object_id']:exact(o['payload_json']) for o in row['_objects']}
        charge=[]
        for fee in fees:
            for obj in fee.get('_objects',[]):objects[obj['object_id']]=exact(obj['payload_json'])
            if not actual_fee(fee):continue
            item={k:fee.get(k) for k in FEE_FIELDS};ref=fee.get('extra_fee_ref')
            item['extra_fee_json']=canonical(scrub(objects[ref])) if ref else None;charge.append(item)
        t=self.trades;fee_id=None
        if charge:
            fk=canonical(charge);fee_id=t._fees.get(fk)
            if fee_id is None:
                fee_id=len(t.fees);t._fees[fk]=fee_id;t.fees.append(dict(fee_bundle_id=fee_id,legs=charge))
        payload={k:row.get(k) for k in PAYLOAD_FIELDS}
        payload.update(fee_bundle_id=fee_id,legs=financial_legs(row,objects))
        pk=canonical(payload);ident=t._payload.get(pk)
        if ident is None:
            ident=len(t.payloads);t._payload[pk]=ident;t.payloads.append(dict(payload_id=ident,**payload))
        self.row_cache[key]=ident;self.normalized_rows+=1
        return ident

    def page(self,raw,*,market_id,sequence,available_at_ns,requested_at_ns,http_status,version):
        mid=str(market_id);key=(mid,hashlib.sha256(raw.encode()).digest(),http_status)
        cached=self.page_cache.get(key)
        if cached is None:
            problem=None;rows=[]
            if http_status!=200:problem='non_success_HTTP_response'
            else:
                try:body=exact(raw)
                except (ValueError,TypeError):problem='successful_HTTP_non_JSON_body'
                else:
                    if self.venue=='myriad' and isinstance(body,list):rows=body
                    elif not isinstance(body,dict) or not isinstance(body.get('data'),list):
                        problem='successful_HTTP_invalid_data_envelope'
                    else:rows=body['data']
            ids=tuple(self._payload(row,mid) for row in rows)
            counts=[0,0,0]
            for ident in ids:
                membership=self.trades.payloads[ident]['window_membership']
                counts[0 if membership in {'inside','boundary_candidate'} else 2 if membership=='unknown' else 1]+=1
            cached=(ids,counts,problem);self.page_cache[key]=cached
            if len(self.page_cache)>self.cache_pages:self.page_cache.popitem(last=False)
        else:self.page_cache.move_to_end(key)
        ids,counts,problem=cached;self.last_problem=None
        if problem:
            self.page_problems[problem]+=1;self.success_decode_failures+=int(http_status==200)
            self.last_problem=dict(reason=problem,reasons=[problem],http_status=http_status,body_sha256=key[1].hex(),
                body_bytes=len(raw.encode()),source_sequence=sequence,
                response_rows_unknown=http_status==200,acceptable=False)
        self.inside+=counts[0];self.outside+=counts[1];self.unknown+=counts[2]
        t=self.trades;content_key=tuple(enumerate(ids));content_id=t._content.get(content_key)
        if content_id is None:
            content_id=len(t.contents);t._content[content_key]=content_id
            t.contents.append(dict(content_id=content_id,occurrences=[dict(row_index=i,payload_id=p) for i,p in content_key]))
            self.content_hashes[content_id]=content_digest(t.contents[-1],t.payloads,t.fees)
        page=dict(sequence=sequence,market_id=mid,available_at_ns=available_at_ns,requested_at_ns=requested_at_ns,
            source_market_version_id=version,http_status=http_status,returned_rows=len(ids),content_id=content_id)
        for ident in ids:
            payload=t.payloads[ident];t.fee_observations+=len(t.fees[payload['fee_bundle_id']]['legs']) if payload['fee_bundle_id'] is not None else 0
        page_digest(self.source_digest,page,self.content_hashes[content_id])
        t.observations+=len(ids);t.pages.append(page)
        return len(ids)

    def close(self,root):
        out=Output(root);t=self.trades
        for name,rows,schema in [('trade_content',t.payloads,TRADE_SCHEMA),('trade_pages',t.pages,PAGE_SCHEMA),
            ('trade_page_content',t.contents,PAGE_CONTENT_SCHEMA),('trade_fees',t.fees,FEE_SCHEMA)]:
            out.write(f'v2/compact_trades/venue={self.venue}/{name}.parquet',pa.Table.from_pylist(rows,schema=schema))
        proof=dict(observations=t.observations,reported_fee_legs=t.fee_observations,pages=len(t.pages),
            payload_versions=len(t.payloads),fee_bundles=len(t.fees),page_contents=len(t.contents),
            all_financial_fields_and_occurrences_verified=True)
        proof.update(files=out.receipts,venue=self.venue,normalized_distinct_rows=self.normalized_rows,
            ordered_content_sha256=self.source_digest.hexdigest(),digest_format='ordered-page-and-financial-content/1',
            schema='direct-native-trades/1',all_received_occurrences_preserved=self.success_decode_failures==0,
            all_success_response_rows_decoded=self.success_decode_failures==0,
            page_response_problems=dict(self.page_problems),successful_response_decode_failures=self.success_decode_failures,
            financial_replay_scope='decoded_successful_response_rows_only')
        atomic(Path(root)/f'v2/compact_trades/venue={self.venue}/manifest.json',proof)
        return proof


def content_digest(content,payloads,fees):
    h=hashlib.sha256()
    for o in content['occurrences']:
        p=payloads[o['payload_id']];charge=fees[p['fee_bundle_id']]['legs'] if p['fee_bundle_id'] is not None else []
        h.update(canonical([o['row_index'],{k:p[k] for k in PAYLOAD_FIELDS},charge]).encode());h.update(b'\n')
    return h.hexdigest()


def page_digest(h,page,content_hash):
    h.update(canonical([page[k] for k in PAGE_SCHEMA.names if k!='content_id']+[content_hash]).encode());h.update(b'\n')


def import_direct(source,out,venue,day,known_markets):
    """Read back direct parts and verify the existing ordered financial digest."""
    import pyarrow.parquet as pq
    root=Path(source);folder=root/f'v2/compact_trades/venue={venue}'
    proof=json.loads((folder/'manifest.json').read_text());data={}
    for entry in proof['files']:
        file=(root/entry['file']).resolve()
        if not file.is_relative_to(folder.resolve()):raise ValueError('direct_trade_path_escape')
        with file.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        if sha!=entry['sha256']:raise ValueError('direct_trade_source_changed')
        with pq.ParquetFile(file,page_checksum_verification=True) as reader:table=reader.read()
        name=file.stem;data[name]=table.to_pylist()
        if name=='trade_pages':
            for row in data[name]:
                if (venue,row['market_id'],row['source_market_version_id']) not in known_markets:
                    raise ValueError('unresolved_direct_trade_instrument_version')
        out.dataset(name,venue,day,data[name],table.schema)
    h=hashlib.sha256();hashes={};observations=fee_count=0
    for content in data['trade_page_content']:hashes[content['content_id']]=content_digest(content,data['trade_content'],data['trade_fees'])
    for page in data['trade_pages']:
        content=data['trade_page_content'][page['content_id']]
        if len(content['occurrences'])!=page['returned_rows']:raise ValueError('direct_page_row_count_changed')
        if [o['row_index'] for o in content['occurrences']]!=list(range(page['returned_rows'])):raise ValueError('direct_page_order_changed')
        page_digest(h,page,hashes[page['content_id']]);observations+=page['returned_rows']
        for occurrence in content['occurrences']:
            p=data['trade_content'][occurrence['payload_id']];fees=data['trade_fees'][p['fee_bundle_id']]['legs'] if p['fee_bundle_id'] is not None else []
            if p['market_id']!=page['market_id']:raise ValueError('direct_page_market_changed')
            fee_count+=len(fees)
    if h.hexdigest()!=proof['ordered_content_sha256'] or observations!=proof['observations'] or fee_count!=proof['reported_fee_legs']:
        raise ValueError('direct_financial_source_digest_changed')
    return {k:proof[k] for k in ('observations','reported_fee_legs','pages','payload_versions','fee_bundles','page_contents',
        'all_financial_fields_and_occurrences_verified','ordered_content_sha256','digest_format','normalized_distinct_rows')}|dict(
        direct_native_projection=True, page_response_problems=proof.get('page_response_problems',{}),
        successful_response_decode_failures=proof.get('successful_response_decode_failures',0),
        all_success_response_rows_decoded=proof.get('all_success_response_rows_decoded',True),
        financial_replay_scope=proof.get('financial_replay_scope','decoded_successful_response_rows_only'))
