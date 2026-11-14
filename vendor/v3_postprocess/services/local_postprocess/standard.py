"""Append-only local standard layer over the frozen file-spool input contract."""
import gzip
import hashlib
import json
import os
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from marketdata.core import exact
from marketdata.l2 import ExactL2
from native_archive.catalog import market_version, version_observation
from native_archive.fees import canonical, digest
from native_archive.trades import api_observation
from marketdata.poly_trade_policy import window as poly_window
from mixed_recorder.publication import sha_file
from mixed_recorder.store import durable_file, encoded, sync_directory
from pair_recorder.supervisor import OwnerLock
from .books import operations, LEVEL
from .instruments import fee_policy

COMMON=[('event_id',pa.string()),('sequence',pa.uint64()),('received_ns',pa.int64()),
        ('venue',pa.string()),('market_id',pa.string()),('connection_epoch',pa.string())]
SCHEMAS={
 'source_events':pa.schema(COMMON+[('event_type',pa.string()),('transport',pa.string()),('raw_sha256',pa.string()),('envelope_json',pa.large_string())]),
 'book_snapshots':pa.schema(COMMON+[('event_index',pa.uint32()),('instrument',pa.string()),('source_ns',pa.int64()),
     ('source_precision_ns',pa.int64()),('source_hash',pa.string()),('source_cursor_json',pa.string()),('origin',pa.string()),
     ('bids',pa.list_(LEVEL)),('asks',pa.list_(LEVEL)),('boundary',pa.string())]),
 'book_updates':pa.schema(COMMON+[('event_index',pa.uint32()),('instrument',pa.string()),('source_ns',pa.int64()),
     ('source_precision_ns',pa.int64()),('source_hash',pa.string()),('source_cursor_json',pa.string()),('origin',pa.string()),
     ('bids',pa.list_(LEVEL)),('asks',pa.list_(LEVEL)),('applied',pa.bool_())]),
 'observations':pa.schema(COMMON+[('kind',pa.string()),('detail_json',pa.large_string())]),
 'quotes':pa.schema(COMMON+[('event_index',pa.uint32()),('instrument',pa.string()),('source_ns',pa.int64()),('best_bid',pa.string()),('best_ask',pa.string())]),
 'definitions':pa.schema([('kind',pa.string()),('definition_id',pa.string()),('definition_json',pa.large_string())]),
 'trade_payloads':pa.schema([('payload_id',pa.string()),('venue',pa.string()),('market_id',pa.string()),
     ('execution_ns',pa.int64()),('source_precision_ns',pa.int64()),('price',pa.string()),('size',pa.string()),
     ('price_unit',pa.string()),('size_unit',pa.string()),('action',pa.string()),('detail_json',pa.large_string())]),
 'trade_occurrences':pa.schema(COMMON+[('row_index',pa.uint32()),('payload_id',pa.string()),('page_sha256',pa.string()),
     ('occurrence_rank',pa.uint32()),('window_membership',pa.string()),('market_version_id',pa.string())]),
}

class Tables:
    def __init__(self,root):self.root=Path(root);self.buffers={};self.writers={};self.counts=Counter()
    def put(self,name,row):
        # Small files are grouped by venue and UTC receive hour, not by Pair.
        stamp=row.get('received_ns');dayhour=datetime.fromtimestamp(stamp//10**9,timezone.utc).strftime('%Y-%m-%d/%H') if stamp is not None else 'shared'
        venue=row.get('venue','shared');key=(name,venue,dayhour)
        buf=self.buffers.setdefault(key,[]);buf.append(row);self.counts[name]+=1
        if len(buf)>=1024:self.flush(key)
    def flush(self,key):
        buf=self.buffers[key]
        if not buf:return
        if key not in self.writers:
            name,venue,dayhour=key;folder=self.root/name/('venue='+venue)/dayhour;folder.mkdir(parents=True,exist_ok=True)
            self.writers[key]=pq.ParquetWriter(folder/'part.parquet',SCHEMAS[name],compression='zstd',compression_level=9,write_page_checksum=True)
        self.writers[key].write_table(pa.Table.from_pylist(buf,schema=SCHEMAS[key[0]]));buf.clear()
    def close(self):
        try:
            for key in self.buffers:self.flush(key)
        finally:
            for w in self.writers.values():w.close()

class Semantic:
    def __init__(self,collection,state,tables):
        self.collection,self.state,self.out=collection,state,tables
        self.stats=Counter();self.unsupported=Counter();self.book=ExactL2()
        for instrument,b in state.get('books',{}).items():self.book.snapshot(instrument,b['epoch'],b['bids'],b['asks'])
    def definition(self,kind,ident,value):
        key=kind+':'+ident
        if key not in self.state['definitions']:
            self.out.put('definitions',dict(kind=kind,definition_id=ident,definition_json=canonical(value)))
            self.state['definitions'][key]=True
    def common(self,e):
        return dict(event_id=digest([self.collection,e['collector_sequence']]),sequence=e['collector_sequence'],
            received_ns=e['timestamp_received_ns'],venue=e['venue'],market_id=e['native_market_id'],connection_epoch=e['connection_epoch'])
    def observation(self,base,kind,value):self.out.put('observations',dict(base,kind=kind,detail_json=canonical(value)))
    def metadata(self,e,raw,base):
        m=raw.get('data',raw) if isinstance(raw,dict) else raw
        v=e['venue'];mid=str(e['native_market_id']);key=v+':'+mid
        if isinstance(m,list):
            matches=[x for x in m if isinstance(x,dict) and str(x.get('id'))==mid]
            if len(matches)!=1:raise ValueError('metadata_list_identity_unresolved')
            m=matches[0]
        if not isinstance(m,dict):raise ValueError('metadata_object_required')
        market,fee=market_version(v,m,observed_at_ns=base['received_ns'],source_ref=base['event_id'])
        if market['market_id']!=mid:raise ValueError('metadata_market_identity_changed')
        obs=version_observation(market,fee)
        for obj in market.pop('_objects')+fee.pop('_objects'):self.definition('object',obj['object_id'],obj)
        self.definition('market',market['market_version_id'],market);self.definition('fee',fee['fee_version_id'],fee)
        assets=m.get('clobTokenIds',[]) if v=='polymarket' else m.get('outcomes',[])
        assets=json.loads(assets) if isinstance(assets,str) else assets
        labels=m.get('outcomes',[]) if v=='polymarket' else []
        labels=json.loads(labels) if isinstance(labels,str) else labels
        for i,asset in enumerate(assets):
            ident=str(asset) if v=='polymarket' else str(asset.get('onChainId',asset.get('tokenId')))
            instrument=dict(venue=v,market_id=mid,native_asset_id=ident,
                label=labels[i] if v=='polymarket' else asset.get('name',asset.get('title')),
                native_outcome_id=asset.get('id') if v=='myriad' else None,
                index_set=asset.get('indexSet') if v=='predictfun' else None,
                book_origin='derived_complement' if v=='predictfun' and asset.get('indexSet')==2 else 'native',
                market_version_id=market['market_version_id'],price_increment=market['price_increment'],
                declared_decimal_precision=market['price_decimals'],minimum_order_size=market['minimum_order_size'],
                quote_token=m.get('token'),size_increment=None)
            self.definition('instrument',digest(instrument),instrument)
        policy=fee_policy(v,m);self.definition('fee_policy',digest(policy),policy)
        self.observation(base,'version_assignment',obs)
        history=self.state['metadata'].setdefault(key,[])
        if not history or history[-1]['version']!=market['market_version_id'] or history[-1]['fee']!=fee['fee_version_id']:
            history.append(dict(at=base['received_ns'],version=market['market_version_id'],fee=fee['fee_version_id'],raw=canonical(m)))
        if v=='polymarket':
            ids=m.get('clobTokenIds',[]);ids=json.loads(ids) if isinstance(ids,str) else ids
            for asset in ids:self.state['assets']['polymarket:'+str(asset)]=mid
            self.state['conditions'][str(m.get('conditionId'))]=mid
    def asof(self,venue,mid,at):
        values=[m for m in self.state['metadata'].get(venue+':'+str(mid),[]) if m['at']<=at]
        return max(values,key=lambda m:m['at']) if values else None
    def invalidate(self,base):
        # Connection transitions invalidate only their matching epoch; other
        # independent shards remain valid. Opening with no prior epoch is safe.
        epoch=base['connection_epoch'];prefix=base['venue']+':';keys=[]
        for key in self.book.books:
            if key.startswith(prefix) and (epoch is None or self.book.books[key]['epoch']==epoch):keys.append(key)
        self.book.invalidate(keys);self.observation(base,'connection_boundary',dict(invalidated=len(keys)))
    def book_event(self,e,base,n,index,origin='source'):
        v=e['venue'];kind=n.get('kind');ops=[];stamp=None;precision=None
        if v=='polymarket':
            mapped=self.state['assets'].get('polymarket:'+str(n.get('asset_id'))) or self.state['conditions'].get(str(n.get('market')))
            base=dict(base,market_id=mapped)
            ops=list(operations(n,v))
            if n.get('timestamp') is not None:
                d=Decimal(str(n['timestamp']))
                if d!=d.to_integral_value():raise ValueError('nonintegral_source_ms')
                stamp=int(d)*10**6;precision=10**6
        elif v=='predictfun':
            ops=list(operations(e,v));d=n.get('data',{});value=d.get('updateTimestampMs',d.get('tsMs'))
            if value is not None:
                d=Decimal(str(value))
                if d!=d.to_integral_value():raise ValueError('nonintegral_source_ms')
                stamp=int(d)*10**6;precision=10**6
        elif v=='myriad' and kind in ('snapshot','delta'):
            ops=[dict(instrument=n['instrument'],operation=kind,bids=n['bids'],asks=n['asks'])]
            base=dict(base,market_id=n['market_id']);stamp=n.get('source_timestamp_ms');stamp=int(stamp)*10**6 if stamp is not None else None
            precision=10**6 if stamp is not None else None
        for j,op in enumerate(ops):
            instrument=op.pop('instrument');operation=op.pop('operation');key=v+':'+instrument;epoch=e['connection_epoch']
            entry=dict(base,event_index=index*100000+j,instrument=instrument,source_ns=stamp,source_precision_ns=precision,
                source_hash=n.get('hash'),source_cursor_json=canonical(n.get('source_cursor')) if n.get('source_cursor') else None,origin=origin,**op)
            if operation=='snapshot':
                self.book.snapshot(key,epoch,op['bids'],op['asks'])
                self.out.put('book_snapshots',dict(entry,boundary=n.get('boundary','source_snapshot')))
            else:
                try:applied=self.book.delta(key,epoch,op['bids'],op['asks'])
                except ValueError:applied=False
                # ExactL2 returns a boolean acceptance boundary.
                self.out.put('book_updates',dict(entry,applied=bool(applied)))
                if not applied:self.stats['delta_without_valid_snapshot']+=1
            self.stats[v+':'+operation]+=1
            if base['market_id'] is None or self.asof(v,base['market_id'],base['received_ns']) is None:self.stats['book_missing_asof_metadata']+=1
    def trade_page(self,e,base,body):
        if e.get('http_status')!=200:
            self.observation(base,'trade_HTTP_failure',dict(status=e.get('http_status')));return
        v=e['venue'];mid=base['market_id'];meta=self.asof(v,mid,base['received_ns'])
        if meta is None:raise ValueError('trade_asof_metadata_required')
        if v=='myriad':items=body
        else:
            if v=='predictfun' and body.get('success') is not True:raise ValueError('predict_API_success_required')
            items=body.get('data')
        if not isinstance(items,list):raise ValueError('trade_array_required')
        h=hashlib.sha256(e['raw_payload'].encode()).hexdigest();seen=Counter();window=self.state.get('window')
        self.observation(base,'trade_page',dict(page_sha256=h,returned_rows=len(items),requested_at_ns=e.get('request_sent_ns'),
            http_status=e['http_status'],work_class=e.get('work_class'),request_params=e.get('request_params')))
        for i,item in enumerate(items):
            try:
                if v=='polymarket' and item.get('condition_id') and str(item['condition_id'])!=str(exact(meta['raw']).get('conditionId')):
                    raise ValueError('trade_condition_identity_changed')
                pid=digest([v,mid,item,meta['version']]);seen[pid]+=1
                cached=self.state['trade_payloads'].get(pid)
                if isinstance(cached,dict):
                    executed=cached['execution_ns'];membership='unknown'
                    if window and executed is not None:
                        if v=='polymarket':
                            inside,label=poly_window(executed,window['start_ns'],window['end_ns'])
                            membership=label if inside else 'outside'
                        else:membership='inside' if window['start_ns']<=executed<window['end_ns'] else 'outside'
                    self.out.put('trade_occurrences',dict(base,row_index=i,payload_id=pid,page_sha256=h,occurrence_rank=seen[pid],
                        window_membership=membership,market_version_id=meta['version']))
                    self.stats[v+':trade_occurrences']+=1;self.stats['cached_trade_payload_observations']+=1
                    continue
                row,fees=api_observation(v,item,market_id=mid,available_at_ns=base['received_ns'],source_ref=base['event_id'],
                    page_sha256=h,row_index=i,market_version_id=meta['version'],window_start_ns=window['start_ns'] if window else 0,
                    window_end_ns=window['end_ns'] if window and window['end_ns'] else 2**63-1,market_metadata=exact(meta['raw']))
                # Payload content identity shares bytes only; it is NOT a fill ID.
                membership=row['window_membership'] if window else 'unknown'
                for obj in row.pop('_objects'):self.definition('object',obj['object_id'],obj)
                for fee in fees:
                    for obj in fee.pop('_objects',[]):self.definition('object',obj['object_id'],obj)
                detail=dict(native=item,normalized=row,reported_fees=fees,unique_fill_certified=False)
                if pid not in self.state['trade_payloads']:
                    # Occurrence clocks/provenance live below, not in shared payloads.
                    for key in ('observation_id','available_at_ns','source_ref','source_page_sha256','source_row_index','window_membership'):
                        detail['normalized'].pop(key,None)
                    self.out.put('trade_payloads',dict(payload_id=pid,venue=v,market_id=mid,execution_ns=row['execution_ns'],
                        source_precision_ns=row['source_precision_ns'],price=row['price_native'],size=row['size_native'],
                        price_unit=row['price_unit'],size_unit=row['size_unit'],action=row['side'],detail_json=canonical(detail)))
                    self.state['trade_payloads'][pid]=dict(execution_ns=row['execution_ns'])
                self.out.put('trade_occurrences',dict(base,row_index=i,payload_id=pid,page_sha256=h,occurrence_rank=seen[pid],
                    window_membership=membership,market_version_id=meta['version']))
                self.stats[v+':trade_occurrences']+=1
            except (ValueError,KeyError,TypeError) as exc:self.observation(base,'unsupported_trade_row',dict(row_index=i,reason=str(exc)));self.stats['unsupported_trade_rows']+=1
    def event(self,e,line):
        base=self.common(e)
        extras={k:v for k,v in e.items() if k!='raw_payload'}
        self.out.put('source_events',dict(base,event_type=e['event_type'],transport=e['source_transport'],
            raw_sha256=hashlib.sha256(e['raw_payload'].encode()).hexdigest(),envelope_json=canonical(extras)))
        self.stats[e['venue']+':'+e['event_type']]+=1
        try:
            kind=e['event_type'];v=e['venue']
            raw=None if v=='myriad' and kind=='ws_frame' else exact(e['raw_payload']) if e['raw_payload'].strip().startswith(('{','[')) else None
            if kind=='market_metadata' and e.get('http_status')==200:self.metadata(e,raw,base)
            elif kind=='trade_page':self.trade_page(e,base,raw)
            elif kind in ('disconnect','connection_closed','connection_epoch','connection_opening','capture_closed'):
                self.invalidate(base)
            elif v=='polymarket' and kind=='ws_frame':
                frames=raw if isinstance(raw,list) else [raw]
                for i,n in enumerate(frames):
                    if not isinstance(n,dict):
                        self.observation(base,'poly:non_object_frame',dict(frame_index=i));continue
                    native=n.get('event_type')
                    if native in ('book','price_change'):self.book_event(e,base,n,i)
                    elif native=='best_bid_ask':
                        self.out.put('quotes',dict(base,market_id=self.state['assets'].get('polymarket:'+str(n['asset_id'])),
                            event_index=i,instrument=n['asset_id'],source_ns=int(n['timestamp'])*10**6,
                            best_bid=str(n['best_bid']) if n.get('best_bid') is not None else None,
                            best_ask=str(n['best_ask']) if n.get('best_ask') is not None else None))
                    else:self.observation(base,'poly:'+str(native),dict(frame_index=i,native=n))
            elif v=='predictfun' and kind=='orderbook_snapshot':self.book_event(e,base,raw,0)
            elif v=='myriad' and kind in ('ws_frame','http_response'):
                # Use the producer interpretation exactly once, retain original in source archive.
                for i,n in enumerate(e.get('normalized_events',[])):
                    if n['kind'] in ('snapshot','delta'):self.book_event(e,base,n,i)
                    elif n['kind']=='invalidate':self.invalidate(base)
                    else:self.observation(base,'myriad:'+n['kind'],n)
            elif kind.startswith('trade_') or kind in ('request_gap','lifecycle','admission_book','subscription_sent','ws_control'):
                self.observation(base,kind,dict(payload=raw,fields=extras))
            else:self.unsupported[v+':'+kind]+=1
        except (ValueError,KeyError,TypeError) as exc:
            self.stats['unsupported_events']+=1;self.observation(base,'unsupported_semantics',dict(reason=str(exc)))
    def checkpoint(self):
        from marketdata.l2 import decimal_text
        self.state['books']={k:dict(epoch=b['epoch'],bids=[dict(price=decimal_text(p),size=decimal_text(q)) for p,q in b['bids'].items()],
            asks=[dict(price=decimal_text(p),size=decimal_text(q)) for p,q in b['asks'].items()]) for k,b in self.book.books.items()}

def verify_release(folder,receipt):
    for f in receipt['files']:
        p=Path(folder)/f['file']
        if p.stat().st_size!=f['bytes'] or sha_file(p)!=f['sha256']:raise ValueError('standard_committed_file_changed')

def run(source,output,*,max_batches=2,fail_before_commit=False):
    """One bounded incremental pass, checkpoint after atomic release publication."""
    if max_batches<1:raise ValueError('positive_batch_limit_required')
    output=Path(output);output.mkdir(parents=True,exist_ok=True);lock=OwnerLock(output/'writer.lock')
    started=time.perf_counter();results=[]
    converter_sha256=sha_file(Path(__file__))
    try:
        path=output/'state.json'
        state=json.loads(path.read_bytes()) if path.exists() else dict(schema='market-standard-state/1',repo=source.repo,
            collection=source.collection,last=0,commits=[])
        if state['repo']!=source.repo or state['collection']!=source.collection:raise ValueError('standard_source_identity_changed')
        if state.get('converter_sha256',converter_sha256)!=converter_sha256:raise ValueError('converter_changed_use_new_output_version')
        state['converter_sha256']=converter_sha256
        manifests=source.manifests();by_id={x['manifest']['identity']:x for x in manifests}
        for c in state['commits']:
            if c['identity'] not in by_id or by_id[c['identity']]['sha256']!=c['source_sha256']:raise ValueError('source_history_changed')
            folder=output/'releases'/c['identity'];r=json.loads((folder/'release.json').read_bytes())
            if sha_file(folder/'release.json')!=c['release_sha256']:raise ValueError('release_receipt_changed')
            verify_release(folder,r)
        pending=[x for x in manifests if x['manifest']['last']>state['last']][:max_batches]
        semantic_state=dict(metadata={},assets={},conditions={},definitions={},trade_payloads={},books={},window=None)
        if state['commits']:
            with gzip.open(output/'releases'/state['commits'][-1]['identity']/'checkpoint.json.gz','rb') as f:semantic_state=json.loads(f.read())
        for descriptor in pending:
            m=descriptor['manifest'];ident=m['identity']
            if m['first']!=state['last']+1:raise ValueError('missing_source_prefix')
            final=output/'releases'/ident
            if final.exists():
                receipt=json.loads((final/'release.json').read_bytes());verify_release(final,receipt)
                if receipt['source_sha256']!=descriptor['sha256']:raise ValueError('recovery_source_changed')
                with gzip.open(final/'checkpoint.json.gz','rb') as f:semantic_state=json.loads(f.read())
            else:
                work=output/'work'/ident
                if work.exists():
                    # Keep incomplete attempt for inspection; never reuse partial Parquet.
                    work.rename(work.with_name(ident+'-incomplete-'+str(time.time_ns())))
                work.mkdir(parents=True);tables=Tables(work);engine=Semantic(source.collection,deepcopy(semantic_state),tables)
                catalog=source.catalog(m);window=catalog['recording_window']
                if window:engine.state['window']=window
                # Catalog is preserved but never seeded as an earlier event-time state.
                tables.put('observations',dict(event_id=digest([source.collection,ident,'catalog']),sequence=m['last'],received_ns=0,
                    venue='shared',market_id=None,connection_epoch=None,kind='catalog_snapshot',detail_json=canonical(catalog)))
                try:
                    for e,line in source.events(m):engine.event(e,line)
                finally:tables.close()
                engine.checkpoint();semantic_state=engine.state
                (work/'checkpoint.json.gz').write_bytes(gzip.compress(encoded(semantic_state),compresslevel=6,mtime=0))
                files=[]
                for p in work.rglob('*'):
                    if not p.is_file():continue
                    count=None
                    if p.suffix=='.parquet':
                        with pq.ParquetFile(p,page_checksum_verification=True) as pf:
                            count=0
                            for batch in pf.iter_batches(batch_size=1024):count+=batch.num_rows
                    with p.open('r+b') as stream:stream.flush();os.fsync(stream.fileno())
                    files.append(dict(file=p.relative_to(work).as_posix(),sha256=sha_file(p),bytes=p.stat().st_size,rows=count))
                receipt=dict(schema='market-standard-release/1',input_profile='hf-file-spool/1',collection=source.collection,
                    converter_sha256=converter_sha256,
                    source_repo=source.repo,source_revision=source.revision,source_manifest=descriptor['path'],source_sha256=descriptor['sha256'],
                    manifest_receipt_checked=False,first=m['first'],last=m['last'],source_files=m['files'],
                    stats=dict(engine.stats),unsupported_types=dict(engine.unsupported),counts=dict(tables.counts),files=files,
                    exchange_completeness_certified=False,trade_occurrences_are_not_unique_fills=True,
                    source_archives_retained=True,server_mutated=False,upload_performed=False)
                durable_file(work/'release.json',encoded(receipt));final.parent.mkdir(exist_ok=True)
                os.replace(work,final);sync_directory(final.parent)
            if fail_before_commit:raise RuntimeError('injected_checkpoint_interruption')
            commit=dict(identity=ident,last=m['last'],source_sha256=descriptor['sha256'],release_sha256=sha_file(final/'release.json'))
            state['commits'].append(commit);state['last']=m['last'];state['last_revision']=source.revision
            durable_file(path,encoded(state),replace=True);results.append(receipt)
        return dict(schema='market-standard-pass/1',new_batches=len(results),through_sequence=state['last'],
            wall_seconds=time.perf_counter()-started,releases=results,source_revision=source.revision,server_mutated=False,upload_performed=False)
    finally:lock.close()
