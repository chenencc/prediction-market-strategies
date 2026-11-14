"""Immutable event shards. Legacy exact JSONL or explicit compact Zstd profile.

ZIP is only a stored container for new Zstd entries, never double compression.
Metadata and source fences retain their original dependency/inventory bindings.
"""
import hashlib,json,os,time,zipfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit,urlencode
from urllib.request import Request,build_opener,HTTPRedirectHandler
from marketdata.core import atomic
from task_control.contracts import require,verified,seal,digest
from .publication import bound_file,sha_file
from .store import durable_file,encoded,sync_directory
from .raw_event_codec import Compactor,LEVELS,profile

TABLES={'accept_poly_v3','accept_extra_v3'}


def source_rows(url,fence):
    require(urlsplit(url).hostname in {'127.0.0.1','localhost','::1'} and urlsplit(url).scheme=='http',
            'raw_export_owned_loopback_only')
    table=fence['table'];require(table in TABLES,'raw_known_native_table_required')
    lower=fence['after_sequence'];upper=fence['through_sequence']
    require(type(lower) is int and type(upper) is int and 0<=lower<=upper,'raw_source_range_required')
    query=(f'SELECT sequence,toUnixTimestamp64Nano(timestamp_received) AS received_ns,data FROM {table} FINAL '
           f'WHERE sequence>{lower} AND sequence<={upper} ORDER BY sequence FORMAT JSONEachRow')
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self,*args,**kwargs):return None
    endpoint=url+('&' if '?' in url else '?')+urlencode(dict(max_threads=1,max_memory_usage=256*1024**2))
    with build_opener(NoRedirect).open(Request(endpoint,data=query.encode()),timeout=60) as response:
        require(response.status==200,'raw_export_native_HTTP_failed')
        pending=b''
        for chunk in iter(lambda:response.read(65536),b''):
            pending+=chunk
            while b'\n' in pending:
                line,pending=pending.split(b'\n',1)
                if line:yield line+b'\n'
            require(len(pending)<=32*1024**2,'raw_source_frame_guard')
        require(not pending,'raw_export_incomplete_final_line')


def metadata_ref(root,name,value):
    require(name in {'plan','lifecycle','market_index','recording_contract'},'raw_metadata_kind_required')
    body=encoded(value);sha=hashlib.sha256(body).hexdigest();relative='metadata/'+sha+'.json'
    (root/'metadata').mkdir(exist_ok=True)
    durable_file(root/relative,body)
    return dict(kind=name,file=relative,sha256=sha,bytes=len(body))


def seal_cut(capture,cut,*,url,rows=source_rows,metadata_encoding=None):
    codec=profile(cut.get('raw_event_codec'))
    capture=Path(capture).resolve();root=capture/'raw-segments';root.mkdir(exist_ok=True)
    index=cut['index'];require(type(index) is int and 0<=index<1000000,'raw_cut_index_required')
    (root/'completed').mkdir(exist_ok=True);receipt=root/'completed'/f'{index:08}.json'
    if receipt.exists():
        saved=verified(json.loads(receipt.read_text()),'raw-event-segment/1')
        require(saved['cut_sha256']==digest(cut),'raw_closed_cut_changed')
        return saved
    previous=None;prior=None
    if index:
        prior=verified(json.loads(bound_file(root,f'completed/{index-1:08}.json').read_text()),'raw-event-segment/1')
        previous=dict(index=index-1,manifest_sha256=sha_file(root/f'completed/{index-1:08}.json'),contract_hash=prior['contract_hash'])
        previous_fences={f['table']:f for f in prior['source_fences']}
        for f in cut['native_fences']:
            require(f['generation']==previous_fences[f['table']]['generation'] and
                    f['after_sequence']==previous_fences[f['table']]['through_sequence'],'raw_shard_sequence_discontinuity')
    plan_ref=cut['projection_plan_ref']
    require(plan_ref=='projection-plans/'+cut['projection_plan_sha256']+'.json','raw_plan_reference_changed')
    plan=json.loads(bound_file(capture,plan_ref).read_text(encoding='utf8'))
    require(digest(plan)==cut['projection_plan_sha256'],'raw_plan_changed')
    require(metadata_encoding in (None,'gzip-delta-v1'),'raw_metadata_encoding_unknown')
    def reference(name,value):
        if metadata_encoding is None:return metadata_ref(root,name,value)
        from .raw_metadata_codec import write
        old=next((m for m in prior['metadata'] if m['kind']==name),None) if prior else None
        return write(root,name,value,previous=old)
    refs=[reference('plan',plan)]
    source_contract=cut.get('raw_recording_contract_ref')
    if source_contract:
        body=bound_file(capture,source_contract['file'])
        require(sha_file(body)==source_contract['sha256'],'raw_recording_contract_changed')
        contract=verified(json.loads(body.read_bytes()),'native-recording-contract/1')
        require(contract['observed_at_ns']<=cut['captured_at_ns'],'raw_recording_contract_future_observation')
        require(contract['persisted_schemas'].get('archive_profile')==codec,'raw_recording_contract_codec_changed')
        refs.append(reference('recording_contract',contract))
    context=cut.get('raw_lifecycle_ref')
    if context:
        body=bound_file(capture,context['file']);require(sha_file(body)==context['sha256'],'raw_lifecycle_changed')
        snapshot=json.loads(body.read_text(encoding='utf8'))
        refs.append(reference('lifecycle',snapshot))
        from .raw_market_index import market_index
        refs.append(reference('market_index',market_index(snapshot)))
    require({f['table'] for f in cut['native_fences']}==TABLES and len(cut['native_fences'])==2,'raw_both_native_streams_required')
    target=root/'payloads'/f'{index:08}.zip';target.parent.mkdir(exist_ok=True)
    pending=target.with_suffix('.zip.part');entries=[];started=time.perf_counter();cpu=time.process_time()
    # Only this worker's uncommitted partial is replaced. Sources and completed
    # archives are immutable; crash retry re-exports the exact same fenced cut.
    require(not target.is_symlink() and not pending.is_symlink(),'raw_archive_symlink_not_allowed')
    compression=zipfile.ZIP_STORED if codec else zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(pending,'w',compression=compression,compresslevel=None if codec else 3,allowZip64=True) as archive:
        for original in cut['native_fences']:
            f=verified(original,'native-prefix-fence/1');last=f['after_sequence'];count=0;inventory=hashlib.sha256();content=hashlib.sha256();size=0
            require(f['through_sequence']-last==f['row_count'],'raw_fence_count_changed')
            name='events/'+f['table']+'.jsonl'+('.zst' if codec else '')
            compactor=Compactor() if codec else None
            with write_entry(archive,name,codec) as output:
                for line in rows(url,f):
                    require(isinstance(line,bytes) and line.endswith(b'\n') and len(line)<=32*1024**2,'raw_event_line_required')
                    row=json.loads(line);require(set(row)=={'sequence','received_ns','data'} and isinstance(row['data'],str),'raw_row_columns_changed')
                    seq=int(row['sequence']);stamp=int(row['received_ns'])
                    require(seq==last+1 and seq<=f['through_sequence'] and stamp>=0,'raw_source_sequence_gap')
                    body_sha=hashlib.sha256(row['data'].encode()).hexdigest()
                    inventory.update(f'[{seq},{stamp},"{body_sha}"]\n'.encode())
                    if compactor:line=compactor.transform(f['table'],row,line)
                    output.write(line);content.update(line);size+=len(line);count+=1;last=seq
            require(count==f['row_count'] and inventory.hexdigest()==f['source_inventory_sha256'],'raw_source_inventory_changed')
            entry=dict(file=name,rows=count,bytes=size,sha256=content.hexdigest())
            if codec:entry.update(encoding='zstd',transform_counts=dict(compactor.counts))
            entries.append(entry)
        envelope=dict(schema='native-raw-events/2' if codec else 'native-raw-events/1',index=index,previous=previous,source_fences=cut['native_fences'],metadata=refs,
                      source_cut_sha256=digest(cut),native_body_preserved=not bool(codec),book_replay_performed=False,
                      exchange_completeness_certified=False)
        if codec:envelope.update(archive_profile=codec,entries=entries,ws_originals_preserved=True,
            omitted_fields=['myriad REST outcomes[*].price_charts'],poly_projections='same_shard_verified_reference_or_original')
        archive.writestr('manifest.json',encoded(envelope))
    with pending.open('r+b') as stream:os.fsync(stream.fileno())
    os.replace(pending,target)
    sync_directory(target.parent)
    result=seal('raw-event-segment/1',index=index,cut_sha256=digest(cut),previous=previous,metadata=refs,
        source_fences=cut['native_fences'],entries=entries,
        payload=dict(file=target.relative_to(root).as_posix(),sha256=sha_file(target),bytes=target.stat().st_size),
        sealed_at_ns=time.time_ns(),wall_seconds=time.perf_counter()-started,CPU_seconds=time.process_time()-cpu,
        raw_events_preserved=not bool(codec),conversion_performed=False,semantic_replay_performed=False,
        **(dict(archive_profile=codec,archive_schema='native-raw-events/2',ws_originals_preserved=True,
                archive_compaction_performed=True,source_inventory_verified=True) if codec else {}))
    durable_file(receipt,encoded(result));return result


@contextmanager
def write_entry(archive,name,codec):
    with archive.open(name,'w',force_zip64=True) as output:
        if codec:
            import zstandard
            with zstandard.ZstdCompressor(level=LEVELS[codec],write_checksum=True).stream_writer(output,closefd=False) as stream:
                yield stream
        else:yield output


@contextmanager
def read_entry(archive,item):
    """Shared offline reader for both archive generations; bounded decoding."""
    encoding=item.get('encoding')
    require(encoding in (None,'zstd'),'raw_entry_encoding_unknown')
    with archive.open(item['file']) as source:
        if encoding=='zstd':
            import zstandard
            with zstandard.ZstdDecompressor(max_window_size=64*1024**2).stream_reader(source) as stream:yield stream
        else:yield source


def validate_profile(receipt,cut=None):
    """Cheap policy binding for publisher/retirement, no full archive readback."""
    codec=profile(receipt.get('archive_profile'))
    if cut is not None:require(codec==profile(cut.get('raw_event_codec')),'raw_archive_profile_cut_changed')
    if not codec:return
    require(receipt.get('archive_schema')=='native-raw-events/2' and receipt.get('ws_originals_preserved') is True and
            receipt.get('raw_events_preserved') is False and receipt.get('source_inventory_verified') is True,
            'raw_compact_profile_claims')
    require({e['file'] for e in receipt['entries']}=={'events/'+t+'.jsonl.zst' for t in TABLES} and
            len(receipt['entries'])==2,'raw_compact_entry_set')
    fences={f['table']:f for f in receipt['source_fences']}
    for item in receipt['entries']:
        counts=item['transform_counts'];table=item['file'].split('/')[-1].split('.')[0]
        require(item['encoding']=='zstd' and item['rows']==fences[table]['row_count']==counts['input_rows'] and
                all(type(v) is int and v>=0 for v in counts.values()) and
                counts['input_rows']==counts['unchanged_rows']+counts['poly_projection_references']+counts['myriad_chart_rows'],
                'raw_compact_counts_changed')
        require(counts['source_unavailable_rows']<=counts['unchanged_rows'] and counts['unsupported_rows']<=counts['unchanged_rows'] and
                counts['myriad_chart_fields']>=counts['myriad_chart_rows'] and
                (counts['myriad_chart_rows']==0 if table=='accept_poly_v3' else counts['poly_projection_references']==0),
                'raw_compact_transform_scope_changed')


def verify_saved_zip(root,receipt):
    """Explicit offline/sample integrity check; not on ordinary capture path."""
    root=Path(root);r=verified(receipt,'raw-event-segment/1');p=bound_file(root,r['payload']['file'])
    require(sha_file(p)==r['payload']['sha256'] and p.stat().st_size==r['payload']['bytes'],'raw_zip_hash_changed')
    validate_profile(r)
    with zipfile.ZipFile(p) as archive:
        require(set(archive.namelist())=={'manifest.json',*(x['file'] for x in r['entries'])},'raw_zip_member_set_changed')
        manifest=json.loads(archive.read('manifest.json'))
        require(manifest['source_cut_sha256']==r['cut_sha256'] and manifest['source_fences']==r['source_fences'] and
                manifest['metadata']==r['metadata'] and manifest['previous']==r['previous'],'raw_inner_manifest_changed')
        if r.get('archive_profile'):
            require(manifest['schema']=='native-raw-events/2' and manifest['archive_profile']==r['archive_profile'] and
                    manifest['entries']==r['entries'],'raw_inner_profile_changed')
        for item in r['entries']:
            h=hashlib.sha256();size=0
            with read_entry(archive,item) as stream:
                for block in iter(lambda:stream.read(65536),b''):
                    h.update(block);size+=len(block);require(size<=item['bytes'],'raw_member_size_limit')
            require(h.hexdigest()==item['sha256'] and size==item['bytes'],'raw_zip_member_changed')
    return dict(verified=True,semantic_replay=False)


async def run_raw_capture(capture,environment,*,metadata_encoding=None):
    import asyncio
    from pair_recorder.supervisor import OwnerLock
    capture=Path(capture);root=capture/'raw-segments';root.mkdir(exist_ok=True);owner=OwnerLock(root/'writer.lock')
    index=0;last=None
    try:
        while True:
            path=capture/f'cut-{index:03}.json';closed=capture/'book-cuts-closed.json'
            if path.exists():
                cut=json.loads(path.read_text());last=await asyncio.to_thread(seal_cut,capture,cut,url=environment.clickhouse_url,metadata_encoding=metadata_encoding)
                index+=1;atomic(capture/'rolling-books-progress.json',dict(status='raw_sealed',next_index=index,at_ns=time.time_ns(),conversion_performed=False))
            elif closed.exists():
                ack=verified(json.loads(closed.read_text()),'native-book-cut-producer-closed/1')
                require(ack['controller_id']==environment.prefix and index==ack['expected_cuts'],'raw_close_cut_count_changed')
                final_cut=json.loads((capture/f'cut-{index-1:03}.json').read_text()) if index else None
                require(ack['last_cut_sha256']==(digest(final_cut) if final_cut else None),'raw_close_last_cut_changed')
                result=dict(status='rolling_raw_event_cuts_sealed',segments=index,last_receipt=last,at_ns=time.time_ns(),
                            native_inputs_deleted=False,conversion_performed=False,semantic_replay_performed=False)
                atomic(capture/'rolling-books-result.json',result);return result
            else:await asyncio.sleep(.5)
    finally:owner.close()
