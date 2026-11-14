"""V3 publication adapter. Receiver stays independent; verified HF gates deletion.

The compact encoder keeps only its two checkpoint generations and active trade
identity buckets. No API request or reconstruction from unverified cloud bytes.
"""
import base64,gzip,hashlib,json,re,shutil,time
from pathlib import Path
from decimal import Decimal
from .store import durable_file,encoded,sha256_file,sync_directory
from .backtest_spool import run_batch,ENCODING,CODE_SHA256,MAX_LINE,collect_recovery
from .backtest_index import TradeIndex
from task_control.contracts import require

PROFILE='backtest-v3-zstd6'
STATE='publication-v3'
MAX_BLOCKS=16  # <=128 MiB of ordinary sealed inputs, bounded separately from capture.


def validate(m,prefix):
    require(m.get('schema')=='file-spool-publication/3' and m.get('release')=='v3' and
        m.get('encoding')==ENCODING and m.get('projection_policy')==PROFILE and
        m.get('source_payload_is_official_event_envelope') is False and
        m.get('full_trade_completeness_certified') is False,'v3_manifest_profile')
    identity=f'{m["first"]:020d}-{m["last"]:020d}'
    require(m['identity']==identity and 1<=len(m['blocks'])<=MAX_BLOCKS,'v3_manifest_identity')
    cursor=m['first']
    for b in m['blocks']:
        require(b['file']==f'{cursor:020d}.jsonl' and b['first']==cursor and b['rows']==b['last']-cursor+1,'v3_block_sequence')
        cursor=b['last']+1
    require(cursor==m['last']+1,'v3_last_sequence')
    a=m['archive'];files=m['files']
    require(a['schema']=='market-backtest-chunk/1' and a['encoding']==ENCODING and
        a['identity']==identity and a['first']==m['first'] and a['last']==m['last'],'v3_archive_binding')
    require(len(files)==1 and files[0]==dict(remote_path=prefix+'/v3/events/'+identity+'.zip',
        bytes=a['bytes'],sha256=a['sha256']),'v3_inventory')
    require(re.fullmatch('[a-f0-9]{64}',m['codec_sha256']) is not None,'v3_codec_binding')
    return True


def prepare(root,prefix):
    root=Path(root);out=root/'outbox';out.mkdir(exist_ok=True);spool=root/'spool'
    unfinished=next((p for p in sorted(out.glob('*/prepare.json')) if not (p.parent/'pending.json').exists()),None)
    if unfinished:
        plan=json.loads(unfinished.read_bytes());work=unfinished.parent
        require(plan['publication_profile']==PROFILE and plan['prefix']==prefix and plan['identity']==work.name,'v3_prepare_binding')
    else:
        paths=sorted((spool/'sealed').glob('*.json'))[:MAX_BLOCKS]
        if not paths:return None
        blocks=[json.loads(p.read_bytes()) for p in paths]
        identity=f'{blocks[0]["first"]:020d}-{blocks[-1]["last"]:020d}'
        work=out/identity;work.mkdir(exist_ok=True)
        body=gzip.compress((root/'catalog.json').read_bytes(),compresslevel=3,mtime=0)
        plan=dict(schema='file-spool-prepare/1',publication_profile=PROFILE,prefix=prefix,identity=identity,blocks=blocks,
            book_gap_policy='wire_connection_events',
            catalog_gzip_base64=base64.b64encode(body).decode(),catalog_sha256=hashlib.sha256(body).hexdigest())
        durable_file(work/'prepare.json',encoded(plan))
    identity=plan['identity'];blocks=plan['blocks'];body=base64.b64decode(plan['catalog_gzip_base64'],validate=True)
    require(hashlib.sha256(body).hexdigest()==plan['catalog_sha256'],'v3_catalog_hash')
    cursor=blocks[0]['first'];sources=[]
    for b in blocks:
        p=spool/'blocks'/b['file']
        require(p.parent==spool/'blocks' and not p.is_symlink() and b['first']==cursor and
            b['file']==f'{cursor:020d}.jsonl' and b['rows']==b['last']-cursor+1,'v3_source_scope')
        require(p.stat().st_size==b['bytes'] and sha256_file(p)==b['sha256'],'v3_source_changed')
        cursor=b['last']+1;sources.append(p)
    def events():
        for p in sources:
            with p.open('rb') as f:
                while True:
                    line=f.readline(MAX_LINE+1)
                    if not line:break
                    require(len(line)<=MAX_LINE and line.endswith(b'\n'),'v3_source_line_bound')
                    yield json.loads(line)
    started=time.monotonic()
    catalog=json.loads(gzip.decompress(body),parse_float=Decimal)
    binding_fields=dict(blocks=blocks,catalog_sha256=plan['catalog_sha256'])
    if plan.get('book_gap_policy')=='wire_connection_events':
        # Catalog readiness is a durability/admission observation. A new full
        # snapshot awaiting fsync can briefly make it false on the SAME live
        # socket. It is not a missing wire event. Actual connection_opening,
        # connection_gap/closed and epoch changes stay in the source stream.
        catalog.pop('connection_gaps',None)
        binding_fields['book_gap_policy']=plan['book_gap_policy']
    else:
        require('book_gap_policy' not in plan,'v3_unknown_book_gap_policy')
    binding=hashlib.sha256(encoded(binding_fields)).hexdigest()
    result=run_batch(root/STATE,collection=prefix+'/v3',identity=identity,first=blocks[0]['first'],last=blocks[-1]['last'],
        input_sha256=binding,catalog=catalog,event_factory=events,allow_external_prefix=True)
    chunk=root/STATE/'chunks'/identity;a=json.loads((chunk/'chunk.json').read_bytes())
    target=work/(identity+'.zip')
    if target.exists():require(target.stat().st_size==a['bytes'] and sha256_file(target)==a['sha256'],'v3_staging_changed')
    else:
        # Separate inode: cleanup of outbox must never invalidate converter state.
        temp=work/(identity+'.part');shutil.copyfile(chunk/'data.zip',temp)
        with temp.open('r+b') as f:
            import os
            os.fsync(f.fileno())
        temp.replace(target);sync_directory(work)
    require(sha256_file(target)==a['sha256'],'v3_staging_hash')
    files=[dict(local=target.relative_to(root).as_posix(),remote_path=prefix+'/v3/events/'+target.name,bytes=a['bytes'],sha256=a['sha256'])]
    m=dict(schema='file-spool-publication/3',release='v3',identity=identity,first=blocks[0]['first'],last=blocks[-1]['last'],
        encoding=ENCODING,projection_policy=PROFILE,codec_sha256=CODE_SHA256,archive=a,blocks=blocks,
        book_gap_policy=plan.get('book_gap_policy','legacy_catalog_readiness'),
        files=[{k:v for k,v in x.items() if k!='local'} for x in files],
        source_payload_is_official_event_envelope=False,full_trade_completeness_certified=False,
        recovery_scope='local_two_checkpoints_and_content_addressed_trade_index',
        previous_raw_prefix=prefix,external_prefix_requires_history=True)
    validate(m,prefix);marker=work/'manifest.json';durable_file(marker,encoded(m));remote=prefix+'/segments/'+identity+'.json'
    files.append(dict(local=marker.relative_to(root).as_posix(),remote_path=remote,bytes=marker.stat().st_size,sha256=sha256_file(marker)))
    item=dict(schema='file-spool-pending/1',identity=identity,blocks=blocks,files=files,marker=remote,manifest=m,
        compression_seconds=time.monotonic()-started,compact_stats=result.get('stats',{}),prepared_at_ns=time.time_ns())
    durable_file(work/'pending.json',encoded(item));return work/'pending.json'


def retire_compact(root,item,receipt):
    """Called only after durable exact-revision proof; restart-safe and fail closed."""
    root=Path(root);output=root/STATE;identity=item['identity'];m=item['manifest']
    validate(m,item['marker'].rsplit('/segments/',1)[0])
    # Independently bind the exact payload and manifest, not just a boolean ACK.
    expected=[{k:v for k,v in f.items() if k!='local'} for f in item['files']]
    require(receipt['identity']==identity and receipt['files']==expected and receipt['revision']==receipt['verification']['revision'] and
        [{k:x[k] for k in ('remote_path','bytes','sha256')} for x in receipt['verification']['files']]==expected,'v3_exact_proof')
    head=json.loads((output/'head.json').read_bytes())
    require(head['through']==m['last'] and head['identity']==identity and head['codec_sha256']==m['codec_sha256'],'v3_cleanup_checkpoint_boundary')
    collect_recovery(output,head,TradeIndex(output/'trade-index'))
    checkpoint=json.loads(__import__('zstandard').ZstdDecompressor().decompress((output/head['checkpoint']).read_bytes()))
    TradeIndex(output/'trade-index',checkpoint['trade_index']).verify()
    chunk=output/'chunks'/identity
    require(not chunk.is_symlink() and sha256_file(chunk/'chunk.json')==json.loads((chunk/'recovery.json').read_bytes())['manifest_sha256'] and
        json.loads((chunk/'chunk.json').read_bytes())==m['archive'],'v3_cleanup_manifest')
    p=chunk/'data.zip'
    if p.exists():
        require(not p.is_symlink() and p.stat().st_size==m['archive']['bytes'] and sha256_file(p)==m['archive']['sha256'],'v3_cleanup_archive_changed')
        p.unlink();sync_directory(chunk)
    # Retain a small durable latest receipt; historical receipts use existing GC.
    durable_file(output/'verified-head.json',encoded(dict(identity=identity,through=m['last'],revision=receipt['revision'],files=expected)),replace=True)
    # Serialized publisher: every earlier chunk crossed verified cleanup before
    # the next prepare. Never remove a raw source or active checkpoint here.
    dirs=sorted(p for p in (output/'chunks').iterdir() if p.is_dir())
    for old in dirs[:-2]:
        require(re.fullmatch(r'[0-9]{20}-[0-9]{20}',old.name) and old.name<identity and not old.is_symlink(),'v3_control_gc_scope')
        if not any(old.iterdir()):old.rmdir();continue
        intent=old/'gc.json'
        if not intent.exists():
            require({p.name for p in old.iterdir()}=={'chunk.json','recovery.json'},'v3_control_gc_unverified_payload')
            durable_file(intent,encoded({name:sha256_file(old/name) for name in ('chunk.json','recovery.json')}))
        hashes=json.loads(intent.read_bytes())
        require(set(hashes)=={'chunk.json','recovery.json'} and {p.name for p in old.iterdir()}<=set(hashes)|{'gc.json'},'v3_control_gc_intent')
        for name,h in hashes.items():
            p=old/name;require(not p.is_symlink(),'v3_control_gc_symlink')
            if p.exists():
                require(sha256_file(p)==h,'v3_control_gc_changed');p.unlink()
        intent.unlink();old.rmdir()
    sync_directory(output/'chunks')
