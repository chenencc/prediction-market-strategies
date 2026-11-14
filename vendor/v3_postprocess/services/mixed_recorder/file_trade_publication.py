"""Opt-in compact publication. No receiver imports, credentials or provider calls."""
import base64,gzip,hashlib,json,time
from pathlib import Path
from .store import durable_file,encoded,sha256_file,sync_directory
from .file_trade_codec import pack,PROFILE,ENCODING,require


def prepare(root,prefix):
    root=Path(root);spool=root/'spool';out=root/'outbox';out.mkdir(exist_ok=True)
    unfinished=next((p for p in sorted(out.glob('*/prepare.json')) if not (p.parent/'pending.json').exists()),None)
    if unfinished:
        plan=json.loads(unfinished.read_bytes());rows=plan['blocks'];identity=plan['identity'];work=unfinished.parent
        require(plan.get('publication_profile')==PROFILE and plan['prefix']==prefix and work.name==identity,'prepare_binding_changed')
        body=base64.b64decode(plan['catalog_gzip_base64'],validate=True)
    else:
        paths=sorted((spool/'sealed').glob('*.json'))[:64]
        if not paths:return None
        rows=[json.loads(p.read_bytes()) for p in paths];identity=f'{rows[0]["first"]:020d}-{rows[-1]["last"]:020d}'
        work=out/identity;work.mkdir(exist_ok=True)
        if (work/'pending.json').exists():
            saved=json.loads((work/'pending.json').read_bytes())
            validate(saved['manifest'],prefix);return work/'pending.json'
        body=gzip.compress((root/'catalog.json').read_bytes(),compresslevel=3,mtime=0)
        plan=dict(schema='file-spool-prepare/1',publication_profile=PROFILE,identity=identity,prefix=prefix,blocks=rows,
            catalog_gzip_base64=base64.b64encode(body).decode(),catalog_sha256=hashlib.sha256(body).hexdigest())
        durable_file(work/'prepare.json',encoded(plan))
    require(hashlib.sha256(body).hexdigest()==plan['catalog_sha256'],'prepare_catalog_hash_changed')
    cursor=rows[0]['first'];sources=[]
    for row in rows:
        path=spool/'blocks'/row['file'];require(path.parent==spool/'blocks' and not path.is_symlink(),'source_path')
        require(row['first']==cursor and row['rows']==row['last']-cursor+1,'batch_sequence');cursor=row['last']+1
        require(path.stat().st_size==row['bytes'] and sha256_file(path)==row['sha256'],'changed_source');sources.append((path,row))
    started=time.monotonic();target=work/(identity+'.zip')
    if 'archive' not in plan:
        proof=pack(sources,target);plan.update(archive=proof,archive_sha256=sha256_file(target));durable_file(work/'prepare.json',encoded(plan),replace=True)
    else:
        proof=plan['archive'];require(target.stat().st_size==proof['archive_bytes'] and sha256_file(target)==plan['archive_sha256'],'prepared_archive_changed')
    files=[dict(local=str(target.relative_to(root)),remote_path=prefix+'/events/'+target.name,bytes=target.stat().st_size,sha256=sha256_file(target))]
    h=hashlib.sha256(body).hexdigest();meta=work/(h+'.json.gz');durable_file(meta,body)
    files.append(dict(local=str(meta.relative_to(root)),remote_path=prefix+'/metadata/'+meta.name,bytes=len(body),sha256=h))
    manifest=dict(schema='file-spool-publication/2',identity=identity,encoding=ENCODING,first=rows[0]['first'],last=rows[-1]['last'],
        blocks=rows,archive=proof,files=[{k:v for k,v in f.items() if k!='local'} for f in files],
        source_payload_is_official_event_envelope=False,full_trade_completeness_certified=False,
        projection_policy=PROFILE,source_block_hashes_are_provenance_only=True)
    marker=work/'manifest.json';durable_file(marker,encoded(manifest));remote=prefix+'/segments/'+identity+'.json'
    files.append(dict(local=str(marker.relative_to(root)),remote_path=remote,bytes=marker.stat().st_size,sha256=sha256_file(marker)))
    item=dict(schema='file-spool-pending/1',identity=identity,blocks=rows,files=files,marker=remote,manifest=manifest,
        compression_seconds=proof['seconds'],prepare_seconds=time.monotonic()-started,prepared_at_ns=time.time_ns())
    pending=work/'pending.json';durable_file(pending,encoded(item));return pending


def validate(manifest,prefix):
    require(manifest.get('schema')=='file-spool-publication/2' and manifest.get('encoding')==ENCODING and
            manifest.get('projection_policy')==PROFILE and manifest.get('source_block_hashes_are_provenance_only') is True and
            manifest.get('source_payload_is_official_event_envelope') is False and manifest.get('full_trade_completeness_certified') is False,'manifest_profile')
    identity=f'{manifest["first"]:020d}-{manifest["last"]:020d}';require(manifest['identity']==identity,'manifest_identity')
    proof=manifest['archive'];require(proof['schema']=='file-trade-archive/1' and proof['encoding']==ENCODING and
        proof['references_scope']=='same_publication_archive_only' and proof['lossy_display_projection'] is True,'archive_profile')
    require(len(proof['decoded_blocks'])==len(manifest['blocks'])==len(proof['entries']) and 1<=len(manifest['blocks'])<=64,'manifest_blocks')
    cursor=manifest['first']
    for source,decoded,entry in zip(manifest['blocks'],proof['decoded_blocks'],proof['entries']):
        require(source['file']==f'{cursor:020d}.jsonl' and source['first']==cursor and source['rows']==source['last']-cursor+1,'manifest_source_sequence')
        require(all(source[k]==decoded[k] for k in ('file','first','last','rows')) and entry['member']=='events/'+source['file']+'.zst','manifest_decoded_binding');cursor=source['last']+1
    require(cursor==manifest['last']+1 and proof['source_rows']==manifest['last']-manifest['first']+1,'manifest_last')
    files=manifest['files'];names=[f['remote_path'] for f in files];zipname=prefix+'/events/'+identity+'.zip'
    metadata=[f for f in files if f['remote_path']==prefix+'/metadata/'+f['sha256']+'.json.gz']
    require(len(files)==2 and len(set(names))==2 and len(metadata)==1 and zipname in names,'manifest_inventory')
    require(next(f['bytes'] for f in files if f['remote_path']==zipname)==proof['archive_bytes'],'archive_size_binding')
    return True
