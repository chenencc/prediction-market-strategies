"""Bounded derived L2 checkpoints, without pretending they are exchange events.

Original components remain immutable historical data. Catalog blocks with no
new siblings retain their compressed bytes. Growing same-schema catalogs merge
losslessly so checkpoint file count does not grow with each recording cut. Publication
waits for source readback receipts, not for a recursively unbounded replay.
"""
import hashlib,json,os,shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from native_archive.backtest_profile import ENCODING
from task_control.contracts import digest,require,seal
from .archive_replay import replay_book_parts,final_states
from .components import (_current_manifest,_dependencies,CATALOG_DATASETS,component_reference,
                         seal_component,verify_component)
from .publication import sha_file,content_path

LEVEL=pa.struct([('price',pa.string()),('size',pa.string())])
CHECKPOINT_SCHEMA=pa.schema([('condition_id',pa.binary(32)),('market_id',pa.string()),('asset_id',pa.binary(32)),
    ('sequence',pa.uint64()),('available_at_ns',pa.int64()),('bids',pa.list_(pa.field('element',LEVEL))),('asks',pa.list_(pa.field('element',LEVEL)))],
    metadata={b'role':b'derived_received_L2_state_not_exchange_event',b'decimals':b'exact_decimal_text'})
MYRIAD_CHECKPOINT_SCHEMA=pa.schema(list(CHECKPOINT_SCHEMA)+[pa.field('native_outcome_id',pa.uint8()),pa.field('connection_epoch',pa.string())],
    metadata=CHECKPOINT_SCHEMA.metadata)


def checkpoint_catalog(parts,destination):
    """Stream exact catalog rows into bounded files, never select latest-only.

    Each distinct input block occurs once per dataset/venue/schema. A merge
    preserves row multiplicity, ordering, every field and Arrow type. No price,
    fee, version, relation, timestamp or binary identity is cast or discarded.
    """
    groups={};seen=set();destination=Path(destination)
    for root,manifest in parts:
        for entry in manifest['files']:
            name=entry['file'];dataset=name.split('/')[1]
            if dataset not in CATALOG_DATASETS:continue
            venue=name.split('/venue=',1)[1].split('/',1)[0] if '/venue=' in name else None
            original=bound_catalog_file(root,entry)
            schema=pq.read_schema(original);schema_hash=hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
            key=(dataset,venue,schema_hash);identity=(*key,entry['sha256'])
            if identity in seen:continue
            seen.add(identity);groups.setdefault(key,[]).append((original,entry,schema))
    require(len(groups)<=93,'checkpoint_catalog_schema_group_bound')
    settings={k:v for k,v in ENCODING.items() if k!='row_group_size'}
    report=[]
    for (dataset,venue,schema_hash),items in groups.items():
        if len(items)==1:
            original,entry,schema=items[0];target=destination/content_path(entry['file'],entry['sha256'])
            target.parent.mkdir(parents=True,exist_ok=True)
            try:os.link(original,target)
            except OSError:shutil.copyfile(original,target)
            require(sha_file(target)==entry['sha256'],'checkpoint_catalog_copy_changed')
            continue
        prefix=f'v4/{dataset}/'+(f'venue={venue}/' if venue else '')
        target=destination/(prefix+'checkpoint-catalog-'+schema_hash[:16]+'.parquet')
        target.parent.mkdir(parents=True,exist_ok=True);schema=items[0][2];count=0
        with pq.ParquetWriter(target,schema,**settings) as writer:
            for original,entry,current_schema in items:
                require(current_schema.equals(schema,check_metadata=True),'checkpoint_catalog_schema_changed')
                with pq.ParquetFile(original,page_checksum_verification=True) as reader:
                    for batch in reader.iter_batches(batch_size=4096):
                        writer.write_batch(batch,row_group_size=ENCODING['row_group_size']);count+=batch.num_rows
                require(sha_file(original)==entry['sha256'],'checkpoint_catalog_source_changed_during_merge')
        require(count==sum(e['rows'] for _,e,_ in items),'checkpoint_catalog_row_count_changed')
        report.append(dict(dataset=dataset,venue=venue,input_files=len(items),rows=count,schema_sha256=schema_hash,
            output=target.relative_to(destination).as_posix()))
    return report


def bound_catalog_file(root,entry):
    from .publication import bound_file
    original=bound_file(root,entry['file'])
    require(original.stat().st_size==entry['bytes'] and sha_file(original)==entry['sha256'],'checkpoint_catalog_input_changed')
    return original


def failed_checkpoint_inventory(root):
    """Bind a partial failed checkpoint, without blessing its invalid manifest."""
    root=Path(root);require(not root.is_symlink(),'failed_checkpoint_symlink_not_allowed');root=root.resolve()
    require(root.is_dir() and not (root/'manifest.json').exists(),'unsealed_failed_checkpoint_required')
    rows=[]
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink() and path.resolve().is_relative_to(root),'failed_checkpoint_symlink_not_allowed')
        if path.is_dir():continue
        require(path.is_file() and path.suffix=='.parquet' and path.relative_to(root).parts[0]=='v4',
            'failed_checkpoint_unknown_payload')
        require(len(rows)<4096,'failed_checkpoint_inventory_file_bound')
        rows.append(dict(file=path.relative_to(root).as_posix(),bytes=path.stat().st_size,sha256=sha_file(path)))
    total=sum(r['bytes'] for r in rows)
    require(total<=128*1024**2,'failed_checkpoint_inventory_byte_bound')
    return seal('failed-book-checkpoint-inventory/1',files=rows,file_count=len(rows),total_bytes=total)


def create_book_checkpoint(source,destination,*,dependency_roots=None):
    """Replay one checked bounded DAG and commit a self-contained state index."""
    source=Path(source).resolve();destination=Path(destination).resolve()
    require(not destination.exists(),'checkpoint_requires_fresh_owned_directory')
    head=_current_manifest(source);require(head['component']=='books_metadata','checkpoint_book_head_required')
    parts=_dependencies(source,head,dependency_roots or {})+[(source,head)]
    require(all(not destination.is_relative_to(Path(root).resolve()) and not Path(root).resolve().is_relative_to(destination)
                for root,_ in parts),'checkpoint_source_destination_overlap')
    references=[component_reference(root) for root,_ in parts]
    require(len({r['component_id'] for r in references})==len(references),'checkpoint_source_component_duplicate')
    destination.mkdir(parents=True)
    conditions={}
    from native_archive.storage_profile import dataset_rows
    for root,m in parts:
        for venue in ('polymarket','predictfun','myriad'):
            for row in dataset_rows(root,m,'instruments',venue):
                condition=row.get('condition_id')
                if isinstance(condition,str):condition=bytes.fromhex(condition.removeprefix('0x'))
                for instrument in json.loads(row['instruments_json']):
                    if instrument.get('book_origin')!='native':continue
                    asset=int(instrument['asset_id']).to_bytes(32,'big')
                    old=conditions.setdefault((venue,asset),condition)
                    require(old==condition,'checkpoint_asset_condition_conflict')
    checkpoint_catalog(parts,destination)
    writers={};buffers={};settings={k:v for k,v in ENCODING.items() if k!='row_group_size'}
    def flush(venue):
        if not buffers.get(venue):return
        if venue not in writers:
            target=destination/f'v4/book_checkpoints/venue={venue}/checkpoint.parquet';target.parent.mkdir(parents=True,exist_ok=True)
            schema=MYRIAD_CHECKPOINT_SCHEMA if venue=='myriad' else CHECKPOINT_SCHEMA
            writers[venue]=pq.ParquetWriter(target,schema,**settings)
        schema=MYRIAD_CHECKPOINT_SCHEMA if venue=='myriad' else CHECKPOINT_SCHEMA
        writers[venue].write_table(pa.Table.from_pylist(buffers[venue],schema=schema));buffers[venue]=[]
    def state(venue,row):
        condition=conditions.get((venue,row['asset_id']))
        require((venue=='myriad' and (venue,row['asset_id']) in conditions or condition is not None) and row['condition_id'] in (None,condition),'checkpoint_asset_catalog_unresolved')
        row=dict(row,condition_id=condition,available_at_ns=head['window']['end_ns'])
        for side in ('bids','asks'):row[side]=[dict(price=p,size=q) for p,q in row[side]]
        buffers.setdefault(venue,[]).append(row)
        if len(buffers[venue])>=32:flush(venue)
    try:
        # Emit the canonical states during the REQUIRED source audit replay;
        # do not run another complete historical L2 pass to build the checkpoint.
        proof=verify_component(source,dependency_roots=dependency_roots,state_sink=state);replay=proof['books']
        for venue in tuple(buffers):flush(venue)
        if 'myriad' in replay and 'myriad' not in writers:
            # A received disconnect can leave ZERO valid books. Absence of a
            # file means "venue not present", which is a different state. Keep
            # a typed empty checkpoint, without inventing an asset or event.
            require(replay['myriad']['assets']==0,'checkpoint_missing_myriad_state_rows')
            target=destination/'v4/book_checkpoints/venue=myriad/checkpoint.parquet';target.parent.mkdir(parents=True,exist_ok=True)
            pq.write_table(pa.Table.from_pylist([],schema=MYRIAD_CHECKPOINT_SCHEMA),target,**settings)
    finally:
        for writer in writers.values():writer.close()
    checkpoint=seal('native-book-replay-checkpoint/1',source_components=references,source_head=component_reference(source),
        source_replay_sha256=digest(proof),final_states=final_states(replay),as_of_ns=head['window']['end_ns'],
        observed_event=False,source_coverage_certified=False)
    return seal_component(destination,component='books_metadata',window=head['window'],native_fences=head['native_fences'],
                          checkpoint=checkpoint)
