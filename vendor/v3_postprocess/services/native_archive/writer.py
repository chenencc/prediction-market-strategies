"""Immutable compressed shared-catalog parts; no event source or live dispatch.

The original Rust seven-event writer remains authoritative for Polymarket.
This writer covers only our extra metadata, fee and Pair datasets.
"""
import hashlib,json,os,uuid
from collections import defaultdict
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from marketdata.core import atomic
from .fees import canonical,digest
from .layout import extension_part,pair_folder,WRITER_SETTINGS
from .locking import PublicationLock

FEE_SCHEMA=pa.schema([('schema',pa.string()),('venue',pa.string()),
    ('fee_version_id',pa.string()),('fees_enabled',pa.bool_()),('fee_type',pa.string()),
    ('maker_fee_bps',pa.string()),('taker_fee_bps',pa.string()),('legacy_maker_base_fee',pa.string()),
    ('legacy_taker_base_fee',pa.string()),('schedule_rate',pa.string()),('schedule_exponent',pa.string()),
    ('schedule_taker_only',pa.bool_()),('schedule_rebate_rate',pa.string()),('extra_policy_ref',pa.string()),
    ('configuration_status',pa.string()),('actual_paid_fee_certified',pa.bool_())])
MARKET_SCHEMA=pa.schema([('schema',pa.string()),('venue',pa.string()),('market_id',pa.string()),('condition_id',pa.string()),
    ('question',pa.string()),('title',pa.string()),('description',pa.large_string()),('market_version_id',pa.string()),
    ('instrument_set_ref',pa.string()),('parent_event_set_ref',pa.string()),('price_increment',pa.string()),
    ('minimum_order_size',pa.string()),('price_decimals',pa.int32()),('is_neg_risk',pa.bool_()),('extra_metadata_ref',pa.string())])
OBSERVATION_SCHEMA=pa.schema([('schema',pa.string()),('venue',pa.string()),('market_id',pa.string()),
    ('market_version_id',pa.string()),('fee_version_id',pa.string()),('observation_id',pa.string()),
    ('available_at_ns',pa.int64()),('source_effective_at_ns',pa.int64()),('source_ref',pa.string())])
REPORTED_FEE_SCHEMA=pa.schema([('schema',pa.string()),('venue',pa.string()),('market_id',pa.string()),
    ('observation_id',pa.string()),('fee_record_id',pa.string()),('execution_reference',pa.string()),
    ('leg',pa.string()),('leg_index',pa.int32()),('available_at_ns',pa.int64()),('source_ref',pa.string()),
    ('fee_rate_bps',pa.string()),('fee_amount_native',pa.string()),('fee_unit',pa.string()),('extra_fee_ref',pa.string()),
    ('amount_decimals',pa.int32()),('amount_converted_to_cash',pa.string()),('amount_is_estimate',pa.bool_()),
    ('unique_fill_certified',pa.bool_()),('fee_status',pa.string())])
OBJECT_SCHEMA=pa.schema([('schema',pa.string()),('object_id',pa.string()),('payload_json',pa.large_string())])
API_TRADE_SCHEMA=pa.schema([('schema',pa.string()),('venue',pa.string()),('market_id',pa.string()),
    ('observation_id',pa.string()),('available_at_ns',pa.int64()),('execution_ns',pa.int64()),
    ('source_precision_ns',pa.int64()),('source_ref',pa.string()),('source_page_sha256',pa.string()),
    ('source_row_index',pa.int32()),('source_market_version_id',pa.string()),
    ('settlement_id',pa.string()),('transaction_hash',pa.string()),('asset_id',pa.string()),('index_set',pa.int32()),
    ('price_native',pa.string()),('size_native',pa.string()),('price_unit',pa.string()),('size_unit',pa.string()),
    ('side',pa.string()),('window_membership',pa.string()),('taker_ref',pa.string()),('makers_ref',pa.string()),
    ('extra_trade_ref',pa.string()),('unique_fill_certified',pa.bool_())])
QUALITY_SCHEMA=pa.schema([('schema',pa.string()),('venue',pa.string()),('market_id',pa.string()),
    ('observation_id',pa.string()),('available_at_ns',pa.int64()),('source_ref',pa.string()),
    ('quality_kind',pa.string()),('detail_ref',pa.string())])
SCHEMAS={'markets':MARKET_SCHEMA,'fees':FEE_SCHEMA,'market_observations':OBSERVATION_SCHEMA,'reported_fees':REPORTED_FEE_SCHEMA,
    'api_trades':API_TRADE_SCHEMA,'quality':QUALITY_SCHEMA,'objects':OBJECT_SCHEMA}
IDENTITIES={'markets':'market_version_id','fees':'fee_version_id','market_observations':'observation_id','reported_fees':'fee_record_id',
    'api_trades':'observation_id','quality':'observation_id','objects':'object_id'}

def _identity(row,dataset):return (row.get('venue','shared'),'' if dataset in {'fees','objects'} else row.get('market_id',''),row[IDENTITIES[dataset]])
def _content(row,schema):return {name:row.get(name) for name in schema.names}

def _published(root,dataset,schema):
    """Recover the publication index from immutable manifests, including crash tails."""
    seen={};base=Path(root)/'v2'/dataset
    for manifest in base.rglob('*.manifest.json'):
        receipt=json.loads(manifest.read_text())
        if receipt.get('schema')!='shared-catalog-part/2':raise ValueError('catalog_schema_migration_required')
        target=Path(root)/receipt['file']
        if not target.resolve().is_relative_to(Path(root).resolve()):raise ValueError('catalog_manifest_path_escape')
        if hashlib.sha256(target.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('immutable_catalog_part_changed')
        with pq.ParquetFile(target) as reader:
            if reader.schema_arrow!=schema:raise ValueError('catalog_schema_migration_required')
            rows=reader.read().to_pylist()
        for row in rows:
            key=_identity(row,dataset);h=digest(row)
            if key in seen and seen[key][0]!=h:raise ValueError('catalog_identity_payload_conflict')
            seen[key]=(h,receipt)
    return seen

def _check_references(root,dataset,rows):
    objects={key[2] for key in _published(root,'objects',OBJECT_SCHEMA)}
    fields={'markets':['instrument_set_ref','parent_event_set_ref','extra_metadata_ref'],
        'fees':['extra_policy_ref'],'reported_fees':['extra_fee_ref'],
        'api_trades':['taker_ref','makers_ref','extra_trade_ref'],'quality':['detail_ref']}.get(dataset,[])
    for row in rows:
        for field in fields:
            if row.get(field) and row[field] not in objects:raise ValueError('unpublished_shared_object_reference')
    if dataset=='market_observations':
        markets=_published(root,'markets',MARKET_SCHEMA);fees=_published(root,'fees',FEE_SCHEMA)
        for row in rows:
            if (row['venue'],row['market_id'],row['market_version_id']) not in markets or (row['venue'],'',row['fee_version_id']) not in fees:raise ValueError('unpublished_market_or_fee_version')
    if dataset=='api_trades':
        markets=_published(root,'markets',MARKET_SCHEMA)
        for row in rows:
            if (row['venue'],row['market_id'],row['source_market_version_id']) not in markets:raise ValueError('unpublished_API_market_version')

def write_versions(root,dataset,rows):
    if dataset not in SCHEMAS:raise ValueError('unknown_archive_dataset')
    with PublicationLock(Path(root)/'v2'/'_control'/(dataset+'.lock')):
        return _write_versions(root,dataset,rows)

def _write_versions(root,dataset,rows):
    """Within a batch, retain the first available observation of each version."""
    rows=list(rows);schema=SCHEMAS[dataset];id_field=IDENTITIES[dataset];versions={}
    if dataset!='objects':
        objects=[obj for row in rows for obj in row.get('_objects',[])]
        if objects:write_versions(root,'objects',objects)
        _check_references(root,dataset,rows)
    published=_published(root,dataset,schema);reused={}
    for row in rows:
        key=_identity(row,dataset);old=versions.get(key);h=digest(_content(row,schema))
        if key in published:
            if published[key][0]!=h:raise ValueError('catalog_identity_payload_conflict')
            receipt=published[key][1];reused[receipt['file']]=receipt;continue
        if old is not None and digest(_content(old,schema))!=h:raise ValueError('catalog_identity_payload_conflict')
        if old is None or row['available_at_ns']<old['available_at_ns']:versions[key]=row
    groups=defaultdict(list)
    for row in versions.values():
        parent=extension_part(dataset,row['venue'],row['available_at_ns'],'placeholder').rsplit('/',1)[0]
        groups[parent].append(row)
    result=list(reused.values())
    for parent,group in sorted(groups.items()):
        group=sorted(group,key=lambda r:(r.get('market_id',''),r['available_at_ns'],r[id_field]));batch_id=digest([_content(r,schema) for r in group])
        folder=Path(root)/parent;folder.mkdir(parents=True,exist_ok=True);target=folder/('part-'+batch_id[:24]+'.parquet');manifest=target.with_suffix('.manifest.json')
        if manifest.exists():
            receipt=json.loads(manifest.read_text())
            if receipt['batch_hash']!=batch_id or hashlib.sha256(target.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('immutable_catalog_part_changed')
            result.append(receipt);continue
        table=pa.Table.from_pylist(group,schema=schema);temp=target.with_name(target.name+'.'+uuid.uuid4().hex+'.part')
        try:
            pq.write_table(table,temp,**WRITER_SETTINGS)
            with pq.ParquetFile(temp,page_checksum_verification=True) as reader:
                if not reader.read().equals(table):raise ValueError('shared_catalog_readback_changed')
            with temp.open('r+b') as stream:os.fsync(stream.fileno())
            os.replace(temp,target)
        finally:temp.unlink(missing_ok=True)
        receipt=dict(schema='shared-catalog-part/2',dataset=dataset,batch_hash=batch_id,file=str(target.relative_to(root)).replace('\\','/'),
            rows=len(table),bytes=target.stat().st_size,sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
            compression='ZSTD',compression_level=9,local_readback_verified=True,source_is_platform_observation=True)
        atomic(manifest,receipt);result.append(receipt)
    return sorted(result,key=lambda r:r['file'])

def write_pair(root,descriptor):
    markets=_published(root,'markets',MARKET_SCHEMA);fees=_published(root,'fees',FEE_SCHEMA)
    for member in descriptor['members']:
        venue,mid=member.split(':',1)
        if (venue,mid,descriptor['market_versions'][member]) not in markets or (venue,'',descriptor['fee_versions'][member]) not in fees:raise ValueError('unpublished_Pair_leg_version')
    folder=Path(root)/pair_folder(descriptor['pair_id'],descriptor['actual_started_at_ns']);folder.mkdir(parents=True,exist_ok=True)
    target=folder/('relation-'+descriptor['relation_version_id'][:24]+'.json')
    if target.exists():
        if json.loads(target.read_text())!=descriptor:raise ValueError('immutable_Pair_relation_changed')
    else:atomic(target,descriptor)
    return str(target.relative_to(root)).replace('\\','/')
