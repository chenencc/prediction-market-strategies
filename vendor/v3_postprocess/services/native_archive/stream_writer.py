"""Bounded, single-owner native archive projection with one cold read per part.

Uses the same schemas, shared-object identities and ZSTD profile as the ordinary
archive writer. Only semantic definitions are deduplicated; source observations
retain their availability clocks. Raw sources are never removed here.
"""
from collections import defaultdict
import hashlib
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from marketdata.core import atomic
from .writer import SCHEMAS, _identity, _content
from .fees import digest
from .layout import extension_part, WRITER_SETTINGS


def verify_archive_runtime():
    """Reject incomplete archive environments before any market recording."""
    from importlib import import_module
    try:
        pandas=import_module('pandas')
    except ImportError as exc:
        raise RuntimeError('archive_nanosecond_dependency_missing:install_services/native_archive/requirements.txt') from exc
    schema=pa.schema([('received',pa.timestamp('ns',tz='UTC')),('levels',pa.list_(pa.struct([('price',pa.string())])))])
    clock=1793287674848396937
    table=pa.Table.from_pylist([dict(received=clock,levels=[dict(price='0.123456789012345678')])],schema=schema)
    sink=pa.BufferOutputStream()
    pq.write_table(table,sink,compression='zstd',write_page_checksum=True)
    with pq.ParquetFile(pa.BufferReader(sink.getvalue()),page_checksum_verification=True) as reader:
        actual=reader.read()
    if actual['received'].cast(pa.int64())[0].as_py()!=clock or arrow_digest(actual,schema)!=arrow_digest(table,schema):
        raise RuntimeError('archive_nanosecond_roundtrip_failed')
    return dict(pyarrow=pa.__version__,pandas=pandas.__version__,nanosecond_nested_parquet_roundtrip=True)


def _canonical_buffer(array):
    """Reuse only complete, null-free buffers with stable Python roundtrips.

    Null slots may contain arbitrary hidden values, including struct children.
    Slice offsets and dictionary/float/decimal representations also need the
    original canonicalization. Large non-null book level arrays need none of it.
    """
    if array.null_count or array.offset:return False
    t=array.type
    if pa.types.is_struct(t):return all(_canonical_buffer(array.field(i)) for i in range(t.num_fields))
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        width=8 if pa.types.is_large_list(t) else 4
        return (array.buffers()[1].size==(len(array)+1)*width and array.offsets[0].as_py()==0
                and array.offsets[-1].as_py()==len(array.values) and _canonical_buffer(array.values))
    if pa.types.is_fixed_size_list(t):return len(array)*t.list_size==len(array.values) and _canonical_buffer(array.values)
    buffers=array.buffers()
    # A prefix slice can have offset zero while retaining trailing values.
    # IPC can include those bytes (and boolean padding bits) in its digest.
    if pa.types.is_boolean(t):return False
    if pa.types.is_string(t) or pa.types.is_binary(t) or pa.types.is_large_string(t) or pa.types.is_large_binary(t):
        width=8 if pa.types.is_large_string(t) or pa.types.is_large_binary(t) else 4
        offsets=buffers[1]
        if offsets.size!=(len(array)+1)*width:return False
        first=int.from_bytes(offsets.slice(0,width).to_pybytes(),'little',signed=True)
        last=int.from_bytes(offsets.slice(len(array)*width,width).to_pybytes(),'little',signed=True)
        return first==0 and last==buffers[2].size
    if pa.types.is_integer(t) or pa.types.is_timestamp(t) or pa.types.is_fixed_size_binary(t):
        return buffers[1].size==len(array)*(t.bit_width//8)
    return False


def arrow_digest(table, schema):
    # Retain byte-identical legacy IPC hashes, including nested field names.
    # Only nullable/unsupported columns take the Python canonicalization path.
    table=table.cast(schema).combine_chunks()
    arrays=[]
    for column,field in zip(table.columns,schema):
        if column.num_chunks==1 and _canonical_buffer(column.chunk(0)):arrays.append(column.chunk(0))
        else:arrays.append(pa.array(column.to_pylist(),type=field.type))
    table=pa.Table.from_arrays(arrays,schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue()).hexdigest()


def buffered_size(value):
    size=sys.getsizeof(value)
    if isinstance(value,dict):return size+sum(buffered_size(k)+buffered_size(v) for k,v in value.items())
    if isinstance(value,(list,tuple)):return size+sum(buffered_size(v) for v in value)
    return size


class StreamArchive:
    def __init__(self, root, *, rows_per_group=65536, buffer_bytes=16*1024**2):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
        self.rows_per_group=rows_per_group;self.buffers=defaultdict(list)
        self.buffer_bytes=buffer_bytes;self.pending_bytes=defaultdict(int)
        self.open={};self.schemas={};self.hashes=defaultdict(list)
        self.definitions={k:{} for k in ('objects','markets','fees')}
        self.counts=defaultdict(int);self.finished=False

    def catalog(self, dataset, row):
        if dataset!='objects':
            for obj in row.get('_objects',[]):self.catalog('objects',obj)
        schema=SCHEMAS[dataset]
        if dataset in self.definitions:
            key=_identity(row,dataset);content=digest(_content(row,schema))
            previous=self.definitions[dataset].get(key)
            if previous is not None:
                if previous!=content:raise ValueError('catalog_identity_payload_conflict')
                return
            self.definitions[dataset][key]=content
        refs={'markets':['instrument_set_ref','parent_event_set_ref','extra_metadata_ref'],
              'fees':['extra_policy_ref'],'reported_fees':['extra_fee_ref'],
              'api_trades':['taker_ref','makers_ref','extra_trade_ref'],
              'quality':['detail_ref']}.get(dataset,[])
        for field in refs:
            if row.get(field) and ('shared','',row[field]) not in self.definitions['objects']:
                raise ValueError('unpublished_shared_object_reference')
        if dataset in {'market_observations','api_trades'}:
            version=row['market_version_id' if dataset=='market_observations' else 'source_market_version_id']
            if (row['venue'],row['market_id'],version) not in self.definitions['markets']:
                raise ValueError('unpublished_market_version')
        if dataset=='market_observations' and (row['venue'],'',row['fee_version_id']) not in self.definitions['fees']:
            raise ValueError('unpublished_fee_version')
        path=extension_part(dataset,row.get('venue','shared'),row['available_at_ns'],'native-batch')
        self.event(path,schema,row);self.counts[dataset]+=1

    def event(self, path, schema, row):
        if self.finished:raise ValueError('archive_already_closed')
        if path not in self.schemas:self.schemas[path]=schema
        if self.schemas[path]!=schema:raise ValueError('archive_schema_changed')
        self.buffers[path].append(row)
        self.pending_bytes[path]+=buffered_size(row)
        if len(self.buffers[path])>=self.rows_per_group or self.pending_bytes[path]>=self.buffer_bytes:self.flush(path)

    def empty(self,path,schema):
        self.schemas.setdefault(path,schema);self.buffers.setdefault(path,[])

    def flush(self,path):
        schema=self.schemas[path];rows=self.buffers[path]
        if path not in self.open:
            target=self.root/path;target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists() or target.with_suffix('.parquet.part').exists():raise ValueError('archive_part_already_exists')
            settings={k:v for k,v in WRITER_SETTINGS.items() if k!='row_group_size'}
            repeated={'market_id','asset_id','source_market_version_id','transaction_hash','execution_reference',
                'settlement_id','taker_ref','makers_ref','extra_trade_ref','leg','fee_unit','fee_status'}
            settings['use_dictionary']=[c for c in schema.names if c in repeated]
            self.open[path]=pq.ParquetWriter(target.with_suffix('.parquet.part'),schema,write_page_checksum=True,**settings)
        if rows:
            table=pa.Table.from_pylist(rows,schema=schema)
            self.hashes[path].append(arrow_digest(table,schema))
            self.open[path].write_table(table,row_group_size=len(table));rows.clear();self.pending_bytes[path]=0

    def close(self):
        if self.finished:raise ValueError('archive_already_closed')
        for path in self.schemas:self.flush(path)
        for writer in self.open.values():writer.close()
        receipts=[]
        for path,schema in self.schemas.items():
            target=self.root/path;temp=target.with_suffix('.parquet.part');rows=0
            with pq.ParquetFile(temp,page_checksum_verification=True) as reader:
                if reader.num_row_groups!=len(self.hashes[path]):raise ValueError('archive_row_groups_changed')
                for index,expected in enumerate(self.hashes[path]):
                    table=reader.read_row_group(index);rows+=len(table)
                    if arrow_digest(table,schema)!=expected:raise ValueError('archive_cold_readback_changed')
            temp.rename(target)
            with target.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
            receipt=dict(schema='shared-catalog-part/2',file=path,rows=rows,bytes=target.stat().st_size,
                sha256=sha,compression='ZSTD',compression_level=9,local_readback_verified=True,
                source_is_platform_observation=True,row_group_count=len(self.hashes[path]),
                batch_hash=digest(self.hashes[path]),dataset=path.split('/')[1])
            atomic(target.with_suffix('.manifest.json'),receipt);receipts.append(receipt)
        self.finished=True
        return receipts

    def abort(self):
        for writer in self.open.values():
            try:writer.close()
            except Exception:pass
