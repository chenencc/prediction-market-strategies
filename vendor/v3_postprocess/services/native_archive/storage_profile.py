"""Archive-only storage profile2; never imports a venue client or changes capture.

Definite outside-window history is omitted except per-instrument starting
references. Uncertain clocks, boundary candidates and WS-linked discrepancies
stay. All page clocks/order/multiplicity and actual fee revisions survive for
retained observations. Pair JSON values are stored once in compressed Parquet.
"""
from collections import Counter,defaultdict
from copy import deepcopy
from datetime import datetime,timezone
import hashlib,json,os,re,shutil,time
from pathlib import Path
from zoneinfo import ZoneInfo
import pyarrow as pa
import pyarrow.parquet as pq
from marketdata.core import atomic,exact
from .backtest_profile import (Output,CompactTrades,PAYLOAD_FIELDS,TRADE_SCHEMA,ORDER_LEG,
    PAGE_SCHEMA,PAGE_CONTENT_SCHEMA,FEE_SCHEMA,verify_publication as verify_parent)

PROFILE='nautilus-source/2'
ROOT='v4'
CLOCK_GUARD_NS=5*10**9
HASH_MODE='canonical-lowercase-0x-hex32-or-exact-text-fallback'
ORDER_SCHEMA=pa.struct([pa.field(f.name,pa.binary(32) if f.name=='order_id' else f.type)
    for f in ORDER_LEG]+[('order_id_text',pa.string())])
CONTENT_SCHEMA=pa.schema([pa.field(f.name,pa.binary(32) if f.name=='transaction_hash' else
    pa.list_(ORDER_SCHEMA) if f.name=='legs' else f.type) for f in TRADE_SCHEMA]+
    [('transaction_hash_text',pa.string()),('retention_reason',pa.string())],metadata={b'hash_encoding':HASH_MODE.encode()})
PAGES_SCHEMA=pa.schema([pa.field('source_returned_rows' if f.name=='returned_rows' else f.name,f.type) for f in PAGE_SCHEMA]+
    [('retained_rows',pa.uint32()),('omitted_outside_rows',pa.uint32())])
PAIR_SCHEMA=pa.schema([('pair_id',pa.binary(32)),('actual_started_at_ns',pa.int64()),
    ('available_at_ns',pa.int64()),('definition_json',pa.large_string())],metadata={b'pair_id_encoding':b'lowercase-hex32-without-prefix',b'json_encoding':b'exact-original-UTF8'})
GROUP_SCHEMA=pa.schema([('group_id',pa.binary(32)),('task_type',pa.string()),('actual_started_at_ns',pa.int64()),
    ('available_at_ns',pa.int64()),('definition_json',pa.large_string())])
TRADE_DATASETS={'trade_content','trade_fees','trade_pages','trade_page_content'}


def _sha(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def _path(root,relative):
    root=Path(root).resolve();p=(root/relative).resolve()
    if not p.is_relative_to(root) or p==root:raise ValueError('archive_path_escape')
    return p


def encode_hash(value):
    if value is None:return None,None
    if not isinstance(value,str):raise ValueError('identity_must_be_string_or_null')
    if re.fullmatch(r'0x[0-9a-f]{64}',value):return bytes.fromhex(value[2:]),None
    return None,value


def decode_hash(value,text):
    if value is not None:
        if text is not None or not isinstance(value,bytes) or len(value)!=32:raise ValueError('ambiguous_binary_identity')
        return '0x'+value.hex()
    return text


def encode_payload(row,reason):
    row=deepcopy(row);row['transaction_hash'],row['transaction_hash_text']=encode_hash(row['transaction_hash'])
    for leg in row['legs']:leg['order_id'],leg['order_id_text']=encode_hash(leg['order_id'])
    row['retention_reason']=reason;return row


def decode_payload(row):
    row=deepcopy(row);row['transaction_hash']=decode_hash(row['transaction_hash'],row.pop('transaction_hash_text'))
    for leg in row['legs']:leg['order_id']=decode_hash(leg['order_id'],leg.pop('order_id_text'))
    row.pop('retention_reason',None);return row


def _entries(manifest,dataset,venue=None):
    prefix=('v4' if manifest['schema']==PROFILE else 'v3')+'/'+dataset+'/'
    if venue:prefix+='venue='+venue+'/'
    return [r for r in manifest['files'] if r['file'].startswith(prefix)]


def dataset_rows(root,manifest,dataset,venue=None):
    for entry in _entries(manifest,dataset,venue):
        with pq.ParquetFile(_path(root,entry['file']),page_checksum_verification=True) as f:
            for batch in f.iter_batches(batch_size=8192):yield from batch.to_pylist()


def read_trade_payloads(root,venue,manifest=None):
    manifest=manifest or json.loads((Path(root)/'manifest.json').read_text())
    for row in dataset_rows(root,manifest,'trade_content',venue):yield decode_payload(row) if manifest['schema']==PROFILE else row


def read_pair_relations(root,manifest=None,*,raw=False):
    manifest=manifest or json.loads((Path(root)/'manifest.json').read_text())
    if manifest['schema']==PROFILE:
        for row in dataset_rows(root,manifest,'pairs'):
            value=row['definition_json'];obj=exact(value)
            if obj['pair_id']!=row['pair_id'].hex():raise ValueError('Pair_identity_binding_changed')
            yield value if raw else obj
    else:
        for row in manifest.get('auxiliary_files',[]):
            if '/pairs/' in row['file']:
                value=_path(root,row['file']).read_bytes().decode('utf-8');yield value if raw else exact(value)


def _indexed(rows,key):
    out={}
    for row in rows:
        if row[key] in out:raise ValueError('duplicate_archive_reference:'+key)
        out[row[key]]=row
    return out


def trade_observations(root,venue,manifest=None):
    """Restores source page positions and availability; never hides duplicates."""
    manifest=manifest or json.loads((Path(root)/'manifest.json').read_text())
    payloads=_indexed(read_trade_payloads(root,venue,manifest),'payload_id')
    fees=_indexed(dataset_rows(root,manifest,'trade_fees',venue),'fee_bundle_id')
    contents=_indexed(dataset_rows(root,manifest,'trade_page_content',venue),'content_id')
    for page in sorted(dataset_rows(root,manifest,'trade_pages',venue),key=lambda r:r['sequence']):
        occurrences=contents[page['content_id']]['occurrences']
        expected=page['retained_rows'] if manifest['schema']==PROFILE else page['returned_rows']
        if len(occurrences)!=expected:raise ValueError('page_retained_count_changed')
        if manifest['schema']==PROFILE and page['source_returned_rows']!=page['retained_rows']+page['omitted_outside_rows']:
            raise ValueError('page_retention_accounting_changed')
        last=-1
        for occurrence in occurrences:
            if occurrence['row_index']<=last:raise ValueError('page_order_changed')
            last=occurrence['row_index'];payload=payloads[occurrence['payload_id']]
            if payload['market_id']!=page['market_id']:raise ValueError('page_market_binding_changed')
            charge=fees[payload['fee_bundle_id']]['legs'] if payload['fee_bundle_id'] is not None else []
            yield page,occurrence['row_index'],payload,charge


def _reference_key(row):return row['market_id'],row.get('asset_id'),row.get('index_set')


def _wire_key(condition,transaction,asset,side):
    def text(value):return '0x'+value.hex() if isinstance(value,bytes) else str(value).lower() if value is not None else None
    if any(x is None for x in (condition,transaction,asset,side)):return None
    asset=str(int.from_bytes(asset,'big')) if isinstance(asset,bytes) else str(asset)
    return text(condition),text(transaction),asset,str(side).upper()


def _ws_links(source,manifest,window):
    links=set();conditions={}
    for row in dataset_rows(source,manifest,'instruments','polymarket'):conditions[str(row['market_id'])]=row.get('condition_id')
    for entry in _entries(manifest,'events','polymarket'):
        if not entry['file'].endswith('/last_trade_price.parquet'):continue
        with pq.ParquetFile(_path(source,entry['file'])) as f:
            t=f.read()
            if 'timestamp' in t.column_names:t=t.set_column(t.schema.get_field_index('timestamp'),'timestamp',t['timestamp'].cast(pa.int64()))
            for row in t.to_pylist():
                execution=row.get('timestamp')
                if execution is None or not window['start_ns']<=execution*10**6<window['end_ns']:continue
                key=_wire_key(row.get('market'),row.get('transaction_hash'),row.get('asset_id'),row.get('side'))
                if key:links.add(key)
    return links,conditions


def retention_reasons(payloads,window,*,ws_links=(),conditions=None):
    """Conservative omission. Source membership alone is never enough to drop."""
    start,end=window['start_ns'],window['end_ns'];references={};result={};conditions=conditions or {}
    if type(start) is not int or type(end) is not int or not 0<=start<end:raise ValueError('valid_finite_window_required')
    for row in payloads:
        execution=row.get('execution_ns');precision=row.get('source_precision_ns');key=_reference_key(row)
        if type(execution) is int and type(precision) is int and precision>0 and execution+precision<=start:
            references[key]=max(references.get(key,execution),execution)
    for row in payloads:
        ident=row['payload_id'];execution=row.get('execution_ns');precision=row.get('source_precision_ns');membership=row.get('window_membership')
        if membership!='outside':reason='window_or_uncertain_membership'
        elif type(execution) is not int or type(precision) is not int or precision<=0:reason='unknown_execution_clock'
        elif not (execution+precision<=start or execution>=end):reason='membership_clock_conflict'
        elif execution+precision>start-CLOCK_GUARD_NS and execution<end+CLOCK_GUARD_NS:reason='boundary_clock_guard'
        elif _wire_key(conditions.get(str(row['market_id'])),row.get('transaction_hash'),row.get('asset_id'),row.get('side')) in ws_links:reason='known_WS_clock_disagreement'
        elif execution==references.get(_reference_key(row)):reason='starting_reference_available_at_original_receipt'
        else:continue
        result[ident]=reason
    return result


def _roundtrip_hash(root,manifest,venue):
    digest=hashlib.sha256();count=fee_count=0
    for page,index,row,fees in trade_observations(root,venue,manifest):
        CompactTrades._update(digest,page,page['source_market_version_id'],index,{k:row[k] for k in PAYLOAD_FIELDS},fees)
        count+=1;fee_count+=len(fees)
    return dict(ordered_financial_sha256=digest.hexdigest(),retained_observations=count,reported_fee_legs=fee_count)


def _copy_unchanged(source,destination,entry):
    old=_path(source,entry['file']);relative=entry['file'].replace('v3/','v4/',1);new=_path(destination,relative);new.parent.mkdir(parents=True,exist_ok=True)
    try:os.link(old,new)
    except OSError:shutil.copyfile(old,new)
    if _sha(new)!=entry['sha256']:raise ValueError('unchanged_payload_copy_changed')
    return dict(entry,file=relative)


def _project(source,destination,parent):
    started=time.perf_counter();window=parent['window'];out=Output(destination);copied=[];aux=[];proofs={};links,conditions=_ws_links(source,parent,window)
    for entry in parent['files']:
        dataset=entry['file'].split('/')[1]
        if dataset not in TRADE_DATASETS:copied.append(_copy_unchanged(source,destination,entry))
    for entry in parent.get('auxiliary_files',[]):
        if '/pairs/' not in entry['file'] and '/groups/' not in entry['file']:aux.append(_copy_unchanged(source,destination,entry))
    venues=sorted({r['file'].split('venue=',1)[1].split('/',1)[0] for r in _entries(parent,'trade_content')})
    for venue in venues:
        payloads=list(read_trade_payloads(source,venue,parent));by_id=_indexed(payloads,'payload_id')
        reasons=retention_reasons(payloads,window,ws_links=links if venue=='polymarket' else (),conditions=conditions)
        source_fees=_indexed(dataset_rows(source,parent,'trade_fees',venue),'fee_bundle_id')
        source_contents=_indexed(dataset_rows(source,parent,'trade_page_content',venue),'content_id')
        fee_ids=sorted({by_id[i]['fee_bundle_id'] for i in reasons if by_id[i]['fee_bundle_id'] is not None});fee_map={old:new for new,old in enumerate(fee_ids)}
        fees=[dict(source_fees[old],fee_bundle_id=fee_map[old]) for old in fee_ids]
        payload_map={old:new for new,old in enumerate(sorted(reasons))};encoded=[]
        for old,new in payload_map.items():
            row=dict(by_id[old],payload_id=new);row['fee_bundle_id']=fee_map[row['fee_bundle_id']] if row['fee_bundle_id'] is not None else None
            encoded.append(encode_payload(row,reasons[old]))
        pages=[];contents=[];content_index={};expected=hashlib.sha256();retained=omitted=source_observations=0;fee_observations=0
        for page in sorted(dataset_rows(source,parent,'trade_pages',venue),key=lambda r:r['sequence']):
            original=source_contents[page['content_id']]['occurrences'];occurrences=[];last=-1
            if len(original)!=page['returned_rows']:raise ValueError('source_page_count_changed')
            for occurrence in original:
                index=occurrence['row_index'];old=occurrence['payload_id'];row=by_id[old]
                if index!=last+1 or row['market_id']!=page['market_id']:raise ValueError('source_page_binding_changed')
                last=index;source_observations+=1
                if old not in reasons:omitted+=1;continue
                occurrences.append(dict(row_index=index,payload_id=payload_map[old]));charge=source_fees[row['fee_bundle_id']]['legs'] if row['fee_bundle_id'] is not None else []
                CompactTrades._update(expected,page,page['source_market_version_id'],index,{k:row[k] for k in PAYLOAD_FIELDS},charge)
                retained+=1;fee_observations+=len(charge)
            key=tuple((r['row_index'],r['payload_id']) for r in occurrences)
            if key not in content_index:content_index[key]=len(contents);contents.append(dict(content_id=len(contents),occurrences=occurrences))
            new={k:page[k] for k in PAGE_SCHEMA.names if k not in {'returned_rows','content_id'}}
            new.update(source_returned_rows=page['returned_rows'],retained_rows=len(occurrences),omitted_outside_rows=page['returned_rows']-len(occurrences),content_id=content_index[key]);pages.append(new)
        day=time.strftime('%Y-%m-%d',time.gmtime(window['start_ns']//10**9))
        for dataset,data,schema in [('trade_content',encoded,CONTENT_SCHEMA),('trade_fees',fees,FEE_SCHEMA),('trade_page_content',contents,PAGE_CONTENT_SCHEMA),('trade_pages',pages,PAGES_SCHEMA)]:
            partitions={day:data}
            if dataset=='trade_pages' and data:
                partitions=defaultdict(list)
                for row in data:partitions[time.strftime('%Y-%m-%d',time.gmtime(row['available_at_ns']//10**9))].append(row)
            for partition,values in sorted(partitions.items()):
                out.write(f'{ROOT}/{dataset}/venue={venue}/date={partition}/part-core.parquet',pa.Table.from_pylist(values,schema=schema))
        partial=dict(schema=PROFILE,files=copied+out.receipts)
        saved_pages=sorted(dataset_rows(destination,partial,'trade_pages',venue),key=lambda r:r['sequence'])
        if saved_pages!=pages:raise ValueError('page_metadata_roundtrip_changed')
        page_hash=hashlib.sha256(json.dumps(pages,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        actual=_roundtrip_hash(destination,partial,venue)
        if actual!=dict(ordered_financial_sha256=expected.hexdigest(),retained_observations=retained,reported_fee_legs=fee_observations):raise ValueError('retained_financial_roundtrip_changed')
        proofs[venue]=dict(actual,source_observations=source_observations,omitted_definitely_outside_observations=omitted,
            source_payload_versions=len(payloads),retained_payload_versions=len(encoded),omitted_definitely_outside_payload_versions=len(payloads)-len(encoded),
            retention_reasons=dict(Counter(reasons.values())),pages=len(pages),page_metadata_sha256=page_hash,all_page_availability_clocks_preserved=True,
            all_retained_financial_fields_and_occurrences_verified=True)
    pairs=defaultdict(list);expected_pairs=[]
    for entry in parent.get('auxiliary_files',[]):
        if '/pairs/' not in entry['file']:continue
        raw=_path(source,entry['file']).read_bytes().decode('utf-8');row=exact(raw);ident=row['pair_id']
        if not re.fullmatch('[0-9a-f]{64}',ident):raise ValueError('canonical_Pair_identity_required')
        stamp=row['actual_started_at_ns'];day=datetime.fromtimestamp(stamp//10**9,timezone.utc).astimezone(ZoneInfo(row.get('start_date_timezone') or 'Asia/Shanghai')).strftime('%Y-%m-%d')
        pairs[day].append(dict(pair_id=bytes.fromhex(ident),actual_started_at_ns=stamp,available_at_ns=row.get('available_at_ns'),definition_json=raw));expected_pairs.append(raw)
    for day,data in sorted(pairs.items()):out.write(f'{ROOT}/pairs/start_date={day}/part-relations.parquet',pa.Table.from_pylist(data,schema=PAIR_SCHEMA))
    groups=defaultdict(list);expected_groups=[]
    for entry in parent.get('auxiliary_files',[]):
        if '/groups/' not in entry['file']:continue
        raw=_path(source,entry['file']).read_text(encoding='utf-8');row=json.loads(raw)
        if not re.fullmatch('[0-9a-f]{64}',row['group_id']):raise ValueError('canonical_group_identity_required')
        day=datetime.fromtimestamp(row['actual_started_at_ns']//10**9,timezone.utc).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d')
        groups[(row['task_type'],day)].append(dict(group_id=bytes.fromhex(row['group_id']),task_type=row['task_type'],
            actual_started_at_ns=row['actual_started_at_ns'],available_at_ns=row['available_at_ns'],definition_json=raw));expected_groups.append(raw)
    for (kind,day),data in sorted(groups.items()):out.write(f'{ROOT}/groups/task_type={kind}/start_date={day}/part-relations.parquet',pa.Table.from_pylist(data,schema=GROUP_SCHEMA))
    partial=dict(schema=PROFILE,files=copied+out.receipts)
    if Counter(read_pair_relations(destination,partial,raw=True))!=Counter(expected_pairs):raise ValueError('Pair_JSON_roundtrip_changed')
    if len(expected_pairs)!=parent['pair_relations']:raise ValueError('Pair_relation_count_changed')
    if len(expected_groups)!=parent.get('group_relations',0):raise ValueError('group_relation_count_changed')
    return dict(schema=PROFILE,window=window,created_at_ns=time.time_ns(),files=copied+out.receipts,auxiliary_files=aux,pair_relations=len(expected_pairs),group_relations=len(expected_groups),
        group_definitions_sha256=hashlib.sha256(json.dumps(sorted(expected_groups),ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
        trades=proofs,retention_policy=dict(version='window-and-starting-reference/1',clock_guard_ns=CLOCK_GUARD_NS,
            omitted='only_provably_outside_window_not_starting_reference',uncertain_and_WS_linked_rows_preserved=True,
            starting_references_keep_original_receipt_availability=True,pagination_source_and_retained_counts_separate=True),
        hash_encoding=HASH_MODE,Pair_JSON_roundtrip_verified=True,all_retained_financial_roundtrips_verified=True,
        all_source_trade_payloads_retained=False,full_source_coverage_certified=False,
        unchanged_book_instrument_fee_quality_and_gap_payloads=True,content_id_is_not_a_venue_fill_id=True,
        content_reference_scope='this_archive_manifest_and_venue',ordered_retained_occurrences_and_revisions_preserved=True,
        parent_manifest_sha256=_sha(source/'manifest.json'),parent_profile=parent['schema'],
        parent_parquet_bytes=parent['parquet_bytes'],parent_Pair_JSON_bytes=sum(_path(source,r['file']).stat().st_size for r in parent.get('auxiliary_files',[]) if '/pairs/' in r['file']),
        parquet_bytes=sum(r['bytes'] for r in copied+out.receipts),seconds=time.perf_counter()-started,
        source_payloads_deleted=False,storage_code_sha256=_sha(__file__),
        **{k:deepcopy(parent[k]) for k in ('source_inventory','source_files','source_signature','source_bytes') if k in parent})


def verify_storage(root,proof=None,*,semantic=True):
    root=Path(root).resolve();saved=json.loads((root/'manifest.json').read_text())
    if proof is not None and saved!=proof:raise ValueError('storage_manifest_changed')
    proof=saved
    if proof.get('schema')!=PROFILE or not proof.get('all_retained_financial_roundtrips_verified') or not proof.get('Pair_JSON_roundtrip_verified'):raise ValueError('verified_profile2_receipt_required')
    for entry in proof['files']+proof.get('auxiliary_files',[]):
        if _sha(_path(root,entry['file']))!=entry['sha256']:raise ValueError('storage_payload_changed')
    if semantic:
        groups=list(dataset_rows(root,proof,'groups'))
        if len(groups)!=proof.get('group_relations',0):raise ValueError('group_relation_count_changed')
        definitions=[]
        for row in groups:
            definition=json.loads(row['definition_json'])
            if (definition['group_id']!=row['group_id'].hex() or definition['task_type']!=row['task_type']
                or definition['actual_started_at_ns']!=row['actual_started_at_ns'] or definition['available_at_ns']!=row['available_at_ns']):
                raise ValueError('group_identity_or_clock_binding_changed')
            definitions.append(row['definition_json'])
        if groups:
            expected=hashlib.sha256(json.dumps(sorted(definitions),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
            if expected!=proof.get('group_definitions_sha256'):raise ValueError('group_definition_roundtrip_changed')
        for venue,expected in proof['trades'].items():
            pages=sorted(dataset_rows(root,proof,'trade_pages',venue),key=lambda r:r['sequence'])
            page_hash=hashlib.sha256(json.dumps(pages,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            if len(pages)!=expected['pages'] or page_hash!=expected['page_metadata_sha256']:raise ValueError('page_metadata_roundtrip_changed')
            actual=_roundtrip_hash(root,proof,venue)
            if any(expected[k]!=v for k,v in actual.items()):raise ValueError('retained_financial_roundtrip_changed')
        if sum(1 for _ in read_pair_relations(root,proof))!=proof['pair_relations']:raise ValueError('Pair_relation_count_changed')
    return proof


def optimize_archive(source,destination):
    """Transactional upgrade of a closed, verified profile1; never removes source."""
    source=Path(source).resolve();destination=Path(destination).resolve()
    if source==destination or destination.is_relative_to(source) or source.is_relative_to(destination):raise ValueError('distinct_output_root_required')
    parent=json.loads((source/'manifest.json').read_text());verify_parent(source,parent)
    if parent['schema']!='nautilus-source/1':raise ValueError('verified_profile1_input_required')
    parent_hash=_sha(source/'manifest.json')
    if destination.exists():
        proof=verify_storage(destination)
        if proof['parent_manifest_sha256']!=parent_hash:raise ValueError('published_storage_input_changed')
        return proof
    work=destination.with_name('.s2-'+hashlib.sha256(destination.name.encode()).hexdigest()[:10]);intent=dict(profile=PROFILE,source=str(source),parent_manifest_sha256=parent_hash,destination=str(destination))
    if work.exists():
        marker=work/'intent.json'
        if not marker.exists() or json.loads(marker.read_text())!=intent:raise ValueError('incomplete_storage_ownership_mismatch')
        if not work.resolve().is_relative_to(destination.parent) or work in (source,destination):raise ValueError('unsafe_storage_work_path')
        shutil.rmtree(work)
    work.mkdir(parents=True);atomic(work/'intent.json',intent)
    proof=_project(source,work,parent)
    verify_parent(source,parent)
    if _sha(source/'manifest.json')!=parent_hash:raise ValueError('source_manifest_changed_during_storage_projection')
    atomic(work/'manifest.json',proof);verify_storage(work,proof)
    (work/'intent.json').unlink();work.rename(destination)
    return proof


def compact_for_storage(source,destination,*,window,source_gaps=()):
    """Future finalizers: verified native projection -> profile2, no receive work.

    The temporary profile1 archive is deleted only after the exact profile2
    receipt, all retained financial replay and full native-source binding exist.
    Existing historical profile1 archives passed to optimize_archive are retained.
    """
    from .backtest_profile import compact_archive,source_signature
    source=Path(source).resolve();destination=Path(destination).resolve()
    legacy=destination.with_name('.storage1-'+hashlib.sha256(destination.name.encode()).hexdigest()[:10])
    if destination.exists():
        proof=verify_storage(destination)
        if proof['window']!=window:raise ValueError('published_storage_window_changed')
        if source.exists():
            if proof.get('source_signature')!=source_signature(source):raise ValueError('published_storage_source_changed')
        else:
            marker=destination/'source-retirement-intent.json';intent=json.loads(marker.read_text()) if marker.exists() else {}
            if intent.get('source')!=str(source) or not intent.get('verified'):raise ValueError('retired_source_ownership_mismatch')
    else:
        compact_archive(source,legacy,window=window,source_gaps=source_gaps)
        proof=optimize_archive(legacy,destination)
    if legacy.exists():
        if legacy.parent!=destination.parent or not legacy.is_relative_to(destination.parent) or legacy in (source,destination):raise ValueError('unsafe_intermediate_retirement_path')
        old=json.loads((legacy/'manifest.json').read_text());verify_parent(legacy,old)
        if _sha(legacy/'manifest.json')!=proof['parent_manifest_sha256']:raise ValueError('intermediate_ownership_mismatch')
        expected={r['file'] for r in old['files']+old.get('auxiliary_files',[])}|{'manifest.json'}
        actual={p.relative_to(legacy).as_posix() for p in legacy.rglob('*') if p.is_file()}
        if actual!=expected:raise ValueError('unexpected_intermediate_payload')
        # Resolved sibling path, bound manifest, exact inventory and every SHA
        # were checked before this scoped removal. Native inputs remain intact.
        shutil.rmtree(legacy)
    return proof
