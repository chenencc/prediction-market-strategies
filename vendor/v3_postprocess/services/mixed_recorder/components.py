"""Independent immutable book/metadata and trade components, same typed v4 files.

No receive loop or provider client is imported. A book phase can seal without a
trade file or final trade walk. Cross-component data is referenced, never copied.
Dependency chains are bounded; reaching the limit requires a replay checkpoint,
not an unbounded upload/replay or silent omission.
"""
from copy import deepcopy
import hashlib,json
from pathlib import Path
import re
import pyarrow.parquet as pq
from marketdata.core import atomic
from native_archive.storage_profile import (PROFILE,_roundtrip_hash,dataset_rows,read_pair_relations)
from task_control.contracts import digest,evidence,integer,require,seal,verified
from .archive_replay import replay_book_parts,rows,final_states
from .publication import safe_relative,bound_file,sha_file,backend_scope
from .store import sync_directory

BOOK_DATASETS={'events','instruments','instrument_versions','fee_policies','quality','gaps','pairs','groups','book_checkpoints'}
TRADE_DATASETS={'trade_content','trade_pages','trade_page_content','trade_fees','quality'}
CATALOG_DATASETS={'instruments','instrument_versions','fee_policies','pairs','groups'}
MAX_DEPENDENCIES=16


def check_replay_checkpoint(value):
    row=verified(value,'native-book-replay-checkpoint/1')
    require(0<len(row['source_components'])<=MAX_DEPENDENCIES+1,'bounded_checkpoint_source_components_required')
    sources=[check_dependency(r) for r in row['source_components']]
    require(len({r['component_id'] for r in sources})==len(sources) and row['source_head'] in sources,
            'checkpoint_source_head_binding_required')
    require(row['observed_event'] is False and row['source_coverage_certified'] is False,'checkpoint_is_derived_state_only')
    integer(row['as_of_ns'],'checkpoint_clock_required')
    require(re.fullmatch('[0-9a-f]{64}',row['source_replay_sha256']),'checkpoint_source_replay_hash_required')
    require(set(row['final_states']) in ({'polymarket','predictfun'},{'polymarket','predictfun','myriad'}),'checkpoint_venue_states_required')
    for state in row['final_states'].values():
        integer(state['assets'],'checkpoint_asset_count_required')
        require(re.fullmatch('[0-9a-f]{64}',state['final_state_sha256']),'checkpoint_state_hash_required')
    return row


def check_dependency(value):
    row=verified(value,'native-sealed-dependency/1')
    require(row['component']=='books_metadata','book_catalog_dependency_required')
    for field in ('component_id','manifest_semantic_sha256','files_sha256'):
        require(re.fullmatch('[0-9a-f]{64}',row[field]),'sealed_dependency_hash_required')
    return row


def component_reference(root):
    m=_current_manifest(root)
    require(m['component']=='books_metadata','shared_book_component_required')
    return seal('native-sealed-dependency/1',component='books_metadata',component_id=m['component_contract']['contract_hash'],
        manifest_semantic_sha256=digest({k:v for k,v in m.items() if k!='local_component_replay'}),files_sha256=digest(m['files']))


def check_remote_dependency(value):
    row=verified(value,'native-component-dependency/1')
    require(re.fullmatch('[0-9a-f]{64}',row['publication_id']) and re.fullmatch('[0-9a-f]{64}',row['component_id']) and re.fullmatch('[0-9a-f]{40}',row['revision']),
            'exact_component_dependency_identity_required')
    safe_relative(row['manifest_remote_path'])
    require(row['manifest_remote_path'].startswith('collections/') and row['component']=='books_metadata',
            'only_verified_book_catalog_dependencies_supported')
    for field in ('public_manifest_sha256','files_sha256','readback_receipt_hash'):
        require(re.fullmatch('[0-9a-f]{64}',row[field]),'component_dependency_hash_required')
    return row


def materialize_dependencies(backend,references,resolved,destination,*,catalog_only=False):
    """Fresh exact-revision reads of bounded DAG nodes, no implicit HEAD/cache."""
    destination=Path(destination);seen={};visiting=set();total=0
    def fetch(value,remote_values):
        nonlocal total
        logical=check_dependency(value);key=logical['component_id']
        candidates=[r for r in remote_values if r.get('component_id')==key]
        require(len(candidates)==1,'exact_dependency_resolution_required')
        ref=check_remote_dependency(candidates[0])
        require(ref['backend_identity']==backend_scope(backend),'dependency_backend_scope_changed')
        require(key not in visiting,'component_dependency_cycle')
        if key in seen:
            require(seen[key]==ref,'component_dependency_identity_conflict');return
        require(len(seen)+len(visiting)<MAX_DEPENDENCIES,'component_checkpoint_required_dependency_bound')
        visiting.add(key);root=destination/key;root.mkdir(parents=True,exist_ok=False)
        marker=Path(backend.download(ref['revision'],ref['manifest_remote_path'],root/'publication.json'))
        require(marker.resolve()==(root/'publication.json').resolve() and marker.stat().st_size<=2*1024**2 and sha_file(marker)==ref['public_manifest_sha256'],
                'exact_dependency_manifest_changed')
        public=json.loads(marker.read_text(encoding='utf8'))
        require(public['publication_id']==ref['publication_id'] and public['component_id']==key and public['component']=='books_metadata' and
                public['archive_profile']==PROFILE and digest(public['files'])==ref['files_sha256'],
                'dependency_publication_binding_changed')
        require(0<len(public['files'])<=96,'dependency_file_count_bound')
        manifest=public['archive_manifest'];require(manifest.get('component')=='books_metadata','dependency_kind_changed')
        require(manifest['component_contract']['contract_hash']==key and digest(manifest['files'])==logical['files_sha256'] and
                digest({k:v for k,v in manifest.items() if k!='local_component_replay'})==logical['manifest_semantic_sha256'],
                'resolved_dependency_not_the_sealed_component')
        for row in public['files']:
            safe_relative(row['remote_path']);safe_relative(row['logical_path'])
            if catalog_only and row['logical_path'].split('/')[1] not in CATALOG_DATASETS:continue
            total+=row['bytes'];require(total<=512*1024**2,'component_checkpoint_required_byte_bound')
            target=root/row['logical_path'];target.parent.mkdir(parents=True,exist_ok=True)
            path=Path(backend.download(ref['revision'],row['remote_path'],target))
            require(path.resolve()==target.resolve() and path.stat().st_size==row['bytes'] and sha_file(path)==row['sha256'],
                    'exact_dependency_payload_changed')
        (root/'manifest.json').write_text(json.dumps(manifest),encoding='utf8')
        for parent in manifest['dependencies']:fetch(parent,public['resolved_dependencies'])
        visiting.remove(key);seen[key]=ref
    for reference in references:fetch(reference,resolved)
    return seen


def _current_manifest(root,manifest=None,*,catalog_only=False):
    root=Path(root).resolve();m=deepcopy(manifest) if manifest is not None else json.loads(bound_file(root,'manifest.json').read_text(encoding='utf8'))
    require(m.get('schema')==PROFILE,'native_profile2_component_required')
    contract=verified(m['component_contract'],'native-component-seal/1')
    require(contract['component'] in {'books_metadata','trades'} and contract['component']==m['component'],
            'independent_component_kind_required')
    require(contract['files_sha256']==digest(m['files']) and contract['window']==m['window'],
            'component_inventory_or_clock_changed')
    require(contract['dependencies']==m['dependencies'],'component_dependency_set_changed')
    require(contract['native_fences']==m['native_fences'],'component_native_fence_changed')
    require(contract.get('checkpoint')==m.get('checkpoint'),'component_checkpoint_contract_changed')
    if 'myriad_native_projection' in m:
        require(contract.get('myriad_projection_sha256')==digest(m['myriad_native_projection']),'component_myriad_projection_binding_changed')
    if m.get('checkpoint'):
        checkpoint=check_replay_checkpoint(m['checkpoint'])
        require(m['component']=='books_metadata' and not m['dependencies'] and checkpoint['as_of_ns']==m['window']['end_ns'],
                'self_contained_book_checkpoint_required')
    allowed=BOOK_DATASETS if m['component']=='books_metadata' else TRADE_DATASETS
    seen=set()
    for entry in m['files']:
        name=entry['file'];safe_relative(name)
        require(name not in seen and name.startswith('v4/') and Path(name).suffix=='.parquet','unique_typed_component_parts_required')
        seen.add(name);require(name.split('/')[1] in allowed,'cross_component_payload_not_allowed')
        if m.get('checkpoint'):require(name.split('/')[1] in CATALOG_DATASETS|{'book_checkpoints'},'checkpoint_cannot_copy_historical_events')
        if name.split('/')[1]=='book_checkpoints':require(bool(m.get('checkpoint')),'derived_checkpoint_contract_required')
        if not catalog_only or name.split('/')[1] in CATALOG_DATASETS:
            file=bound_file(root,name)
            require(file.stat().st_size==entry['bytes'] and sha_file(file)==entry['sha256'],'sealed_component_part_changed')
    require(0<len(m['files'])<=96 and sum(f['bytes'] for f in m['files'])<=128*1024**2,'bounded_native_component_required')
    return m


def _dependencies(root,m,mapping,*,catalog_only=False):
    """Validate an explicit DAG, deduplicating shared ancestors by publication ID."""
    parts=[];seen={};visiting=set();total=0
    def visit(ref):
        nonlocal total
        ref=check_dependency(ref);key=ref['component_id']
        require(key not in visiting,'component_dependency_cycle')
        if key in seen:
            require(seen[key]==ref,'component_dependency_identity_conflict');return
        require(len(seen)+len(visiting)<MAX_DEPENDENCIES,'component_checkpoint_required_dependency_bound')
        visiting.add(key)
        directory=Path(mapping[key]) if key in mapping else Path(root)/'_dependencies'/key
        dep=_current_manifest(directory,catalog_only=catalog_only)
        require(dep['component']=='books_metadata','trade_component_cannot_be_a_book_dependency')
        require(dep['component_contract']['contract_hash']==key and digest(dep['files'])==ref['files_sha256'] and
                digest({k:v for k,v in dep.items() if k!='local_component_replay'})==ref['manifest_semantic_sha256'],
                'component_dependency_file_binding_changed')
        for parent in dep['dependencies']:visit(parent)
        total+=sum(f['bytes'] for f in dep['files'] if not catalog_only or f['file'].split('/')[1] in CATALOG_DATASETS)
        require(total<=512*1024**2,'component_checkpoint_required_byte_bound')
        visiting.remove(key);seen[key]=ref;parts.append((directory,dep))
    for dep in m['dependencies']:visit(dep)
    return parts


def _catalog(parts):
    markets={};fees={};assets=set();assignments=[];groups=[];pages=[]
    for index,(root,m) in enumerate(parts):
        current=index==len(parts)-1
        for venue in ('polymarket','predictfun','myriad'):
            for row in dataset_rows(root,m,'instruments',venue):
                key=(venue,str(row['market_id']),row['market_version_id'])
                if key in markets:
                    require(markets[key]==row,'shared_instrument_identity_payload_conflict')
                    require(not current,'duplicate_shared_instrument_definition_use_dependency')
                markets[key]=row
                condition=row.get('condition_id')
                if isinstance(condition,str) and re.fullmatch('0x[0-9a-fA-F]{64}',condition):condition=bytes.fromhex(condition[2:])
                for instrument in json.loads(row['instruments_json']):
                    if instrument.get('book_origin')=='native':assets.add((venue,condition,int(instrument['asset_id']).to_bytes(32,'big')))
            for row in dataset_rows(root,m,'fee_policies',venue):
                key=(venue,row['fee_version_id'])
                value={k:v for k,v in row.items() if k not in {'market_id','available_at_ns','source_effective_at_ns','source_ref','effective_time_basis'}}
                if key in fees:
                    require(fees[key]==value,'shared_fee_identity_payload_conflict')
                    require(not current,'duplicate_shared_fee_definition_use_dependency')
                fees[key]=value
            assignments.extend(dataset_rows(root,m,'instrument_versions',venue))
            pages.extend((venue,row) for row in dataset_rows(root,m,'trade_pages',venue))
        groups.extend(json.loads(r['definition_json']) for r in dataset_rows(root,m,'groups'))
        groups.extend(read_pair_relations(root,m))
    for row in assignments:
        require((row['venue'],str(row['market_id']),row['market_version_id']) in markets and
                (row['venue'],row['fee_version_id']) in fees,'component_unresolved_instrument_or_fee_version')
    for venue,row in pages:
        require((venue,str(row['market_id']),row['source_market_version_id']) in markets,'component_trade_instrument_unresolved')
    for group in groups:
        for member in group['members']:
            venue,mid=member.split(':',1)
            require((venue,mid,group['market_versions'][member]) in markets and (venue,group['fee_versions'][member]) in fees,
                    'component_group_version_unresolved')
    return dict(markets=len(markets),fee_policies=len(fees),native_assets=len(assets),relations=len(groups)),assets


def verify_component(root,*,manifest=None,dependency_roots=None,state_sink=None):
    root=Path(root).resolve();m=_current_manifest(root,manifest)
    deps=_dependencies(root,m,dependency_roots or {},catalog_only=m['component']=='trades')
    parts=deps+[(root,m)];catalog,assets=_catalog(parts);count=0
    myriad_markets={str(row['market_id']) for p,d in parts for row in dataset_rows(p,d,'instruments','myriad')}
    for entry in m['files']:
        name=entry['file'];n=0
        if '/book_checkpoints/' in name:
            from .checkpoints import CHECKPOINT_SCHEMA,MYRIAD_CHECKPOINT_SCHEMA
            schema=MYRIAD_CHECKPOINT_SCHEMA if '/venue=myriad/' in name else CHECKPOINT_SCHEMA
            require(pq.read_schema(root/name).equals(schema,check_metadata=True),'checkpoint_typed_schema_changed')
        for row in rows(root/name,16 if Path(name).name=='book.parquet' else 512):
            n+=1
            if '/events/' in name and Path(name).stem in {'book','price_change','last_trade_price','tick_size_change','best_bid_ask'}:
                venue=name.split('venue=',1)[1].split('/',1)[0]
                condition=row.get('condition_id',row.get('market'))
                if venue=='myriad' and row['asset_id'] is None and Path(name).stem=='tick_size_change':
                    require(row['market_id'] in myriad_markets,'component_book_instrument_unresolved')
                else:require((venue,condition,row['asset_id']) in assets,'component_book_instrument_unresolved')
            if '/book_checkpoints/' in name:
                venue=name.split('venue=',1)[1].split('/',1)[0]
                require((venue,row['condition_id'],row['asset_id']) in assets,'component_checkpoint_instrument_unresolved')
                require(row['available_at_ns']==m['checkpoint']['as_of_ns'],'checkpoint_state_clock_changed')
        require(n==entry['rows'],'component_cold_row_count_changed');count+=n
    for venue,expected in m['trades'].items():
        actual=_roundtrip_hash(root,m,venue)
        require(all(expected[k]==v for k,v in actual.items()),'component_financial_roundtrip_changed')
        pages=sorted(dataset_rows(root,m,'trade_pages',venue),key=lambda r:r['sequence'])
        require(len(pages)==expected['pages'] and digest(pages)==expected['page_metadata_sha256'],'component_page_metadata_changed')
    require(state_sink is None or m['component']=='books_metadata','checkpoint_state_sink_requires_book_component')
    if 'myriad_native_projection' in m:
        from marketdata.l2 import ExactL2
        from task5.replay import replay
        from myriad.archive import verify_events
        native=m['myriad_native_projection'];state=ExactL2()
        require(native['component_dependencies']==[component_reference(p) for p,d in deps],'component_myriad_dependency_chain_changed')
        replay([(p,d['files']) for p,d in deps],state=state)
        verify_events(root,m,native['source_semantic_sha256'],state=state,catalog_parts=parts)
    books=replay_book_parts([(p,d['files']) for p,d in parts],state_sink=state_sink) if m['component']=='books_metadata' else None
    if m.get('checkpoint'):require(final_states(books)==m['checkpoint']['final_states'],'checkpoint_received_state_replay_changed')
    binding=digest({k:v for k,v in m.items() if k!='local_component_replay'})
    return dict(verified=True,profile=PROFILE,component=m['component'],manifest_semantic_sha256=binding,
        parquet_files=len(m['files']),parquet_rows=count,dependency_components=len(deps),catalog=catalog,
        books=books,financial_replay=m['trades'],received_data_replay_only=True,
        dependency_read_scope='catalog_only' if m['component']=='trades' else 'book_and_catalog',
        full_exchange_fill_coverage=False,independent_book_delivery=m['component']=='books_metadata')


def seal_component(root,*,component,window,native_fences,dependencies=(),dependency_roots=None,checkpoint=None,myriad_projection=None):
    """Seal already-closed native parts; an active trade phase is irrelevant."""
    require(component in {'books_metadata','trades'},'independent_component_kind_required')
    integer(window['start_ns'],'component_start_required');integer(window['end_ns'],'component_end_required')
    require(window['start_ns']<window['end_ns'],'component_window_invalid')
    require(0<len(native_fences)<=2,'native_persistence_fences_required')
    for fence in native_fences:verified(fence,'native-prefix-fence/1')
    dependencies=[check_dependency(d) for d in dependencies]
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    if (root/'manifest.json').exists():
        m=_current_manifest(root);require(m['window']==window and m['component']==component and m['native_fences']==native_fences and
            m['dependencies']==dependencies and m.get('checkpoint')==checkpoint and m.get('myriad_native_projection')==myriad_projection,'component_reseal_identity_changed')
        return verify_component(root,dependency_roots=dependency_roots)
    files=[]
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink(),'component_symlink_not_allowed')
        if not path.is_file():continue
        name=path.relative_to(root).as_posix();require(name.startswith('v4/') and path.suffix=='.parquet','component_unclosed_or_unknown_file')
        with pq.ParquetFile(path,page_checksum_verification=True) as reader:row_count=reader.metadata.num_rows
        files.append(dict(file=name,rows=row_count,bytes=path.stat().st_size,sha256=sha_file(path)))
    m=dict(schema=PROFILE,component=component,window=deepcopy(window),native_fences=native_fences,files=files,
           dependencies=dependencies,trades={},source_payloads_deleted=False,full_source_coverage_certified=False)
    if checkpoint:m['checkpoint']=check_replay_checkpoint(checkpoint)
    if myriad_projection is not None:m['myriad_native_projection']=deepcopy(myriad_projection)
    venues=sorted({f['file'].split('venue=',1)[1].split('/',1)[0] for f in files if f['file'].startswith('v4/trade_pages/')})
    for venue in venues:
        proof=_roundtrip_hash(root,m,venue);pages=sorted(dataset_rows(root,m,'trade_pages',venue),key=lambda r:r['sequence'])
        proof.update(pages=len(pages),page_metadata_sha256=digest(pages));m['trades'][venue]=proof
    m['component_contract']=seal('native-component-seal/1',component=component,window=window,files_sha256=digest(files),
        dependencies=dependencies,native_fences=native_fences,source_fill_completeness_certified=False,
        **(dict(checkpoint=m['checkpoint']) if checkpoint else {}),
        **(dict(myriad_projection_sha256=digest(myriad_projection)) if myriad_projection is not None else {}))
    proof=verify_component(root,manifest=m,dependency_roots=dependency_roots)
    m['local_component_replay']=proof;atomic(root/'manifest.json',m);sync_directory(root)
    return proof
