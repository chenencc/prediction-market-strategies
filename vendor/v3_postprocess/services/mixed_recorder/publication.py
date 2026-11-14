"""Independent immutable archive outbox; no market client or receive-loop work.

Remote verification is bound to an exact revision. Retrying an uncertain commit
first reconciles its immutable manifest. Cleanup is a separate guarded action.
Daily HF verification compares exact-revision content SHA256/size inventories.
Full cold replay remains available for explicit audits and legacy adapters.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tempfile
import time

from task_control.contracts import digest, evidence, require, seal, verified
from .store import sync_directory
from .control import transaction


def sha_file(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream, 'sha256').hexdigest()


def safe_relative(name):
    require(isinstance(name, str) and '\\' not in name, 'portable_relative_archive_path_required')
    path = PurePosixPath(name)
    require(not path.is_absolute() and bool(path.parts) and all(x not in {'.', '..'} for x in name.split('/')),
            'archive_path_escape')
    return path


def bound_file(root, name):
    safe_relative(name); root = Path(root).resolve(); path = root/name
    require(not any(p.is_symlink() for p in [path, *path.parents] if p != root.parent), 'archive_symlink_not_allowed')
    require(path.resolve().is_relative_to(root) and path.is_file(), 'bound_archive_file_required')
    return path


def content_path(name,sha):
    """Reusing an immutable catalog block must reuse its original remote path."""
    path=safe_relative(name)
    require(re.fullmatch('[0-9a-f]{64}',sha),'content_address_hash_required')
    stem=path.stem if path.stem.endswith('-'+sha) else path.stem+'-'+sha
    return str(path.parent/(stem+path.suffix))


def publication_requirements(value):
    if value['component']=='complete':return []
    manifest=value['archive_manifest'];refs=list(manifest['dependencies'])
    if manifest.get('checkpoint'):refs+=manifest['checkpoint']['source_components']
    unique={}
    for ref in refs:
        key=ref['component_id']
        require(key not in unique or unique[key]==ref,'publication_dependency_identity_conflict')
        unique[key]=ref
    return list(unique.values())


def public_bundle(value,key,*,resolved=()):
    public={k:v for k,v in value.items() if k not in {'source_directory','contract_hash'}}
    if value['component']!='complete':public['resolved_dependencies']=list(resolved)
    return dict(public,publication_id=key)


def backend_scope(backend):
    identity=getattr(backend,'identity',None)
    if identity is None:return dict(kind='test_fixture',backend_class=type(backend).__module__+'.'+type(backend).__name__)
    require(isinstance(identity,dict) and identity.get('kind') in {'huggingface_private_dataset','local_archive'},'explicit_publication_backend_identity_required')
    return deepcopy(identity)


def cold_verify_publication(value,key,job,backend,semantic_verify):
    """Explicit full restore audit, including remote dependency materialization."""
    public=public_bundle(value,key,resolved=job.get('resolved_dependencies',[]))
    with tempfile.TemporaryDirectory(prefix='native-cold-read-') as tmp:
        cold=Path(tmp)
        remote_manifest=backend.download(job['revision'],job['manifest_remote_path'],cold/'publication.json')
        require(json.loads(Path(remote_manifest).read_text(encoding='utf8'))==public,'cold_manifest_changed')
        for row in value['files']:
            target=cold/row['logical_path'];target.parent.mkdir(parents=True,exist_ok=True)
            saved=Path(backend.download(job['revision'],row['remote_path'],target))
            require(saved.resolve()==target.resolve(),'cold_download_wrong_destination')
            require(sha_file(saved)==row['sha256'] and saved.stat().st_size==row['bytes'],'cold_payload_changed')
        (cold/'manifest.json').write_text(json.dumps(value['archive_manifest']),encoding='utf8')
        if value['component']!='complete':
            from .components import materialize_dependencies
            materialize_dependencies(backend,value['archive_manifest']['dependencies'],job['resolved_dependencies'],cold/'_dependencies',
                catalog_only=value['component']=='trades')
        semantic=semantic_verify(cold)
        require(isinstance(semantic,dict) and semantic.get('verified') is True,'explicit_cold_semantic_replay_receipt_required')
        require(semantic==value['local_archive_replay'],'cold_semantic_replay_changed')
        return semantic


def bundle(archive, *, collection_id, component, local_verification_ref, max_bytes=128*1024**2, max_files=96):
    """One closed profile2 archive. Component isolation is supplied by the sealer.

    Local files stay in their existing v4 layout. Remote immutable part names
    include their SHA; relationships refer to logical paths through this envelope.
    """
    require(re.fullmatch('[a-zA-Z0-9_-]{1,100}', collection_id), 'safe_collection_id_required')
    require(component in {'books_metadata', 'trades', 'complete'}, 'known_publication_component_required')
    evidence([local_verification_ref])
    root = Path(archive).resolve(); path = bound_file(root, 'manifest.json')
    manifest = json.loads(path.read_text(encoding='utf8'))
    require(manifest.get('schema') == 'nautilus-source/2', 'profile2_archive_required')
    if component=='complete':
        require('component_contract' not in manifest,'independent_component_mislabeled_complete')
        from .archive_replay import verify_cold_archive
        replay=verify_cold_archive(root)
    else:
        from .components import _current_manifest
        _current_manifest(root)
        replay=manifest.get('local_component_replay',{})
        require(manifest['component']==component and replay.get('verified') is True and
                replay.get('manifest_semantic_sha256')==digest({k:v for k,v in manifest.items() if k!='local_component_replay'}),
                'sealed_independent_component_receipt_required')
    entries = manifest['files'] + manifest.get('auxiliary_files', [])
    require(0 < len(entries) <= max_files, 'publication_file_bound_exceeded')
    rows = []; seen = set(); size = 0
    for entry in entries:
        name = entry['file']; require(name not in seen, 'duplicate_archive_path'); seen.add(name)
        local = bound_file(root, name); sha = sha_file(local); count = local.stat().st_size
        require(sha == entry['sha256'], 'local_sealed_input_changed')
        safe_relative(name)
        require(name.startswith('v4/') and local.suffix in {'.parquet', '.json'}, 'typed_v4_archive_parts_only')
        remote = content_path(name,sha)
        rows.append(dict(logical_path=name, remote_path=remote, sha256=sha, bytes=count))
        size += count
    require(size <= max_bytes, 'publication_byte_bound_exceeded_split_segment')
    return seal('native-archive-publication/1', collection_id=collection_id, component=component,
        archive_profile=manifest['schema'], manifest_sha256=sha_file(path), files=rows,
        local_archive_replay=replay,
        local_verification_ref=local_verification_ref, source_quality_certified=False,
        source_directory=str(root), archive_manifest=manifest,
        **(dict(component_id=manifest['component_contract']['contract_hash']) if component!='complete' else {}))


class Outbox:
    def __init__(self, store, *, at_ns, clock_ns=time.time_ns):
        self.store = store; self.clock_ns = clock_ns
        if store.last is None:
            self.state = dict(schema='native-publication-outbox/1', jobs={})
            self.save('initialized', at_ns)
        else:
            self.state = deepcopy(store.state)
            require(self.state['schema'] == 'native-publication-outbox/1', 'wrong_control_store')

    def save(self, event, at_ns):
        self.store.commit(self.state, event=event, at_ns=at_ns,
                          expected_revision=self.store.last['revision'] if self.store.last else 0)

    @transaction
    def enqueue(self, value, *, at_ns):
        from task_control.contracts import verified
        value = verified(value, 'native-archive-publication/1'); key = value['contract_hash']
        if value['component']!='complete':
            matches=[(k,j) for k,j in self.state['jobs'].items() if j['bundle'].get('component_id')==value['component_id']]
            if matches:
                existing,job=matches[0];require(job['bundle']['archive_manifest']==value['archive_manifest'],'sealed_component_identity_conflict')
                return existing
        if key not in self.state['jobs']:
            self.state['jobs'][key] = dict(bundle=value, phase='sealed', revision=None, parent=None,
                enqueued_at_ns=at_ns, failures=0, retry_at_ns=at_ns, last_error_type=None,
                manifest_remote_path='collections/'+value['collection_id']+'/'+value['component']+'/'+key+'/manifest.json',
                verified_receipt=None, cleanup_receipt=None,resolved_dependencies=[],backend_identity=None)
            self.save(dict(enqueued=key), at_ns)
        return key

    def dependency(self,key):
        """Only a durable exact-revision book receipt can become a shared link."""
        job=self.state['jobs'][key];value=job['bundle']
        require(job['phase'] in {'verified','retired'} and job['verified_receipt'] and value['component']=='books_metadata',
                'verified_book_component_dependency_required')
        public=public_bundle(value,key,resolved=job['resolved_dependencies'])
        return seal('native-component-dependency/1',publication_id=key,component_id=value['component_id'],component='books_metadata',revision=job['revision'],
            manifest_remote_path=job['manifest_remote_path'],
            public_manifest_sha256=hashlib.sha256(json.dumps(public,sort_keys=True).encode()).hexdigest(),
            files_sha256=digest(value['files']),readback_receipt_hash=job['verified_receipt']['contract_hash'],backend_identity=job['backend_identity'])

    def dependencies_ready(self,key):
        value=self.state['jobs'][key]['bundle']
        if value['component']=='complete':return True
        for ref in publication_requirements(value):
            matches=[j for j in self.state['jobs'].values() if j['bundle'].get('component_id')==ref['component_id']]
            if len(matches)!=1 or matches[0]['phase'] not in {'verified','retired'}:return False
        return True

    def resolve_dependencies(self,key,backend):
        value=self.state['jobs'][key]['bundle'];result=[]
        if value['component']=='complete':return result
        require(self.dependencies_ready(key),'verified_publication_dependency_pending')
        for ref in publication_requirements(value):
            parent,next_job=next((k,j) for k,j in self.state['jobs'].items() if j['bundle'].get('component_id')==ref['component_id'])
            require(next_job['backend_identity']==backend_scope(backend),'publication_dependency_backend_scope_changed')
            manifest=next_job['bundle']['archive_manifest']
            require(digest(manifest['files'])==ref['files_sha256'] and
                    digest({k:v for k,v in manifest.items() if k!='local_component_replay'})==ref['manifest_semantic_sha256'],
                    'publication_source_dependency_binding_changed')
            result.append(self.dependency(parent))
        checkpoint=value['archive_manifest'].get('checkpoint')
        if checkpoint:
            from .archive_replay import final_states
            source=next(j for j in self.state['jobs'].values() if j['bundle'].get('component_id')==checkpoint['source_head']['component_id'])
            replay=source['verified_receipt']['semantic_receipt']
            require(digest(replay)==checkpoint['source_replay_sha256'] and final_states(replay['books'])==checkpoint['final_states'],
                    'checkpoint_verified_source_state_changed')
        return result

    def deliver(self, key, backend, *, semantic_verify, at_ns):
        """One bounded attempt; no unbounded sleep or provider calls on failure.

        Backend.resolve returns (exact revision, manifest) or None. Downloads must
        bypass prior local cache. The caller schedules retries outside capture.
        """
        job = self.state['jobs'][key]; value = job['bundle']
        scope=backend_scope(backend)
        require(job.get('backend_identity') in (None,scope),'publication_backend_scope_changed')
        if job['phase'] in {'verified', 'retired'}:return deepcopy(job['verified_receipt'])
        try:
            job['last_attempt_ns'] = at_ns
            self.save(dict(delivery_attempt=key), at_ns)
            root = Path(value['source_directory'])
            require(sha_file(bound_file(root, 'manifest.json')) == value['manifest_sha256'], 'local_manifest_changed')
            for row in value['files']:
                require(sha_file(bound_file(root, row['logical_path'])) == row['sha256'], 'sealed_input_changed')
            # No source_directory is published. It is local queue ownership only.
            if job['phase']=='sealed':
                job['resolved_dependencies']=self.resolve_dependencies(key,backend);job['backend_identity']=scope
            public=public_bundle(value,key,resolved=job.get('resolved_dependencies',[]))
            require(len(json.dumps(public, sort_keys=True).encode()) <= 2*1024**2,
                    'publication_manifest_size_guard')
            if job['phase'] == 'sealed':
                job['parent'] = backend.head(); job['phase'] = 'upload_uncertain'
                self.save(dict(upload_intent=key, parent=job['parent']), at_ns)
            if job['phase'] == 'upload_uncertain':
                observed_head = backend.head()
                found = backend.resolve(job['manifest_remote_path'], revision=observed_head)
                if found is not None:
                    revision, remote = found
                    require(remote == public, 'remote_publication_identity_conflict')
                else:
                    # Absence was checked at this exact revision. These are only
                    # immutable content-addressed Adds plus a new manifest, so
                    # unrelated commits can be rebased without deleting files.
                    # A new concurrent HEAD change still fails parent_commit.
                    if observed_head != job['parent']:
                        job['parent'] = observed_head
                        self.save(dict(absent_manifest_reconciled_at_revision=observed_head, publication_id=key), self.clock_ns())
                    with tempfile.TemporaryDirectory(prefix='native-publication-manifest-') as tmp:
                        marker = Path(tmp)/'manifest.json'; marker.write_text(json.dumps(public,sort_keys=True),encoding='utf8')
                        files = [(row['remote_path'], bound_file(root, row['logical_path'])) for row in value['files']]
                        files += [(job['manifest_remote_path'], marker)]
                        revision = backend.commit(files, parent=job['parent'], message='native archive '+key)
                require(isinstance(revision, str) and re.fullmatch('[0-9a-f]{40}', revision), 'exact_remote_revision_required')
                job['revision'] = revision; job['phase'] = 'uploaded'; self.save(dict(uploaded=key), self.clock_ns())
            require(job['phase'] == 'uploaded', 'invalid_delivery_phase')
            extra={}
            if getattr(backend,'verification_mode',None)=='metadata':
                require(scope['kind']=='huggingface_private_dataset','HF_metadata_backend_required')
                body=json.dumps(public,sort_keys=True).encode('utf8')
                expected=[dict(remote_path=r['remote_path'],sha256=r['sha256'],bytes=r['bytes']) for r in value['files']]
                expected.append(dict(remote_path=job['manifest_remote_path'],sha256=hashlib.sha256(body).hexdigest(),bytes=len(body)))
                metadata=verified(backend.verify_files(job['revision'],expected),'HF-exact-revision-file-metadata/1')
                require(metadata['revision']==job['revision'] and metadata['backend_identity']==scope and
                    [{k:r[k] for k in ('remote_path','sha256','bytes')} for r in metadata['files']]==expected,
                    'HF_exact_metadata_receipt_binding_changed')
                semantic=deepcopy(value['local_archive_replay'])
                extra=dict(verification_mode='HF_exact_revision_SHA256_metadata',remote_file_metadata=metadata,
                    semantic_receipt_source='sealed_local_archive_replay',remote_payload_readback=False,remote_semantic_replay=False)
            else:
                semantic=cold_verify_publication(value,key,job,backend,semantic_verify)
                extra=dict(verification_mode='full_cold_readback',semantic_receipt_source='remote_cold_replay',
                    remote_payload_readback=True,remote_semantic_replay=True)
            verified_at_ns = self.clock_ns()
            require(verified_at_ns >= at_ns, 'verification_clock_regressed')
            # Keep the existing envelope for the unchanged live receiver's
            # retirement consumer. The explicit mode distinguishes metadata
            # reads from a full payload restore; semantic proof stays local.
            receipt = seal('native-exact-revision-readback/1', publication_id=key, revision=job['revision'],
                file_count=len(value['files']), total_bytes=sum(r['bytes'] for r in value['files']),
                files_sha256=digest(value['files']), semantic_receipt=semantic,
                available_at_ns=verified_at_ns, source_fill_completeness_certified=False,backend_identity=scope,**extra)
            job['phase'] = 'verified'; job['verified_receipt'] = receipt
            self.save(dict(exact_revision_verified=key), verified_at_ns)
            return deepcopy(receipt)
        except BaseException:
            self.state = deepcopy(self.store.reload()); raise

    @transaction
    def retire(self, key, *, active_writer, dependency_in_use, at_ns):
        """Only sealed archive files, never live CH/native tables or credentials.

        The durable verification receipt precedes unlink. Partial cleanup is
        resumable from its immutable inventory and a persisted retirement intent.
        """
        job = self.state['jobs'][key]; value = job['bundle']
        require(job['phase'] in {'verified', 'retiring', 'retired'} and job['verified_receipt'], 'remote_verification_required_before_cleanup')
        require(active_writer is False and dependency_in_use is False, 'active_dependency_blocks_retirement')
        if job['phase'] == 'retired':return deepcopy(job['cleanup_receipt'])
        root = Path(value['source_directory']).resolve()
        inventory = {r['logical_path']:r['sha256'] for r in value['files']} | {'manifest.json':value['manifest_sha256']}
        actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() or p.is_symlink()}
        require(actual <= set(inventory), 'unexpected_archive_file_blocks_cleanup')
        if job['phase'] == 'verified':require(actual == set(inventory), 'archive_inventory_changed')
        for name in actual:
            require(sha_file(bound_file(root, name)) == inventory[name], 'changed_input_blocks_cleanup')
        job['phase'] = 'retiring'; self.save(dict(retirement_intent=key, inventory=inventory), at_ns)
        for name in sorted(actual):bound_file(root, name).unlink()
        sync_directory(root)
        receipt = seal('native-archive-cleanup/1', publication_id=key,
            readback_receipt_hash=job['verified_receipt']['contract_hash'], inventory_sha256=digest(inventory),
            at_ns=at_ns, native_staging_deleted=False, credentials_touched=False)
        job['phase'] = 'retired'; job['cleanup_receipt'] = receipt
        self.save(dict(retired=key), at_ns)
        return receipt
