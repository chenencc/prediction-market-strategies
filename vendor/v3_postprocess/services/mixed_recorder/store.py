"""Small immutable control commits, OS single-writer lease, no market payloads.

Rename is the commit point. A failed caller must reload before retrying; a
temporary file is never interpreted as a committed operation. Kernel locks are
released on process exit, so a stale PID file cannot authorize a second owner.
"""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import io
import json
import os
import time
from pathlib import Path
import uuid
from task_control.contracts import require, integer, seal, verified, digest

PACK_EVERY = 256
PACK_AT_BYTES = 4*1024**2
MAX_COMMIT_BYTES = 8*1024**2
MAX_PACK_BYTES = PACK_AT_BYTES+MAX_COMMIT_BYTES+PACK_EVERY


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf8')


def sha256_file(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(65536),b''):h.update(block)
    return h.hexdigest()


def history_anchor(root):
    """Atomic single-file gzip anchor; legacy plain anchors remain readable."""
    root=Path(root);packed=root/'history-anchor.json.gz';plain=root/'history-anchor.json'
    path=packed if packed.exists() else plain
    if not path.exists():return None
    require(not path.is_symlink() and path.stat().st_size<=128*1024**2,'bounded_control_anchor_required')
    if path==packed:
        with gzip.open(path,'rb') as stream:body=stream.read(128*1024**2+1)
    else:body=path.read_bytes()
    require(len(body)<=128*1024**2,'control_anchor_decode_bound')
    return verified(json.loads(body),'mixed-control-history-anchor/1')


def durable_file(path, payload, *, replace=False):
    """Immutable pack first, atomic checkpoint second; loose commits retire last."""
    path=Path(path);tmp=path.parent/('.'+uuid.uuid4().hex+'.part')
    try:
        if path.exists() and not replace:
            require(not path.is_symlink() and path.read_bytes()==payload,'control_pack_identity_conflict')
            return
        with tmp.open('xb') as stream:
            stream.write(payload);stream.flush();os.fsync(stream.fileno())
        os.replace(tmp,path);sync_directory(path.parent)
    finally:tmp.unlink(missing_ok=True)


def changes(old, new, path=()):
    """Control deltas retain each offer/segment once, not a full state per tick."""
    if old is new or old == new:return []
    if isinstance(old, dict) and isinstance(new, dict):
        result = []
        for key in sorted(set(old)-set(new)):result.append(dict(op='remove', path=[*path,key]))
        for key in sorted(new):
            if key in old:result.extend(changes(old[key], new[key], (*path,key)))
            else:result.append(dict(op='set', path=[*path,key], value=deepcopy(new[key])))
        return result
    if isinstance(old, list) and isinstance(new, list) and len(new) >= len(old) and new[:len(old)] == old:
        return [dict(op='append', path=list(path), value=deepcopy(new[len(old):]))]
    return [dict(op='set', path=list(path), value=deepcopy(new))]


def apply_changes(state, delta):
    state = deepcopy(state)
    for item in delta:
        path = item['path']
        if not path:
            require(item['op'] == 'set', 'invalid_root_control_delta'); state = deepcopy(item['value']); continue
        parent = state
        for key in path[:-1]:parent = parent[key]
        key = path[-1]
        if item['op'] == 'set':parent[key] = deepcopy(item['value'])
        elif item['op'] == 'remove':del parent[key]
        elif item['op'] == 'append':parent[key].extend(deepcopy(item['value']))
        else:raise ValueError('unknown_control_delta')
    return state


def sync_directory(root):
    if os.name != 'nt':
        fd = os.open(root, os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)


class Store:
    def __init__(self, root, *, pack_every=PACK_EVERY,background_packing=False):
        require(type(pack_every) is int and 2<=pack_every<=PACK_EVERY,'bounded_control_pack_interval_required')
        self.pack_every=pack_every
        require(type(background_packing) is bool,'boolean_background_packing_required')
        self.background_packing=background_packing;self._packer=None;self._packing=None;self._packing_context=None
        self.root = Path(root).resolve(); self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root/'writer.lock').open('a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                self.lock.seek(0); self.lock.write(b'0'); self.lock.flush(); self.lock.seek(0)
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close(); raise RuntimeError('mixed_control_writer_already_owned') from None
        self.last = None; self.state = None;self.checkpoint=None;self.active_bytes=0
        self.timing={name+suffix:0 for name in ('prepare_ns','write_ns','copy_ns','compact_ns','background_prepare_ns','background_publish_ns','background_wait_ns') for suffix in ('_sum','_max')}
        self.timing.update(commits=0,commit_bytes=0,background_packs=0,maximum_loose_commits=0)
        try:self.reload()
        except BaseException:self.close(); raise

    def reload(self):
        # A failed caller may have lost the checkpoint rename reply. Join any
        # prepare-only task before reloading; an orphan pack is never a commit.
        if self._packing is not None:
            try:self._packing.result()
            except Exception:pass
            self._packing=None;self._packing_context=None
        previous=None;state=None;base=0;checkpoint=None;active_bytes=0
        cp=self.root/'checkpoint.json'
        if cp.exists():
            require(not cp.is_symlink(),'control_checkpoint_symlink_not_allowed')
            checkpoint=verified(json.loads(cp.read_text(encoding='utf8')),'mixed-control-checkpoint/1')
            previous=verified(checkpoint['last'],'mixed-control-commit/2');state=checkpoint['state'];base=previous['revision']
            require(checkpoint['revision']==base and digest(state)==checkpoint['state_sha256']==previous['state_sha256'],
                    'control_checkpoint_state_changed')
            descriptor,raw=self._pack(checkpoint['history'])
            require(descriptor['last_revision']==base and descriptor['last_commit_hash']==previous['contract_hash'] and
                    raw[-1][1]==previous,'control_checkpoint_history_changed')
        paths=sorted(self.root.glob('commit-*.json'))
        for path in paths:
            require(not path.is_symlink() and len(path.stem)==19 and path.stem[7:].isdigit(),'control_commit_path_invalid')
            if int(path.stem[7:])<=base:continue # Crash after checkpoint rename, before covered loose unlink.
            index=(previous['revision'] if previous else 0)+1
            require(path.name == f'commit-{index:012}.json', 'control_commit_sequence_gap')
            require(path.stat().st_size<=MAX_COMMIT_BYTES,'control_commit_byte_bound')
            payload=path.read_bytes();active_bytes+=len(payload)
            row = verified(json.loads(payload), 'mixed-control-commit/2')
            require(row['revision'] == index and row['previous'] == (previous['contract_hash'] if previous else None),
                    'control_commit_chain_changed')
            if previous:require(row['at_ns'] >= previous['at_ns'], 'control_clock_regressed')
            state = apply_changes(state, row['delta'])
            require(digest(state) == row['state_sha256'], 'control_delta_state_changed')
            previous = row
        self.last = previous; self.state = state;self.checkpoint=checkpoint;self.active_bytes=active_bytes
        return deepcopy(state)

    def _pack(self, reference):
        """Read one bounded lossless block, never silently trust a file listing."""
        name=reference['file']
        require(isinstance(name,str) and Path(name).name==name and name.startswith('pack-') and name.endswith('.json'),
                'control_pack_path_invalid')
        history=self.root/'history'
        require(not history.is_symlink(),'control_history_symlink_not_allowed')
        path=history/name
        require(not path.is_symlink() and path.is_file() and path.stat().st_size<=8192,'bounded_control_pack_descriptor_required')
        payload=path.read_bytes()
        require(hashlib.sha256(payload).hexdigest()==reference['sha256'],'control_pack_descriptor_changed')
        descriptor=verified(json.loads(payload),'mixed-control-pack/1')
        require(descriptor['contract_hash']==reference['contract_hash'],'control_pack_contract_changed')
        archive=descriptor['archive'];require(archive==name[:-5]+'.jsonl.gz','control_pack_archive_path_changed')
        file=history/archive
        require(not file.is_symlink() and file.is_file() and file.stat().st_size==descriptor['archive_bytes']<=MAX_PACK_BYTES+65536,
                'bounded_control_pack_archive_required')
        compressed=file.read_bytes()
        require(hashlib.sha256(compressed).hexdigest()==descriptor['archive_sha256'],'control_pack_archive_changed')
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:raw=stream.read(MAX_PACK_BYTES+1)
        require(len(raw)==descriptor['raw_bytes']<=MAX_PACK_BYTES and hashlib.sha256(raw).hexdigest()==descriptor['raw_sha256'],
                'control_pack_raw_changed_or_too_large')
        records=[];previous_hash=descriptor['previous_commit'];last_clock=None
        for index,line in enumerate(raw.splitlines(),descriptor['first_revision']):
            row=verified(json.loads(line),'mixed-control-commit/2')
            require(row['revision']==index and row['previous']==previous_hash,'control_pack_commit_chain_changed')
            require(last_clock is None or row['at_ns']>=last_clock,'control_clock_regressed')
            records.append((line,row));previous_hash=row['contract_hash'];last_clock=row['at_ns']
        require(0<len(records)<=PACK_EVERY and len(records)==descriptor['rows'] and
                records[-1][1]['revision']==descriptor['last_revision'] and previous_hash==descriptor['last_commit_hash'],
                'control_pack_range_changed')
        return descriptor,records

    def compact(self):
        """Checkpoint committed state; preserve original commit bytes in gzip packs.

        Startup verifies only the latest packed block and uncovered tail. Full
        historical replay is an explicit offline audit, outside capture startup.
        A power loss at any rename leaves either loose history or a checked pack.
        """
        if self.background_packing:
            while self._packing is not None:self._background_maintenance(wait=True)
        if self.last is None:return None
        start=self.checkpoint['revision']+1 if self.checkpoint else 1;end=self.last['revision']
        if start>end:
            if self.checkpoint:self._retire_covered(self.checkpoint)
            return deepcopy(self.checkpoint)
        require(end-start+1<=PACK_EVERY,'control_pack_interval_exceeded')
        records=[];paths=[]
        for revision in range(start,end+1):
            path=self.root/f'commit-{revision:012}.json'
            require(not path.is_symlink() and path.stat().st_size<=MAX_COMMIT_BYTES,'bounded_control_commit_required')
            records.append(path.read_bytes());paths.append(path)
        raw=b'\n'.join(records)+b'\n';require(len(raw)<=MAX_PACK_BYTES,'control_pack_byte_bound')
        compressed=gzip.compress(raw,compresslevel=6,mtime=0)
        name=f'pack-{start:012}-{end:012}';archive=name+'.jsonl.gz'
        descriptor=seal('mixed-control-pack/1',first_revision=start,last_revision=end,rows=len(records),
            previous_commit=json.loads(records[0])['previous'],last_commit_hash=self.last['contract_hash'],
            previous_pack=self.checkpoint['history'] if self.checkpoint else None,
            archive=archive,archive_bytes=len(compressed),archive_sha256=hashlib.sha256(compressed).hexdigest(),
            raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
        history=self.root/'history';require(not history.is_symlink(),'control_history_symlink_not_allowed')
        history.mkdir(exist_ok=True);sync_directory(self.root)
        durable_file(history/archive,compressed);payload=encoded(descriptor);durable_file(history/(name+'.json'),payload)
        reference=dict(file=name+'.json',sha256=hashlib.sha256(payload).hexdigest(),contract_hash=descriptor['contract_hash'])
        checked,decoded=self._pack(reference)
        require([line for line,_ in decoded]==records and checked==descriptor,'control_pack_roundtrip_changed')
        checkpoint=seal('mixed-control-checkpoint/1',revision=end,last=self.last,state=self.state,
                        state_sha256=digest(self.state),history=reference)
        durable_file(self.root/'checkpoint.json',encoded(checkpoint),replace=True)
        # Cold reload checks checkpoint+pack before unlinking any covered input.
        self.reload()
        require(self.checkpoint==checkpoint,'control_checkpoint_readback_changed')
        self._retire_covered(checkpoint)
        return deepcopy(checkpoint)

    def _background_build(self,context):
        """Prepare an immutable bounded pack; this worker cannot publish a head."""
        started=time.perf_counter_ns();start=context['start'];end=context['last']['revision'];records=[]
        require(0<end-start+1<=PACK_EVERY,'control_pack_interval_exceeded')
        for revision in range(start,end+1):
            path=self.root/f'commit-{revision:012}.json'
            require(not path.is_symlink() and path.stat().st_size<=MAX_COMMIT_BYTES,'bounded_control_commit_required')
            records.append(path.read_bytes())
        raw=b'\n'.join(records)+b'\n';require(len(raw)<=MAX_PACK_BYTES,'control_pack_byte_bound')
        compressed=gzip.compress(raw,compresslevel=6,mtime=0);name=f'pack-{start:012}-{end:012}'
        descriptor=seal('mixed-control-pack/1',first_revision=start,last_revision=end,rows=len(records),
            previous_commit=json.loads(records[0])['previous'],last_commit_hash=context['last']['contract_hash'],
            previous_pack=context['previous_pack'],archive=name+'.jsonl.gz',archive_bytes=len(compressed),
            archive_sha256=hashlib.sha256(compressed).hexdigest(),raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
        history=self.root/'history';require(not history.is_symlink(),'control_history_symlink_not_allowed')
        history.mkdir(exist_ok=True);sync_directory(self.root)
        durable_file(history/descriptor['archive'],compressed);payload=encoded(descriptor);durable_file(history/(name+'.json'),payload)
        reference=dict(file=name+'.json',sha256=hashlib.sha256(payload).hexdigest(),contract_hash=descriptor['contract_hash'])
        checked,decoded=self._pack(reference)
        require([line for line,_ in decoded]==records and checked==descriptor,'control_pack_roundtrip_changed')
        require(digest(context['state'])==context['last']['state_sha256'],'control_pack_snapshot_changed')
        checkpoint=seal('mixed-control-checkpoint/1',revision=end,last=context['last'],state=context['state'],
            state_sha256=context['last']['state_sha256'],history=reference)
        return dict(checkpoint=checkpoint,records=records,bytes=context['bytes'],prepare_ns=time.perf_counter_ns()-started)

    def _background_maintenance(self,*,before=False,wait=False):
        if self.last is None:return
        base=self.checkpoint['revision'] if self.checkpoint else 0
        # Start halfway to the old bound. Preparation may overlap new commits,
        # but a slow worker cannot grow the loose tail past the original bound.
        must_wait=before and (self.last['revision']-base>=self.pack_every or self.active_bytes>=PACK_AT_BYTES)
        if self._packing is not None and (self._packing.done() or wait or must_wait):
            started=time.perf_counter_ns();future=self._packing
            try:prepared=future.result()
            finally:self._packing=None;self._packing_context=None
            self._timed('background_wait_ns',started);started=time.perf_counter_ns()
            cp=prepared['checkpoint'];require(cp['revision']<=self.last['revision'],'background_checkpoint_ahead_of_writer')
            prior=base+1;require(cp['history'] and json.loads(prepared['records'][0])['revision']==prior,'background_checkpoint_base_changed')
            payload=encoded(cp);durable_file(self.root/'checkpoint.json',payload,replace=True)
            require((self.root/'checkpoint.json').read_bytes()==payload,'control_checkpoint_readback_changed')
            self.checkpoint=cp;self.active_bytes-=prepared['bytes'];require(self.active_bytes>=0,'background_control_bytes_changed')
            # The verified pack retains every original byte. Only its exact
            # covered loose control records retire; newer records are untouched.
            for original in prepared['records']:
                row=json.loads(original);path=self.root/f"commit-{row['revision']:012}.json"
                require(not path.is_symlink() and path.read_bytes()==original,'control_commit_changed_before_pack_retirement')
                path.unlink()
            sync_directory(self.root);self._timed('background_publish_ns',started)
            elapsed=prepared['prepare_ns'];self.timing['background_prepare_ns_sum']+=elapsed
            self.timing['background_prepare_ns_max']=max(self.timing['background_prepare_ns_max'],elapsed)
            self.timing['background_packs']+=1
        base=self.checkpoint['revision'] if self.checkpoint else 0
        loose=self.last['revision']-base;self.timing['maximum_loose_commits']=max(self.timing['maximum_loose_commits'],loose)
        if self._packing is None and (loose>=max(2,self.pack_every//2) or self.active_bytes>=PACK_AT_BYTES//2):
            if self._packer is None:self._packer=ThreadPoolExecutor(max_workers=1,thread_name_prefix='control-pack')
            context=dict(start=base+1,last=self.last,state=self.state,bytes=self.active_bytes,
                         previous_pack=self.checkpoint['history'] if self.checkpoint else None)
            self._packing_context=context;self._packing=self._packer.submit(self._background_build,context)
            if before and must_wait:self._background_maintenance(wait=True)

    def _retire_covered(self,checkpoint):
        _,records=self._pack(checkpoint['history'])
        for payload,row in records:
            path=self.root/f"commit-{row['revision']:012}.json"
            if not path.exists():continue
            require(not path.is_symlink() and path.read_bytes()==payload,'control_commit_changed_before_pack_retirement')
            path.unlink()
        sync_directory(self.root)

    def verify_history(self, *, max_packs=4096):
        """Explicit cold lossless audit, bounded by caller; does not change state."""
        require(type(max_packs) is int and max_packs>0,'bounded_control_history_audit_required')
        self.reload();packs=[];reference=self.checkpoint['history'] if self.checkpoint else None;seen=set()
        anchor=None;anchor_proof=history_anchor(self.root)
        if anchor_proof is not None:
            anchor=anchor_proof['checkpoint']
            verified(anchor,'mixed-control-checkpoint/1');verified(anchor['last'],'mixed-control-commit/2')
            require(anchor['revision']==anchor['last']['revision'] and
                    digest(anchor['state'])==anchor['state_sha256']==anchor['last']['state_sha256'],
                    'control_history_anchor_state_changed')
        while reference:
            require(reference['contract_hash'] not in seen and len(packs)<max_packs,'control_history_audit_bound_or_cycle')
            seen.add(reference['contract_hash']);descriptor,_=self._pack(reference)
            if anchor and descriptor['last_revision']==anchor['revision']:
                require(reference==anchor['history'] and descriptor['last_commit_hash']==anchor['last']['contract_hash'],
                        'control_history_anchor_boundary_changed')
                break
            require(not anchor or descriptor['first_revision']>anchor['revision'],'control_history_anchor_gap')
            packs.append(reference)
            reference=descriptor['previous_pack']
        require(not anchor or reference==anchor['history'],'control_history_anchor_not_reached')
        previous=anchor['last'] if anchor else None;state=deepcopy(anchor['state']) if anchor else None;count=anchor['revision'] if anchor else 0
        def accept(row):
            nonlocal previous,state,count
            require(row['revision']==count+1 and row['previous']==(previous['contract_hash'] if previous else None),
                    'control_history_sequence_or_chain_changed')
            require(previous is None or row['at_ns']>=previous['at_ns'],'control_clock_regressed')
            state=apply_changes(state,row['delta']);require(digest(state)==row['state_sha256'],'control_delta_state_changed')
            previous=row;count+=1
        for ref in reversed(packs):
            _,records=self._pack(ref)
            for _,row in records:accept(row)
        base=self.checkpoint['revision'] if self.checkpoint else 0
        for path in sorted(self.root.glob('commit-*.json')):
            if int(path.stem[7:])>base:accept(verified(json.loads(path.read_bytes()),'mixed-control-commit/2'))
        require(previous==self.last and state==self.state,'control_history_checkpoint_disagrees')
        return dict(verified=True,commits=count,packs=len(packs),head=previous['contract_hash'] if previous else None,
                    state_sha256=digest(state),history_preserved=anchor is None,
                    history_start_revision=anchor['revision'] if anchor else 0)

    def retire_history(self):
        """Owner-only rolling checkpoint; never enabled implicitly or for quotas.

        Keep the previous checkpoint and its whole pack, current checkpoint pack,
        and uncovered commits. Historical replay explicitly starts at the durable
        anchor; ordinary reload remains compatible with the pre-retention reader.
        """
        require(not self.lock.closed,'control_history_owner_required')
        # The exclusive owner already validated/committed this exact state.
        # Do not replay its large registry tail again just to run retention.
        if self.last is not None:
            verified(self.last,'mixed-control-commit/2')
            require(digest(self.state)==self.last['state_sha256'],'control_retention_state_changed')
        pending=self.root/'history-retirement.json'
        if pending.exists():
            require(not pending.is_symlink(),'control_history_retirement_symlink')
            intent=verified(json.loads(pending.read_bytes()),'mixed-control-history-retirement/1')
            if intent['phase']=='retiring':return self._finish_history_retirement(intent)
        anchor=self.checkpoint
        self.compact()
        if anchor is None:return dict(status='waiting_previous_checkpoint',files=0,bytes=0)
        descriptor,_=self._pack(anchor['history']);cutoff=descriptor['first_revision']
        rows=[]
        for path in sorted((self.root/'history').glob('pack-*.json')):
            require(not path.is_symlink(),'control_history_descriptor_symlink')
            value=verified(json.loads(path.read_bytes()),'mixed-control-pack/1')
            if value['last_revision']>=cutoff:continue
            ref=dict(file=path.name,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),contract_hash=value['contract_hash'])
            checked,_=self._pack(ref)
            for name in (path.name,checked['archive']):
                p=self.root/'history'/name
                rows.append(dict(file=name,sha256=sha256_file(p),bytes=p.stat().st_size))
        proof=seal('mixed-control-history-anchor/1',checkpoint=anchor,at_ns=time.time_ns(),
                   historical_prefix_pruned=True,quota_history_eligible=False)
        durable_file(self.root/'history-anchor.json.gz',gzip.compress(encoded(proof),compresslevel=6,mtime=0),replace=True)
        require(history_anchor(self.root)==proof,'control_history_anchor_readback_changed')
        # compact() already cold-reloaded the current state and latest pack.
        # The anchor's pack was independently checked above; historical replay
        # of every intermediate registry state stays an explicit offline audit.
        intent=seal('mixed-control-history-retirement/1',anchor_hash=proof['contract_hash'],anchor_revision=anchor['revision'],
                    phase='retiring',files=rows)
        durable_file(pending,encoded(intent),replace=True)
        return self._finish_history_retirement(intent)

    def _finish_history_retirement(self,intent):
        import re
        anchor=history_anchor(self.root)
        require(anchor['contract_hash']==intent['anchor_hash'],'control_history_retirement_anchor_changed')
        descriptor,_=self._pack(anchor['checkpoint']['history']);cutoff=descriptor['first_revision']
        size=0
        for row in intent['files']:
            match=re.fullmatch(r'pack-([0-9]{12})-([0-9]{12})\.(json|jsonl\.gz)',row['file'])
            require(match is not None and int(match[2])<cutoff,'control_history_retirement_scope')
            p=self.root/'history'/row['file'];require(not p.is_symlink(),'control_history_retirement_symlink')
            if not p.exists():continue
            require(sha256_file(p)==row['sha256'] and p.stat().st_size==row['bytes'],'control_history_input_changed')
            p.unlink();size+=row['bytes']
        sync_directory(self.root/'history')
        done={k:v for k,v in intent.items() if k not in ('schema','contract_hash','phase')}
        durable_file(self.root/'history-retirement.json',encoded(seal('mixed-control-history-retirement/1',**done,phase='retired')),replace=True)
        legacy=self.root/'history-anchor.json'
        if (self.root/'history-anchor.json.gz').exists() and legacy.exists():
            require(not legacy.is_symlink(),'control_legacy_anchor_symlink');legacy.unlink();sync_directory(self.root)
        return dict(status='history_checkpoint_retained',files=len(intent['files']),bytes=size,anchor_revision=intent['anchor_revision'])

    def commit(self, state, *, event, at_ns, expected_revision, transfer_immutable=False):
        # Opt-in ownership transfer for copy-on-write producers only. Neither
        # producer nor background packer may mutate a published snapshot. Other
        # callers retain the defensive copy and unchanged on-disk contract.
        require(type(transfer_immutable) is bool,'boolean_snapshot_transfer_required')
        integer(at_ns, 'invalid_control_clock')
        revision = self.last['revision'] if self.last else 0
        require(expected_revision == revision, 'stale_control_revision_reload_required')
        require(self.last is None or at_ns >= self.last['at_ns'], 'control_clock_regressed')
        # A previous caller may have lost the reply before checkpoint creation.
        # Pack that already committed tail before accepting another operation.
        base=self.checkpoint['revision'] if self.checkpoint else 0
        started=time.perf_counter_ns()
        if self.background_packing:self._background_maintenance(before=True)
        elif revision-base>=self.pack_every or self.active_bytes>=PACK_AT_BYTES:self.compact()
        self._timed('compact_ns',started);started=time.perf_counter_ns()
        row = seal('mixed-control-commit/2', revision=revision+1,
                   previous=self.last['contract_hash'] if self.last else None,
                   at_ns=at_ns, event=event, delta=changes(self.state, state), state_sha256=digest(state))
        payload=encoded(row);require(len(payload)<=MAX_COMMIT_BYTES,'control_commit_byte_bound')
        self._timed('prepare_ns',started)
        path = self.root/f'commit-{revision+1:012}.json'
        require(not path.exists(), 'control_commit_already_exists_reload_required')
        tmp = self.root/('.'+uuid.uuid4().hex+'.part')
        try:
            started=time.perf_counter_ns()
            with tmp.open('xb') as stream:
                stream.write(payload)
                stream.flush(); os.fsync(stream.fileno())
            os.replace(tmp, path); sync_directory(self.root)
            self._timed('write_ns',started);started=time.perf_counter_ns()
            self.last = row; self.state = state if transfer_immutable else deepcopy(state);self.active_bytes+=len(payload)
            self._timed('copy_ns',started);self.timing['commits']+=1;self.timing['commit_bytes']+=len(payload)
        finally:tmp.unlink(missing_ok=True)
        base=self.checkpoint['revision'] if self.checkpoint else 0
        started=time.perf_counter_ns()
        if self.background_packing:self._background_maintenance()
        elif self.last['revision']-base>=self.pack_every or self.active_bytes>=PACK_AT_BYTES:self.compact()
        self._timed('compact_ns',started)
        return row

    def _timed(self,name,started):
        elapsed=time.perf_counter_ns()-started
        self.timing[name+'_sum']+=elapsed;self.timing[name+'_max']=max(self.timing[name+'_max'],elapsed)

    def close(self):
        failure=None
        try:
            while self._packing is not None:self._background_maintenance(wait=True)
        except BaseException as exc:failure=exc
        finally:
            if self._packer:self._packer.shutdown(wait=True);self._packer=None
        if not self.lock.closed:
            if os.name == 'nt':
                import msvcrt
                self.lock.seek(0); msvcrt.locking(self.lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock.fileno(), fcntl.LOCK_UN)
            self.lock.close()
        if failure:raise failure

    def __enter__(self):return self
    def __exit__(self, *args):self.close()
