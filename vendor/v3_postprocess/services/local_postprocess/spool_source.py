"""Read-only exact-revision hf-file-spool/1 reader; no collector imports/runtime."""
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
import zstandard
from jsonschema import Draft202012Validator
from .restore import safe
from mixed_recorder.publication import sha_file

CONTRACT=json.loads((Path(__file__).resolve().parents[2]/'contracts/hf-file-spool-v1.schema.json').read_bytes())

def validate(kind,value):
    Draft202012Validator({'$ref':'#/$defs/'+kind,'$defs':CONTRACT['$defs']}).validate(value)

class Source:
    def __init__(self,repo,revision,collection,cache,token=None):
        if not re.fullmatch('[a-f0-9]{40}',revision):raise ValueError('exact_revision_required')
        if not re.fullmatch('[A-Za-z0-9_-]{1,100}',collection):raise ValueError('safe_collection_required')
        self.repo,self.revision,self.collection=repo,revision,collection
        self.cache=Path(cache);self.cache.mkdir(parents=True,exist_ok=True);self.token=token
        self.prefix='raw-collections/'+collection

    def download(self,name,expected=None):
        from huggingface_hub import hf_hub_download
        safe(self.cache,name)
        ident=expected['sha256'] if expected else hashlib.sha256((self.revision+':'+name).encode()).hexdigest()
        target=self.cache/ident
        if not target.exists():
            os.environ['HF_HUB_DISABLE_XET']='1'
            from huggingface_hub import constants
            constants.HF_HUB_DISABLE_XET=True
            cache=str((self.cache/'hf').resolve())
            if os.name=='nt':cache='\\\\?\\'+cache
            incoming=hf_hub_download(self.repo,name,repo_type='dataset',revision=self.revision,
                                    token=self.token,cache_dir=cache)
            temporary=target.with_suffix('.part');shutil.copyfile(incoming,temporary);os.replace(temporary,target)
        if expected and (target.stat().st_size!=expected['bytes'] or sha_file(target)!=expected['sha256']):
            raise ValueError('HF_file_hash_or_size_changed')
        return target

    def manifest_paths(self):
        from huggingface_hub import HfApi
        return [r.path for r in HfApi(token=self.token).list_repo_tree(self.repo,
            path_in_repo=self.prefix+'/segments',repo_type='dataset',revision=self.revision)
               if r.path.endswith('.json')]

    def manifests(self):
        paths=self.manifest_paths()
        if len(paths)>10000:raise ValueError('manifest_discovery_bound')
        result=[]
        for name in sorted(paths):
            file=self.download(name)
            if file.stat().st_size>8*1024**2:raise ValueError('manifest_size_bound')
            m=json.loads(file.read_bytes())
            if m.get('schema')=='file-spool-publication/3':
                from mixed_recorder.file_backtest_publication import validate as validate_v3
                validate_v3(m,self.prefix)
                if name!=self.prefix+'/segments/'+m['identity']+'.json':raise ValueError('manifest_identity_changed')
                result.append(dict(manifest=m,path=name,sha256=sha_file(file)));continue
            if m.get('schema')=='file-spool-publication/2':
                from mixed_recorder.file_trade_publication import validate as validate_compact
                validate_compact(m,self.prefix)
                if name!=self.prefix+'/segments/'+m['identity']+'.json':raise ValueError('manifest_identity_changed')
                result.append(dict(manifest=m,path=name,sha256=sha_file(file)));continue
            validate('publication',m)
            if m['encoding']!='marketdata-event/2-jsonl-zstd3':raise ValueError('unknown_spool_encoding')
            if m['identity']!=f'{m["first"]:020d}-{m["last"]:020d}' or name!=self.prefix+'/segments/'+m['identity']+'.json':
                raise ValueError('manifest_identity_changed')
            cursor=m['first'];wanted=set()
            for b in m['blocks']:
                if b['file']!=f'{cursor:020d}.jsonl' or b['first']!=cursor or b['rows']!=b['last']-cursor+1:
                    raise ValueError('block_sequence_scope_changed')
                wanted.add(self.prefix+'/events/'+Path(b['file']).stem+'.zip');cursor=b['last']+1
            files={f['remote_path']:f for f in m['files']}
            metadata=[p for p in files if p.startswith(self.prefix+'/metadata/') and p.endswith('.json.gz')]
            if cursor!=m['last']+1 or len(files)!=len(m['files']) or len(metadata)!=1 or set(files)!=wanted|set(metadata):
                raise ValueError('manifest_inventory_changed')
            result.append(dict(manifest=m,path=name,sha256=sha_file(file)))
        return result

    def catalog(self,m):
        if m.get('schema')=='file-spool-publication/3':
            raise ValueError('v3_requires_profile_v3_and_separate_standard_output')
        f=next(f for f in m['files'] if '/metadata/' in f['remote_path'])
        if Path(f['remote_path']).name!=f['sha256']+'.json.gz':raise ValueError('catalog_content_address_changed')
        path=self.download(f['remote_path'],f)
        with gzip.open(path,'rb') as stream:raw=stream.read(128*1024**2+1)
        if len(raw)>128*1024**2:raise ValueError('catalog_size_bound')
        value=json.loads(raw);validate('catalog',value)
        # Producer canonical JSON is intentionally distinct from Decimal semantic parsing.
        from task_control.contracts import digest
        for ident,body in value['bodies'].items():
            if digest(body)!=ident:raise ValueError('catalog_body_hash_changed')
        for member,ref in value['metadata'].items():
            if ref['body_sha256'] not in value['bodies']:raise ValueError('catalog_body_missing')
        return value

    def events(self,m):
        if m.get('schema')=='file-spool-publication/3':
            raise ValueError('v3_is_compact_projection_use_compact_standard_not_raw_event_decoder')
        if m.get('schema')=='file-spool-publication/2':
            yield from self.compact_events(m);return
        files={f['remote_path']:f for f in m['files']};expected=m['first']
        for block in m['blocks']:
            stem=Path(block['file']).stem;name=self.prefix+'/events/'+stem+'.zip'
            path=self.download(name,files[name]);h=hashlib.sha256();size=count=0
            with zipfile.ZipFile(path) as archive:
                if archive.namelist()!=[stem+'.jsonl.zst'] or archive.infolist()[0].compress_type!=zipfile.ZIP_STORED:
                    raise ValueError('spool_ZIP_member_contract')
                with archive.open(stem+'.jsonl.zst') as packed,zstandard.ZstdDecompressor().stream_reader(packed) as decoded:
                    buffered=io.BufferedReader(decoded)
                    while True:
                        line=buffered.readline(64*1024**2+1)
                        if not line:break
                        if len(line)>64*1024**2 or not line.endswith(b'\n'):raise ValueError('spool_line_bound_or_truncated')
                        row=json.loads(line)
                        if row.get('schema_version')!='marketdata-event/2' or type(row.get('collector_sequence')) is not int or row['collector_sequence']!=expected:
                            raise ValueError('event_schema_or_sequence_gap')
                        for field in ('timestamp_received_ns','receive_monotonic_ns'):
                            if type(row[field]) is not int or not 0<=row[field]<2**63:raise ValueError('exact_ns_clock_required')
                        if not isinstance(row['raw_payload'],str):raise ValueError('raw_payload_required')
                        h.update(line);size+=len(line);count+=1;expected+=1
                        yield row,line.decode('utf8').rstrip('\n')
            if size!=block['bytes'] or count!=block['rows'] or h.hexdigest()!=block['sha256'] or expected!=block['last']+1:
                raise ValueError('decoded_block_hash_count_changed')
        if expected!=m['last']+1:raise ValueError('batch_sequence_gap')

    def compact_events(self,m):
        from mixed_recorder.file_trade_publication import validate as validate_compact
        from mixed_recorder.file_trade_codec import read
        validate_compact(m,self.prefix)
        f=next(f for f in m['files'] if f['remote_path'].endswith('.zip'))
        path=self.download(f['remote_path'],f);expected=m['first']
        with tempfile.TemporaryDirectory(prefix='trade-refs-',dir=self.cache) as scratch:
            for row,line in read(path,m['archive'],scratch):
                if row.get('schema_version')!='marketdata-event/2' or type(row.get('collector_sequence')) is not int or row['collector_sequence']!=expected:
                    raise ValueError('event_schema_or_sequence_gap')
                for field in ('timestamp_received_ns','receive_monotonic_ns'):
                    if type(row[field]) is not int or not 0<=row[field]<2**63:raise ValueError('exact_ns_clock_required')
                if not isinstance(row['raw_payload'],str):raise ValueError('raw_payload_required')
                expected+=1;yield row,line.decode('utf8').rstrip('\n')
        if expected!=m['last']+1:raise ValueError('batch_sequence_gap')
