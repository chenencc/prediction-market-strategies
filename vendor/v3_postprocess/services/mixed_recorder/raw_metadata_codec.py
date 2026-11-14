"""Lossless bounded metadata checkpoints/deltas, independent of event reception.

Encoded SHA256 identifies every compressed object. Decoded SHA256 binds the exact
existing compact JSON representation. At most sixteen deltas precede a full
checkpoint; dependency references are explicit and must survive cleanup.
Legacy JSON references remain readable. This module performs no provider IO.
"""
from copy import deepcopy
import gzip,hashlib,json,re
from pathlib import Path
from task_control.contracts import require
from .publication import bound_file,sha_file
from .store import encoded,durable_file

MAX_BYTES=128*1024**2
MAX_DEPTH=16
MAX_OPS=100000
CODEC='raw-metadata-gzip-delta/1'


def difference(old,new,path=()):
    if type(old) is type(new) and old==new:return []
    if isinstance(old,dict) and isinstance(new,dict):
        ops=[dict(op='remove',path=[*path,k]) for k in old if k not in new]
        for k,v in new.items():
            ops.extend(difference(old[k],v,(*path,k)) if k in old else [dict(op='set',path=[*path,k],value=v)])
        return ops
    if isinstance(old,list) and isinstance(new,list) and len(old)==len(new):
        return [op for i,(a,b) in enumerate(zip(old,new)) for op in difference(a,b,(*path,i))]
    return [dict(op='set',path=list(path),value=new)]


def apply(old,ops):
    require(isinstance(ops,list) and len(ops)<=MAX_OPS,'metadata_delta_operation_bound')
    out=deepcopy(old)
    for op in ops:
        path=op['path'];require(isinstance(path,list) and len(path)<=64,'metadata_delta_path_bound')
        require(op['op'] in ('set','remove'),'metadata_delta_operation_unknown')
        if not path:
            require(op['op']=='set','metadata_delta_root_remove');out=deepcopy(op['value']);continue
        parent=out
        for key in path[:-1]:parent=parent[key]
        key=path[-1]
        require(isinstance(parent,dict) and isinstance(key,str) or
                isinstance(parent,list) and type(key) is int and 0<=key<len(parent),'metadata_delta_path_type')
        if op['op']=='remove':
            require(isinstance(parent,dict) and key in parent,'metadata_delta_remove_missing');del parent[key]
        else:parent[key]=deepcopy(op['value'])
    return out


def identity(ref):return {k:ref[k] for k in ('file','sha256','bytes')}


def read(root,ref,*,_seen=None,_depth=0,_closure=None):
    require(_depth<=MAX_DEPTH,'metadata_delta_chain_bound')
    seen=set() if _seen is None else _seen
    closure=[] if _closure is None else _closure
    require(ref['sha256'] not in seen,'metadata_delta_cycle');seen.add(ref['sha256'])
    path=bound_file(root,ref['file']);require(type(ref['bytes']) is int and 0<ref['bytes']<=MAX_BYTES,'metadata_encoded_size_bound')
    require(path.stat().st_size==ref['bytes'] and sha_file(path)==ref['sha256'],'metadata_encoded_hash_changed')
    if ref.get('codec') is None:
        require(path.suffix=='.json','legacy_metadata_JSON_required');return json.loads(path.read_bytes())
    require(ref['codec']==CODEC and type(ref['depth']) is int and 0<=ref['depth']<=MAX_DEPTH,'metadata_codec_or_depth_changed')
    require(ref['file']=='metadata/'+ref['sha256']+'.json.gz','metadata_content_path_changed')
    with gzip.open(path,'rb') as stream:body=stream.read(MAX_BYTES+1)
    require(len(body)<=MAX_BYTES,'metadata_decoded_size_bound');doc=json.loads(body)
    require(doc['schema']==CODEC and doc['kind']==ref['kind'],'metadata_document_identity_changed')
    require(doc['decoded_sha256']==ref['decoded_sha256'] and doc['decoded_bytes']==ref['decoded_bytes'],'metadata_logical_binding_changed')
    if doc['mode']=='full':
        require(ref['depth']==0,'metadata_checkpoint_depth_changed');value=doc['value']
    else:
        require(doc['mode']=='delta' and ref['depth']==doc['base']['depth']+1 and doc['base']['kind']==ref['kind'],
                'metadata_delta_base_changed')
        value=apply(read(root,doc['base'],_seen=seen,_depth=_depth+1,_closure=closure),doc['operations'])
    logical=encoded(value)
    require(len(logical)==ref['decoded_bytes'] and hashlib.sha256(logical).hexdigest()==ref['decoded_sha256'],
            'metadata_decoded_hash_changed')
    closure.append(identity(ref))
    if _depth==0:require(files(ref)==closure,'metadata_dependency_closure_changed')
    return value


def write(root,kind,value,*,previous=None):
    root=Path(root);require(kind in ('plan','lifecycle','market_index','recording_contract'),'metadata_kind_unknown')
    raw=encoded(value);require(len(raw)<=MAX_BYTES,'metadata_logical_size_bound');sha=hashlib.sha256(raw).hexdigest()
    if previous and previous.get('decoded_sha256')==sha:
        require(previous['kind']==kind,'metadata_previous_kind_changed')
        require(encoded(read(root,previous))==raw,'metadata_reused_object_changed');return previous
    base=dict(schema=CODEC,kind=kind,decoded_sha256=sha,decoded_bytes=len(raw))
    full=dict(base,mode='full',value=value);packed=gzip.compress(encoded(full),compresslevel=6,mtime=0)
    depth=0;dependencies=[]
    if previous and previous.get('codec')==CODEC and previous['depth']<MAX_DEPTH:
        require(previous['kind']==kind,'metadata_previous_kind_changed')
        old=read(root,previous);ops=difference(old,value)
        if len(ops)<=MAX_OPS and encoded(apply(old,ops))==raw:
            parent={k:v for k,v in previous.items() if k!='dependencies'}
            candidate=gzip.compress(encoded(dict(base,mode='delta',base=parent,operations=ops)),compresslevel=6,mtime=0)
            if len(candidate)<len(packed):
                packed=candidate;depth=previous['depth']+1
                dependencies=[*previous.get('dependencies',[]),identity(previous)]
    sha_packed=hashlib.sha256(packed).hexdigest();relative='metadata/'+sha_packed+'.json.gz'
    (root/'metadata').mkdir(parents=True,exist_ok=True);durable_file(root/relative,packed)
    ref=dict(kind=kind,file=relative,sha256=sha_packed,bytes=len(packed),codec=CODEC,
        decoded_sha256=sha,decoded_bytes=len(raw),depth=depth,dependencies=dependencies)
    require(encoded(read(root,ref))==raw,'metadata_encode_roundtrip_failed')
    return ref


def files(ref):
    """Upload/retain this complete dependency closure before a logical reference."""
    result=[*ref.get('dependencies',[]),identity(ref)]
    require(len(result)<=MAX_DEPTH+1 and len({r['file'] for r in result})==len(result),'metadata_dependency_closure_invalid')
    if ref.get('codec'):
        require(ref['codec']==CODEC and type(ref['depth']) is int and len(result)==ref['depth']+1,
                'metadata_dependency_depth_changed')
        for row in result:
            require(set(row)=={'file','sha256','bytes'} and isinstance(row['sha256'],str) and
                    re.fullmatch('[a-f0-9]{64}',row['sha256']) and
                    row['file']=='metadata/'+row['sha256']+'.json.gz' and
                    type(row['bytes']) is int and 0<row['bytes']<=MAX_BYTES,'metadata_dependency_identity_invalid')
    else:require(len(result)==1,'legacy_metadata_dependencies_unexpected')
    return result


def all_files(refs):
    result={}
    for ref in refs:
        for row in files(ref):
            require(row['file'] not in result or result[row['file']]==row,'metadata_dependency_identity_changed')
            result[row['file']]=row
    return list(result.values())
