"""Local immutable recovery shards. Never publish native identity hashes.

Only touched buckets are rewritten; checkpoints contain small hash manifests.
The latest two committed checkpoints protect their referenced buckets from GC.
"""
from collections import OrderedDict
from pathlib import Path
import hashlib,json,re
import zstandard
from .store import durable_file,encoded,sha256_file

MAX_BUCKET_BYTES=2*1024**2
MAX_CACHED_BUCKETS=16

def check(ok,reason):
 if not ok:raise ValueError('backtest_index_'+reason)

class TradeIndex:
 def __init__(self,root,manifest=None):
  self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
  check(not self.root.is_symlink(),'symlink')
  self.manifest=dict(manifest or {});self.cache=OrderedDict();self.dirty=set()
  self.loads=0;self.writes=0;self.count=sum(x["count"] for x in self.manifest.values())
 def __len__(self):return self.count
 @staticmethod
 def bucket(market,fingerprint):
  return hashlib.sha256(str(market).encode()).hexdigest()[:16]+'-'+hashlib.sha256(fingerprint.encode()).hexdigest()[:1]
 def path(self,entry):
  name=entry['file'];check(re.fullmatch(r'[a-f0-9]{64}\.json\.zst',name) is not None,'path')
  path=self.root/name;check(not path.is_symlink(),'symlink');return path
 def _save(self,k):
  if k not in self.dirty:return
  raw=encoded(self.cache[k]);check(len(raw)<=MAX_BUCKET_BYTES,'bucket_capacity')
  blob=zstandard.ZstdCompressor(level=3).compress(raw);digest=hashlib.sha256(blob).hexdigest();path=self.root/(digest+'.json.zst')
  if path.exists():check(sha256_file(path)==digest,'existing_hash')
  else:durable_file(path,blob)
  self.manifest[k]=dict(file=path.name,sha256=digest,count=len(self.cache[k]));self.dirty.remove(k);self.writes+=1
 def _load(self,k):
  if k in self.cache:self.cache.move_to_end(k);return self.cache[k]
  while len(self.cache)>=MAX_CACHED_BUCKETS:
   oldest=next(iter(self.cache));self._save(oldest);self.cache.pop(oldest)
  entry=self.manifest.get(k);rows={}
  if entry:
   path=self.path(entry);check(sha256_file(path)==entry['sha256'],'hash')
   raw=zstandard.ZstdDecompressor().decompress(path.read_bytes(),max_output_size=MAX_BUCKET_BYTES)
   check(len(raw)<=MAX_BUCKET_BYTES,'bucket_capacity');rows=json.loads(raw);check(len(rows)==entry['count'],'count');self.loads+=1
  self.cache[k]=rows;return rows
 def get(self,market,fingerprint):return self._load(self.bucket(market,fingerprint)).get(fingerprint)
 def put(self,market,fingerprint,value):
  k=self.bucket(market,fingerprint);rows=self._load(k)
  if fingerprint not in rows:self.count+=1
  rows[fingerprint]=value;self.dirty.add(k)
 def flush(self):
  for k in sorted(self.dirty):self._save(k)
  return {k:self.manifest[k] for k in sorted(self.manifest)}
 def retire(self,market):
  prefix=hashlib.sha256(str(market).encode()).hexdigest()[:16]+'-'
  for k in list(set(self.manifest)|set(self.cache)):
   if k.startswith(prefix):
    self.count-=len(self.cache[k]) if k in self.cache else self.manifest[k]['count']
    self.manifest.pop(k,None);self.cache.pop(k,None);self.dirty.discard(k)
 def verify(self):
  for entry in self.manifest.values():check(sha256_file(self.path(entry))==entry['sha256'],'hash')
 def collect(self,protected):
  # Caller supplies manifests from BOTH retained, hash-verified checkpoints.
  keep={e['file'] for manifest in protected for e in manifest.values()}
  for p in self.root.glob('*.json.zst'):
   check(not p.is_symlink() and re.fullmatch(r'[a-f0-9]{64}\.json\.zst',p.name) is not None,'gc_scope')
   if p.name not in keep:p.unlink()
