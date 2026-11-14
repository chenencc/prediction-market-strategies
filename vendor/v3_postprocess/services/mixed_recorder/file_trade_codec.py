"""Archive-only trade projection. Receiver/WAL and old HF wire format are unchanged.

Each ZIP owns its dictionary; no references escape the publication batch.
Known display fields are intentionally omitted. All remaining native number
lexemes, page positions, duplicates, envelope clocks and WS bytes survive.
"""
from collections import Counter, OrderedDict
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import time
import zipfile
import zstandard

PROFILE='trade-refs-zstd3-v1'
ENCODING='marketdata-event/2-trade-refs-jsonl-zstd3'
POLY_DISPLAY=frozenset(('icon','title','slug','event_slug','name','pseudonym','bio','profile_image','profile_image_optimized'))
PREDICT_DISPLAY=frozenset(('title','question','description','imageUrl'))
MAX_LINE=64*1024**2
MAX_NODES=250000
MAX_OBJECTS=300000


def require(ok,message):
    if not ok:raise ValueError('trade_archive_'+message)


def dumps(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)
def sha(value):return hashlib.sha256(value).hexdigest()
def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024**2),b''):h.update(chunk)
    return h.hexdigest()
def forbidden(value):raise ValueError('trade_archive_nonfinite_JSON')
DECODER=json.JSONDecoder(parse_float=Decimal,parse_constant=forbidden)


class Node:
    __slots__=('start','end','kind','children')
    def __init__(self,start,end,kind,children=None):self.start,self.end,self.kind,self.children=start,end,kind,children
    def fields(self):return {key:child for key,_,child in self.children} if self.kind=='object' else {}


def parse(text):
    """Single traversal spans; never convert financial numbers through float."""
    count=0
    def ws(i):
        while i<len(text) and text[i] in ' \t\r\n':i+=1
        return i
    def walk(i,depth):
        nonlocal count
        count+=1;require(count<=MAX_NODES and depth<=64,'JSON_bound')
        i=ws(i);start=i;require(i<len(text),'truncated_JSON')
        if text[i]=='{':
            i=ws(i+1);children=[];keys=set()
            if i<len(text) and text[i]=='}':return Node(start,i+1,'object',children)
            while True:
                key_start=i;key,end=DECODER.raw_decode(text,i);require(isinstance(key,str),'object_key')
                require(key not in keys,'duplicate_JSON_key');keys.add(key);i=ws(end)
                require(i<len(text) and text[i]==':','object_separator');child=walk(i+1,depth+1)
                children.append((key,key_start,child));i=ws(child.end)
                require(i<len(text),'truncated_object')
                if text[i]=='}':return Node(start,i+1,'object',children)
                require(text[i]==',','object_comma');i=ws(i+1)
        if text[i]=='[':
            i=ws(i+1);children=[]
            if i<len(text) and text[i]==']':return Node(start,i+1,'array',children)
            while True:
                child=walk(i,depth+1);children.append(child);i=ws(child.end)
                require(i<len(text),'truncated_array')
                if text[i]==']':return Node(start,i+1,'array',children)
                require(text[i]==',','array_comma');i=ws(i+1)
        _,end=DECODER.raw_decode(text,i)
        return Node(start,end,'value')
    root=walk(0,0);require(ws(root.end)==len(text),'trailing_JSON');return root


def token(text,node):return text[node.start:node.end]
def value(text,node):return DECODER.decode(token(text,node))
def replace(text,changes):
    for start,end,new in sorted(changes,reverse=True):text=text[:start]+new+text[end:]
    return text


def trimmed(text,node,omitted,counts,prefix):
    parts=[]
    for key,key_start,child in node.children:
        if key in omitted:counts[prefix+key]+=1
        else:parts.append(text[key_start:child.end])
    return '{'+','.join(parts)+'}'


class Objects:
    def __init__(self,stream,max_objects=MAX_OBJECTS):self.stream=stream;self.seen=set();self.counts=Counter();self.max_objects=max_objects
    def put(self,kind,text,parts=None):
        h=sha(text.encode());key=(kind,h)
        if key not in self.seen:
            require(len(self.seen)<self.max_objects,'object_count_bound')
            record=dict(kind=kind,sha256=h,**({'text':text} if parts is None else {'parts':parts}))
            self.stream.write((dumps(record)+'\n').encode());self.seen.add(key);self.counts[kind+'_definitions']+=1
        else:self.counts[kind+'_reuses']+=1
        return dict(kind=kind,sha256=h)


class Encoder:
    def __init__(self,objects):self.objects=objects;self.counts=Counter()
    def encode(self,line):
        # Fast dispatch parses the ordinary envelope but never native WS JSON.
        row=json.loads(line,parse_float=Decimal,parse_constant=forbidden)
        if not (row.get('event_type')=='trade_page' and row.get('source_transport')=='rest' and
                row.get('venue') in ('polymarket','predictfun') and row.get('http_status')==200 and
                isinstance(row.get('raw_payload'),str) and 'archive_transform' not in row):
            self.counts['unchanged_events']+=1;return line,line
        raw=row['raw_payload'];venue=row['venue']
        try:
            body=parse(raw);fields=body.fields();data=fields.get('data')
            require(body.kind=='object' and data is not None and data.kind=='array','unrecognized_page')
        except (ValueError,TypeError,RecursionError):
            self.counts['unsupported_pages_retained']+=1;return line,line
        # Parse the envelope before emitting dictionary definitions. Malformed
        # or duplicate-key envelopes must fail closed, not become projections.
        text=line.decode();outer=parse(text);outer_fields=outer.fields();raw_node=outer_fields['raw_payload']
        parts=[];projected=[];at=0;omitted=Counter()
        for trade in data.children:
            if trade.kind!='object':continue  # Unknown/null array entries remain exact.
            parts.append(raw[at:trade.start]);projected.append(raw[at:trade.start])
            if venue=='polymarket':
                native=trimmed(raw,trade,POLY_DISPLAY,omitted,'poly.trade.')
                ref=self.objects.put('trade_row',native)
            else:
                row_parts=[];native_parts=[]
                for key,key_start,child in trade.children:
                    if native_parts:native_parts.append(',');row_parts.append(',')
                    prefix=raw[key_start:child.start]
                    native_parts.append(prefix);row_parts.append(prefix)
                    if key=='market' and child.kind=='object':
                        meta=trimmed(raw,child,PREDICT_DISPLAY,omitted,'predict.market.')
                        native_parts.append(meta);row_parts.append(self.objects.put('predict_market',meta))
                    else:
                        v=token(raw,child);native_parts.append(v);row_parts.append(v)
                native='{'+''.join(native_parts)+'}';row_parts=['{']+row_parts+['}']
                # Only nested market references are allowed in row definitions.
                ref=self.objects.put('trade_row',native,row_parts)
            parts.append(ref);projected.append(native);at=trade.end
            self.counts['trade_row_occurrences']+=1
        parts.append(raw[at:]);projected.append(raw[at:]);payload=''.join(projected)
        info=dict(policy=PROFILE,source_payload_sha256=sha(raw.encode()),projected_payload_sha256=sha(payload.encode()),
                  omitted_fields=dict(omitted),original_payload_reconstructible=False)
        # The encoded wrapper is disjoint from marketdata-event/2, so an old
        # reader cannot mistake references for official payload text.
        envelope=replace(text,[(raw_node.start,raw_node.end,'null')]).strip()
        wire=('{"archive_event":"trade-page-refs/1","envelope":'+envelope+',"payload":'+dumps(dict(parts=parts,**info))+'}\n').encode()
        decoded=decode_envelope(envelope,payload,info)
        self.counts['projected_pages']+=1;self.counts.update(omitted)
        return wire,decoded


def decode_envelope(envelope,payload,info):
    tree=parse(envelope);f=tree.fields();require('archive_transform' not in f,'nested_projection')
    changes=[(f['raw_payload'].start,f['raw_payload'].end,dumps(payload))]
    if 'is_derived' in f:changes.append((f['is_derived'].start,f['is_derived'].end,'true'))
    text=replace(envelope,changes);end=text.rfind('}')
    return (text[:end]+',"archive_transform":'+dumps(info)+text[end:]+'\n').encode()


class Dictionary:
    def __init__(self,path,cache_bytes=8*1024**2):
        self.stream=Path(path).open('rb');self.index={};self.cache=OrderedDict();self.cache_bytes=0;self.limit=cache_bytes
        try:
            while True:
                offset=self.stream.tell();line=self.stream.readline(MAX_LINE+1)
                if not line:break
                require(len(line)<=MAX_LINE and line.endswith(b'\n'),'dictionary_line_bound')
                row=json.loads(line);key=(row['kind'],row['sha256']);require(key[0] in ('predict_market','trade_row') and re.fullmatch('[a-f0-9]{64}',key[1]),'dictionary_identity')
                require(key not in self.index,'duplicate_dictionary_definition')
                require(len(self.index)<MAX_OBJECTS,'object_count_bound');self.index[key]=(offset,len(line))
        except BaseException:self.close();raise
    def get(self,ref):
        require(set(ref)=={'kind','sha256'},'reference_shape');key=(ref['kind'],ref['sha256'])
        if key in self.cache:self.cache.move_to_end(key);return self.cache[key]
        require(key in self.index,'missing_reference');offset,size=self.index[key]
        self.stream.seek(offset);row=json.loads(self.stream.read(size))
        require(row['kind']==key[0] and row['sha256']==key[1],'dictionary_identity_changed')
        if key[0]=='predict_market':require(set(row)=={'kind','sha256','text'} and isinstance(row['text'],str),'market_definition');text=row['text']
        elif 'parts' in row:
            require(set(row)=={'kind','sha256','parts'},'row_definition')
            text=self.parts(row['parts'],'predict_market')
        else:require(set(row)=={'kind','sha256','text'} and isinstance(row['text'],str),'row_definition');text=row['text']
        size=len(text.encode());require(sha(text.encode())==key[1],'object_hash_changed')
        if size<=self.limit:
            self.cache[key]=text;self.cache_bytes+=size
            while self.cache_bytes>self.limit:
                _,old=self.cache.popitem(last=False);self.cache_bytes-=len(old.encode())
        return text
    def parts(self,parts,kind):
        require(isinstance(parts,list),'parts_shape');out=[];size=0
        for part in parts:
            if isinstance(part,str):v=part
            else:require(isinstance(part,dict) and part.get('kind')==kind,'reference_kind');v=self.get(part)
            size+=len(v.encode());require(size<=MAX_LINE,'expanded_payload_bound');out.append(v)
        return ''.join(out)
    def close(self):self.stream.close()


def pack(sources,target):
    """Sources: [(immutable JSONL path, source block descriptor), ...]."""
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    temporary=target.with_name(target.name+'.part');objects_path=target.with_name(target.name+'.objects.part')
    started=time.monotonic();entries=[];decoded=[];counts=Counter();source_rows=0
    try:
        with objects_path.open('wb') as objects_file,zipfile.ZipFile(temporary,'w',compression=zipfile.ZIP_STORED) as archive:
            objects=Objects(objects_file);encoder=Encoder(objects)
            for path,proof in sources:
                path=Path(path);require(path.name==proof['file'] and not path.is_symlink(),'source_scope')
                name='events/'+path.name+'.zst';wire_h=hashlib.sha256();out_h=hashlib.sha256();source_h=hashlib.sha256();sizes=Counter();seq=proof['first'];rows=0
                with path.open('rb') as incoming,archive.open(name,'w',force_zip64=True) as packed:
                    with zstandard.ZstdCompressor(level=3).stream_writer(packed,closefd=False) as codec:
                        while True:
                            line=incoming.readline(MAX_LINE+1)
                            if not line:break
                            require(len(line)<=MAX_LINE and line.endswith(b'\n'),'source_line_bound')
                            row=json.loads(line);require(row['collector_sequence']==seq,'source_sequence_gap');seq+=1;rows+=1
                            wire,projected=encoder.encode(line);codec.write(wire)
                            source_h.update(line);wire_h.update(wire);out_h.update(projected)
                            sizes.update(source=len(line),wire=len(wire),decoded=len(projected))
                require(sizes['source']==proof['bytes'] and source_h.hexdigest()==proof['sha256'] and rows==proof['rows'] and seq==proof['last']+1,'changed_source')
                entries.append(dict(member=name,bytes=sizes['wire'],sha256=wire_h.hexdigest()))
                decoded.append(dict(file=proof['file'],first=proof['first'],last=proof['last'],rows=rows,bytes=sizes['decoded'],sha256=out_h.hexdigest()))
                counts.update(sizes);source_rows+=rows
            objects_file.flush();os.fsync(objects_file.fileno())
            dictionary=dict(member='objects.jsonl.zst',bytes=objects_path.stat().st_size,sha256=file_sha(objects_path))
            with archive.open(dictionary['member'],'w',force_zip64=True) as packed,objects_path.open('rb') as incoming:
                with zstandard.ZstdCompressor(level=3).stream_writer(packed,closefd=False) as codec:
                    for data in iter(lambda:incoming.read(1024**2),b''):codec.write(data)
            counts.update(encoder.counts);counts.update(objects.counts)
        with temporary.open('r+b') as f:os.fsync(f.fileno())
        os.replace(temporary,target)
        # Same crash durability rule as immutable publication staging.
        if os.name!='nt':
            directory=os.open(target.parent,os.O_RDONLY)
            try:os.fsync(directory)
            finally:os.close(directory)
        return dict(schema='file-trade-archive/1',encoding=ENCODING,entries=entries,dictionary=dictionary,decoded_blocks=decoded,
                    counts=dict(counts),source_rows=source_rows,seconds=time.monotonic()-started,archive_bytes=target.stat().st_size,
                    references_scope='same_publication_archive_only',lossy_display_projection=True)
    finally:
        objects_path.unlink(missing_ok=True)


def _extract(archive,descriptor,target):
    h=hashlib.sha256();size=0
    with archive.open(descriptor['member']) as packed,zstandard.ZstdDecompressor().stream_reader(packed) as raw,target.open('wb') as out:
        while True:
            data=raw.read(1024**2)
            if not data:break
            size+=len(data);require(size<=descriptor['bytes'],'expanded_member_size');h.update(data);out.write(data)
    require(size==descriptor['bytes'] and h.hexdigest()==descriptor['sha256'],'member_hash_changed')


def read(target,proof,scratch):
    """Decode projected events; caller pins/verifies the outer archive SHA256."""
    scratch=Path(scratch);scratch.mkdir(parents=True,exist_ok=True)
    require(proof['schema']=='file-trade-archive/1' and proof['encoding']==ENCODING and proof['references_scope']=='same_publication_archive_only','unknown_profile')
    require(len(proof['entries'])==len(proof['decoded_blocks']),'block_inventory')
    names=[r['member'] for r in proof['entries']]+[proof['dictionary']['member']]
    require(len(set(names))==len(names),'duplicate_archive_member')
    with zipfile.ZipFile(target) as archive:
        require(set(archive.namelist())==set(names) and len(archive.namelist())==len(names) and all(x.compress_type==zipfile.ZIP_STORED for x in archive.infolist()),'ZIP_inventory')
        objects_path=scratch/'objects.jsonl';_extract(archive,proof['dictionary'],objects_path);dictionary=Dictionary(objects_path)
        try:
            for entry,block in zip(proof['entries'],proof['decoded_blocks']):
                require(entry['member']=='events/'+block['file']+'.zst','block_member')
                temporary=scratch/'events.jsonl';_extract(archive,entry,temporary)
                h=hashlib.sha256();size=rows=0;seq=block['first']
                with temporary.open('rb') as stream:
                    while True:
                        line=stream.readline(MAX_LINE+1)
                        if not line:break
                        require(len(line)<=MAX_LINE and line.endswith(b'\n'),'wire_line_bound');row=json.loads(line)
                        if row.get('archive_event')=='trade-page-refs/1' and 'schema_version' not in row:
                            require(set(row)=={'archive_event','envelope','payload'},'event_wrapper')
                            payload=row['payload'];info={k:v for k,v in payload.items() if k!='parts'}
                            require(info.get('policy')==PROFILE and info.get('original_payload_reconstructible') is False,'projection_policy')
                            native=dictionary.parts(payload['parts'],'trade_row')
                            require(sha(native.encode())==info['projected_payload_sha256'],'page_hash_changed')
                            # Serialize the envelope WITHOUT float conversion.
                            outer=parse(line.decode()).fields()['envelope'];envelope=token(line.decode(),outer)
                            require(outer.kind=='object' and outer.fields()['raw_payload'] is not None and value(line.decode(),outer.fields()['raw_payload']) is None,'envelope_payload')
                            line=decode_envelope(envelope,native,info);row=json.loads(line)
                        require(row['collector_sequence']==seq,'decoded_sequence_gap');seq+=1;rows+=1;h.update(line);size+=len(line)
                        yield row,line
                require(rows==block['rows'] and seq==block['last']+1 and size==block['bytes'] and h.hexdigest()==block['sha256'],'decoded_block_changed')
        finally:dictionary.close()
