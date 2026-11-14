"""Versioned archive-only compaction; native receiver data is never changed.

Only verified, same-shard Poly projections become references. Bounded cache
misses and unrecognized fields preserve the original row. HTTP chart omission
is explicitly lossy; all other raw JSON lexemes (including decimals) survive.
"""
import hashlib
import json
from collections import OrderedDict, Counter
from decimal import Decimal
from task_control.contracts import require
from .poly_wire import _resolve_parsed, SCHEMA

DEFAULT='compact-zstd3-v1'
LEVELS={'compact-zstd3-v1':3,'compact-zstd6-v1':6}
COMMON={'event_type','market','timestamp','wire_source'}
FIELDS={
    'book':{'asset_id','bids','asks'},
    'price_change':{'asset_id','price','size','side','best_bid','best_ask'},
    'last_trade_price':{'asset_id','price','size','side','fee_rate_bps','transaction_hash'},
    'tick_size_change':{'asset_id','old_tick_size','new_tick_size'},
    'best_bid_ask':{'asset_id','best_bid','best_ask','spread'},
    'new_market':{'id','assets_ids','outcomes','question','slug'},
    'market_resolved':{'id','assets_ids','winning_asset_id','winning_outcome'},
}
SOURCE_FIELDS={'schema','connection_id','connection_epoch','frame_sequence','parent_index',
               'child_index','official_hash','fresh_snapshot'}


def profile(value):
    require(value is None or value in LEVELS,'raw_event_codec_unknown')
    return value


def _strict_pairs(pairs):
    result={}
    for key,value in pairs:
        require(key not in result,'raw_compaction_duplicate_JSON_key')
        result[key]=value
    return result


def _loads(value):
    return json.loads(value,object_pairs_hook=_strict_pairs,parse_float=Decimal)


def trim_charts(text):
    """Remove only root.outcomes[*].price_charts without reserializing numbers."""
    parsed=_loads(text)
    if not isinstance(parsed,dict) or not isinstance(parsed.get('outcomes'),list):return text,0
    decoder=json.JSONDecoder();spans=[]
    def ws(pos):
        while pos<len(text) and text[pos] in ' \t\r\n':pos+=1
        return pos
    def members(start):
        pos=ws(start+1);result=[]
        if text[pos]=='}':return result
        while True:
            key_start=pos;key,end=decoder.raw_decode(text,pos);pos=ws(end)
            require(text[pos]==':','raw_chart_object_separator');value_start=ws(pos+1)
            _,value_end=decoder.raw_decode(text,value_start)
            pos=ws(value_end);result.append((key,key_start,value_start,value_end,pos))
            if text[pos]=='}':return result
            require(text[pos]==',','raw_chart_object_comma');pos=ws(pos+1)
    root=members(ws(0))
    outcome=next((m for m in root if m[0]=='outcomes'),None)
    pos=ws(outcome[2]+1)
    for item in parsed['outcomes']:
        _,end=decoder.raw_decode(text,pos)
        if isinstance(item,dict) and 'price_charts' in item:
            parts=members(pos)
            for i,(key,start,_,finish,separator) in enumerate(parts):
                if key!='price_charts':continue
                if len(parts)==1:spans.append((start,finish))
                elif i<len(parts)-1:spans.append((start,parts[i+1][1]))
                else:spans.append((parts[i-1][4],finish))
        pos=ws(end)
        if text[pos]==',':pos=ws(pos+1)
    for start,end in reversed(spans):text=text[:start]+text[end:]
    return text,len(spans)


class Compactor:
    def __init__(self,*,cache_bytes=16*1024**2,cache_frames=256):
        self.frames=OrderedDict();self.bytes=0;self.limit=cache_bytes;self.frames_limit=cache_frames
        self.counts=Counter(input_rows=0,unchanged_rows=0,poly_projection_references=0,
                            myriad_chart_rows=0,myriad_chart_fields=0,source_unavailable_rows=0,
                            unsupported_rows=0)

    def transform(self,table,row,line):
        self.counts['input_rows']+=1
        # Parse only for recognized transformations; ordinary rows remain exact.
        body=json.loads(row['data'])
        if not isinstance(body,dict):return self.unchanged(line)
        if table=='accept_poly_v3':
            if body.get('event_type')=='ws_message':
                size=len(line)
                if size<=self.limit:
                    # Parse an exploded parent once, not once per child. Cached
                    # parsed bytes are also bounded by frame count and raw size.
                    try:
                        parsed=_loads(row['data'])
                        if parsed.get('schema')!=SCHEMA or parsed.get('message_kind')!='text':return self.unchanged(line)
                        parents=_loads(parsed['payload'])
                    except (ValueError,KeyError,TypeError,RecursionError):
                        self.counts['unsupported_rows']+=1;return self.unchanged(line)
                    self.frames[int(row['sequence'])]=(row,size,parsed,parents);self.bytes+=size
                    while self.bytes>self.limit or len(self.frames)>self.frames_limit:
                        _,old=self.frames.popitem(last=False);self.bytes-=old[1]
                return self.unchanged(line)
            kind=body.get('event_type');source=body.get('wire_source')
            if source is not None and kind in FIELDS:
                if (not isinstance(source,dict) or source.get('schema')!=SCHEMA or
                    set(body)-COMMON-FIELDS[kind] or set(source)-SOURCE_FIELDS or
                    type(source.get('frame_sequence')) is not int or
                    type(source.get('fresh_snapshot')) is not bool):
                    self.counts['unsupported_rows']+=1;return self.unchanged(line)
                if kind=='book' and not all(isinstance(body.get(side),list) and
                    all(isinstance(level,list) and len(level)==2 for level in body[side]) for side in ('bids','asks')):
                    self.counts['unsupported_rows']+=1;return self.unchanged(line)
                frame=self.frames.get(source.get('frame_sequence'))
                if frame is None:
                    self.counts['source_unavailable_rows']+=1;return self.unchanged(line)
                # Reject duplicate JSON keys before trusting resolver projections.
                exact=_loads(row['data'])
                proof=_resolve_parsed(row,exact,frame[0],frame[2],frame[3])
                child=proof['child']
                # Resolver verifies all common book/decimal fields. Explicitly
                # cover lifecycle fields and null optionals, never drop extras.
                for field in FIELDS[kind]-{'bids','asks'}:
                    if field not in exact:continue
                    if field in {'price','size','best_bid','best_ask','spread','old_tick_size','new_tick_size'}:
                        if exact[field] is None:require(child.get(field) is None,'wire_optional_mismatch')
                    else:
                        default=[] if field in {'assets_ids','outcomes'} else None
                        require(exact[field]==child.get(field,default),'wire_projection_field_mismatch')
                body=dict(event_type='ws_projection_reference',
                          ref=[source['frame_sequence'],source['parent_index'],source['child_index']],
                          fresh_snapshot=source['fresh_snapshot'])
                self.counts['poly_projection_references']+=1
                return self.changed(row,body)
        elif (table=='accept_extra_v3' and body.get('venue')=='myriad' and
              body.get('event_type') in {'market_metadata','http_response'} and body.get('source_transport')=='rest' and
              isinstance(body.get('raw_payload'),str) and 'archive_transform' not in body):
            raw=body['raw_payload']
            try:trimmed,count=trim_charts(raw)
            except (ValueError,KeyError,TypeError,RecursionError):
                self.counts['unsupported_rows']+=1;return self.unchanged(line)
            if count:
                # Never silently round any outer extension numeric fields.
                _loads(row['data'])
                body['raw_payload']=trimmed
                body['archive_transform']=dict(policy='myriad-http-charts-omitted/1',fields=count,
                                               source_payload_sha256=hashlib.sha256(raw.encode()).hexdigest())
                # Replace just the raw_payload token; all other native lexemes stay.
                decoder=json.JSONDecoder();pos=1;data=row['data'];pos=len(data)-len(data.lstrip())+1
                while True:
                    while data[pos] in ' \t\r\n,':pos+=1
                    key,end=decoder.raw_decode(data,pos);pos=end
                    while data[pos] in ' \t\r\n:':pos+=1
                    _,end=decoder.raw_decode(data,pos)
                    if key=='raw_payload':
                        changed=data[:pos]+json.dumps(trimmed,ensure_ascii=False)+data[end:]
                        end_object=len(changed.rstrip())-1
                        marker=json.dumps(body['archive_transform'],separators=(',',':'))
                        changed=changed[:end_object]+',"archive_transform":'+marker+changed[end_object:]
                        self.counts['myriad_chart_rows']+=1;self.counts['myriad_chart_fields']+=count
                        return self.changed(row,None,data=changed)
                    pos=end
        return self.unchanged(line)

    def unchanged(self,line):
        self.counts['unchanged_rows']+=1;return line

    @staticmethod
    def changed(row,body,*,data=None):
        data=data if data is not None else json.dumps(body,separators=(',',':'),ensure_ascii=False)
        return (json.dumps(dict(row,data=data),separators=(',',':'),ensure_ascii=False)+'\n').encode()


def resolve_reference(row,lookup):
    """Offline semantic view; not a reconstruction of original normalized bytes."""
    body=json.loads(row['data'])
    require(body.get('event_type')=='ws_projection_reference','compact_reference_type')
    seq,pi,ci=body['ref'];frame=lookup(seq)
    require(frame is not None and int(frame['sequence'])==seq and seq<int(row['sequence']) and
            seq>>48==int(row['sequence'])>>48 and frame['received_ns']==row['received_ns'],
            'compact_reference_dependency')
    original=_loads(frame['data'])
    require(original.get('schema')==SCHEMA and original.get('event_type')=='ws_message' and
            original.get('message_kind')=='text','compact_reference_original')
    parents=_loads(original['payload']);parents=parents if isinstance(parents,list) else [parents]
    require(type(pi) is int and 0<=pi<len(parents) and type(ci) is int and ci>=0,'compact_reference_index')
    parent=parents[pi]
    if parent['event_type']=='price_change':
        require(ci<len(parent['price_changes']),'compact_reference_child');child=parent['price_changes'][ci]
    else:require(ci==0,'compact_reference_child');child=parent
    return dict(parent=parent,child=child,connection_id=original['connection_id'],
                connection_epoch=original['connection_epoch'],fresh_snapshot=body['fresh_snapshot'],
                original_message=original['payload'],normalized_bytes_reconstructed=False)
