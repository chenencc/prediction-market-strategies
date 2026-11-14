"""Lossless native journal shared by venue adapters, outside the receive hot path.

Collector sequence means local accepted order, never exchange order. Original
text is authoritative; numeric strings in projections are views, not wire data.
"""
import hashlib,json,os,queue,threading,time,uuid
import re
from datetime import datetime,timezone
from pathlib import Path
from decimal import Decimal,localcontext
import zstandard as zstd

def atomic(path,obj):
    # Keep the temporary basename bounded. Appending a UUID to an already long
    # relation filename can exceed Windows MAX_PATH even when the final path is
    # valid. The sibling remains on the same filesystem for atomic replacement.
    path=Path(path);tmp=path.parent/('.'+uuid.uuid4().hex+'.part');operation='atomic_file_write'
    try:
        with tmp.open('x',encoding='utf8') as f:
            json.dump(obj,f,ensure_ascii=False,indent=2,default=lambda x:str(x) if isinstance(x,Decimal) else (_ for _ in ()).throw(TypeError(type(x).__name__)));f.flush();os.fsync(f.fileno())
        operation='atomic_file_replace'
        for attempt in range(8):
            try:os.replace(tmp,path);break
            except PermissionError as e:
                # Only bounded filesystem sharing/access retries, never HTTP auth.
                if os.name!='nt' or getattr(e,'winerror',None) not in (5,32,33) or attempt==7:raise
                time.sleep(min(.01*2**attempt,.25))
    except OSError as e:
        e.safe_operation=operation;e.safe_filename=path.name
        raise
    finally:
        try:tmp.unlink(missing_ok=True)
        except OSError:pass

def exact(raw):return json.loads(raw,parse_float=Decimal,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite_json')))
def dumps(obj):return json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=lambda x:str(x) if isinstance(x,Decimal) else (_ for _ in ()).throw(TypeError(type(x).__name__)))

def iso_to_ns(value):
    match=re.fullmatch(r'(.*?)(?:\.([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})',value)
    if not match:raise ValueError('timezone_aware_native_timestamp_required')
    base,fraction,zone=match.groups();fraction=fraction or ''
    if len(fraction)>9:raise ValueError('native_timestamp_precision_beyond_nanoseconds_raw_retained')
    dt=datetime.fromisoformat(base+('+00:00' if zone=='Z' else zone));delta=dt-datetime(1970,1,1,tzinfo=timezone.utc)
    return (delta.days*86400+delta.seconds)*10**9+int(fraction.ljust(9,'0') or '0')

def levels(rows):
    out=[]
    for r in rows:
        if not isinstance(r,list) or len(r)<2:raise ValueError('invalid_level')
        p,q=Decimal(str(r[0])),Decimal(str(r[1]))
        if not p.is_finite() or not q.is_finite() or not 0<=p<=1 or q<0:raise ValueError('invalid_level_value')
        out.append({'price':str(p),'size':str(q)})
    return out

def complement(book,event_reference=None):
    # Native indexSet=1 only. The returned view is never another official sample.
    parent=event_reference or book.get('native_event_reference')
    if not parent:raise ValueError('derived_book_requires_native_event_reference')
    prices=[Decimal(x['price']) for side in ('bids','asks') for x in book[side]]
    precision=max([80]+[len(p.as_tuple().digits)+abs(p.as_tuple().exponent)+2 for p in prices])
    if precision>1024:raise ValueError('derived_decimal_precision_guard_raw_retained')
    with localcontext() as context:
        context.prec=precision
        return {'bids':[{'price':str(Decimal(1)-Decimal(x['price'])),'size':x['size']} for x in book['asks']],
                'asks':[{'price':str(Decimal(1)-Decimal(x['price'])),'size':x['size']} for x in book['bids']],
                'is_derived':True,'derived_from_event_reference':parent}

class Journal:
    def __init__(self,root,max_bytes=64*1024**2,max_records=8192,rotate_bytes=16*1024**2,flush_seconds=None):
        if flush_seconds is not None and flush_seconds<=0:raise ValueError('positive_flush_interval_required')
        self.flush_seconds=flush_seconds
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=False)
        self.queue=queue.Queue(max_records);self.limit=max_bytes;self.rotate_bytes=rotate_bytes
        self.lock=threading.Lock();self.pending=0;self.sequence=0;self.error=None
        self.stats={'accepted':0,'written':0,'bytes':0,'queue_peak_bytes':0,'queue_peak_records':0,'max_lag_ms':0,'overflows':0,'segments':[]}
        self.thread=threading.Thread(target=self._write,name='marketdata-durable-writer',daemon=True);self.thread.start()
    def append(self,venue,event_type,market,transport,raw,epoch=None,key_slot=None,stamp=None,**fields):
        if self.error:raise RuntimeError('durable_writer_failed') from self.error
        stamp=stamp or (time.time_ns(),time.perf_counter_ns())
        self.sequence+=1
        row=dict(schema_version='marketdata-event/2',venue=venue,event_type=event_type,native_market_id=str(market) if market is not None else None,
                 source_transport=transport,timestamp_received_ns=stamp[0],receive_monotonic_ns=stamp[1],connection_epoch=epoch,
                 collector_sequence=self.sequence,key_slot=key_slot,is_derived=False,raw_payload=raw,**fields)
        encoded=(dumps(row)+'\n').encode();size=len(encoded)
        with self.lock:
            if self.pending+size>self.limit or self.queue.full():self.stats['overflows']+=1;raise RuntimeError('journal_queue_overflow')
            self.pending+=size
            self.stats['queue_peak_bytes']=max(self.pending,self.stats['queue_peak_bytes'])
        self.queue.put_nowait((encoded,time.perf_counter_ns()))
        self.stats['accepted']+=1;self.stats['queue_peak_records']=max(self.queue.qsize(),self.stats['queue_peak_records'])
        return self.sequence
    def _write(self):
        f=w=None;current=None;written=0;opened=0;last_sync=time.monotonic()
        def sync():
            nonlocal last_sync
            if w:
                w.flush(zstd.FLUSH_BLOCK);f.flush();os.fsync(f.fileno())
                self.stats['durable_written']=self.stats['written']
            last_sync=time.monotonic()
        def seal():
            nonlocal f,w,current
            if w:
                w.close();f.flush();os.fsync(f.fileno());f.close()
                blob=current.read_bytes();self.stats['segments'].append({'file':current.name,'bytes':len(blob),'sha256':hashlib.sha256(blob).hexdigest()});f=w=None
        try:
            while True:
                try:item=self.queue.get(timeout=self.flush_seconds)
                except queue.Empty:sync();continue
                if item is None:self.queue.task_done();break
                encoded,queued=item
                if w is None or written>=self.rotate_bytes or time.monotonic()-opened>=60:
                    seal();current=self.root/f'events-{len(self.stats["segments"]):05}.ndjson.zst';f=current.open('xb')
                    w=zstd.ZstdCompressor(level=1,write_checksum=True).stream_writer(f,closefd=False);written=0;opened=time.monotonic()
                w.write(encoded);written+=len(encoded);self.stats['written']+=1;self.stats['bytes']+=len(encoded)
                self.stats['max_lag_ms']=max(self.stats['max_lag_ms'],(time.perf_counter_ns()-queued)/1e6)
                with self.lock:self.pending-=len(encoded)
                self.queue.task_done()
                if self.flush_seconds is not None and time.monotonic()-last_sync>=self.flush_seconds:sync()
            seal()
        except BaseException as e:
            self.error=e
            try:seal()
            except BaseException:pass
    def close(self):
        if self.error:raise RuntimeError('durable_writer_failed') from self.error
        self.queue.put(None,timeout=10);self.thread.join(timeout=60)
        if self.thread.is_alive() or self.error:raise RuntimeError('durable_writer_drain_failed')
        if self.stats['accepted']!=self.stats['written'] or self.pending:raise RuntimeError('durable_count_mismatch')
        atomic(self.root/'manifest.json',self.stats);return self.stats

def read_journal(root):
    import io
    manifest=json.loads((Path(root)/'manifest.json').read_text())
    expected=1
    for entry in manifest['segments']:
        path=Path(root)/entry['file']
        if hashlib.sha256(path.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('journal_hash_mismatch')
        with path.open('rb') as f,zstd.ZstdDecompressor().stream_reader(f) as r,io.TextIOWrapper(r,encoding='utf8') as lines:
            for line in lines:
                row=json.loads(line)
                if row['collector_sequence']!=expected:raise ValueError('journal_sequence_gap')
                expected+=1;yield row
    if expected-1!=manifest['written']:raise ValueError('journal_readback_count')

class BookState:
    def __init__(self):self.books={};self.epochs={}
    def disconnect(self,markets):
        for m in markets:self.books.pop(str(m),None);self.epochs.pop(str(m),None)
    def predict(self,topic,data,epoch,event_reference=None):
        mid=topic.split('/')[-1]
        if not isinstance(data,dict) or str(data.get('marketId'))!=mid:raise ValueError('book_market_mismatch')
        if 'bids' not in data or 'asks' not in data:raise ValueError('missing_snapshot_side')
        book={'bids':levels(data['bids']),'asks':levels(data['asks']),'timestamp_source_ms':data.get('updateTimestampMs'),
              'native_version':data.get('version'),'native_index_set':1,'is_derived':False,'native_event_reference':event_reference}
        self.books[mid]=book;self.epochs[mid]=epoch;return book
    def polymarket(self,data,epoch):
        # Same envelope and exact arithmetic; native adapter remains separate.
        aid=str(data.get('asset_id'));kind=data.get('event_type')
        if kind=='book':
            self.books[aid]={s:[{'price':str(Decimal(str(x['price']))),'size':str(Decimal(str(x['size'])))} for x in data[s]] for s in ('bids','asks')};self.epochs[aid]=epoch
        elif kind=='price_change':
            for change in data.get('price_changes',[]):
                ident=str(change['asset_id'])
                if ident not in self.books or self.epochs.get(ident)!=epoch:continue
                side='bids' if change['side']=='BUY' else 'asks';p=str(Decimal(str(change['price'])));q=Decimal(str(change['size']))
                state={x['price']:x['size'] for x in self.books[ident][side]}
                if q==0:state.pop(p,None)
                else:state[p]=str(q)
                self.books[ident][side]=[{'price':p,'size':q} for p,q in sorted(state.items(),key=lambda x:Decimal(x[0]),reverse=side=='bids')]
        return self.books.get(aid)
