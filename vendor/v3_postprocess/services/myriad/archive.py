"""Myriad source adapter into the existing native -> nautilus-source/2 writer.

No provider calls or trades. Shared catalog, fee versions, date partitions,
ZSTD9, transactional compaction and readback are the existing implementations.
"""
from collections import Counter,defaultdict
from itertools import groupby
from pathlib import Path
import hashlib
import heapq
import json
import time

import pyarrow.parquet as pq

from marketdata.core import atomic,dumps,exact,read_journal
from marketdata.l2 import ExactL2,scaled_integer,level
from native_archive.catalog import market_version,version_observation
from native_archive.exact_events import SCHEMAS
from native_archive.layout import event_folder
from native_archive.storage_profile import compact_for_storage,verify_storage,dataset_rows
from native_archive.stream_writer import StreamArchive
from native_archive.trades import quality_observation
from . import protocol


CAPABILITIES={
    'book':dict(source='official per-outcome REST snapshots',native=True),
    'price_change':dict(source='orderbook WS absolute level sizes',native=True),
    'best_bid_ask':dict(source='prices WS; book replay when no quote event',native=True,stored_derived_copies=False),
    'new_market':dict(source='catalog discovery / created WS event',native_creation_not_inferred_from_discovery=True),
    'market_resolved':dict(source='explicit resolved metadata or WS event',payouts_not_invented=True),
    'tick_size_change':dict(source='explicit metadata tick change only',independent_WS_topic_available=False,
        documented_default='0.01',documented_default_is_not_observed_market_tick=True),
    'last_trade_price':dict(status='not_requested'),
}


def _asset(markets,mid,outcome):
    token=next(o['tokenId'] for o in markets[mid]['outcomes'] if o['id']==outcome)
    if not isinstance(token,(str,int)) or isinstance(token,bool) or not str(token).isdigit() or not 0<=int(token)<2**256:
        raise ValueError('native_uint256_token_required')
    return int(token).to_bytes(32,'big')


def _base(row,index,event,markets,outcome=None):
    mid=event.get('market_id',row.get('native_market_id')) or '__collector__'
    cursor=event.get('source_cursor') or {}
    offset=cursor.get('offset')
    if offset is not None and (type(offset) is not int or not 0<=offset<2**64):
        raise ValueError('native_offset_uint64_required')
    return dict(timestamp_received=row['timestamp_received_ns'],sequence=row['collector_sequence'],
        event_index=index,timestamp=event.get('source_timestamp_ms'),market_id=mid,
        asset_id=_asset(markets,mid,outcome) if outcome is not None else None,native_outcome_id=outcome,
        connection_epoch=row['connection_epoch'],source_offset=offset,source_epoch=cursor.get('epoch'),
        source_transport=row['source_transport'],origin='source',continuity_unavailable=False)


def _operation(digest,sequence,index,key=None,applied=None,state=None):
    digest.update(dumps([sequence,index,key,applied,state]).encode()+b'\n')


def verify_events(root,manifest,expected,*,state=None,catalog_parts=None):
    """Independently rebuild source state from final typed snapshots/deltas."""
    root=Path(root);streams=[];state=ExactL2() if state is None else state;digest=hashlib.sha256();count=0;assets=defaultdict(set)
    for catalog_root,catalog_manifest in catalog_parts or [(root,manifest)]:
        for definition in dataset_rows(catalog_root,catalog_manifest,'instruments','myriad'):
            for instrument in json.loads(definition['instruments_json']):
                assets[(definition['market_id'],instrument['native_outcome_id'])].add(int(instrument['asset_id']).to_bytes(32,'big'))
    def event_rows(entry):
        path=entry['file'];name=Path(path).stem
        previous=None
        with pq.ParquetFile(root/path,page_checksum_verification=True) as reader:
            for batch in reader.iter_batches(batch_size=1024):
                for row in batch.to_pylist():
                    if name=='market_status' and row['kind'] not in ('connection_invalidated','member_connection_invalidated'):continue
                    if name!='market_status' and row['asset_id'] not in assets[(row['market_id'],row['native_outcome_id'])]:
                        raise ValueError('native_asset_catalog_binding_changed')
                    key=(row['sequence'],row['event_index'],row.get('change_index',0))
                    if previous is not None and key<=previous:raise ValueError('native_file_order_changed')
                    previous=key
                    yield (row['sequence'],row['event_index'],name,row)
    for entry in manifest['files']:
        path=entry['file'];name=Path(path).stem
        if '/events/venue=myriad/' in path and name in ('book','price_change','market_status'):
            streams.append(event_rows(entry))
    events=heapq.merge(*streams,key=lambda e:(e[0],e[1],e[3].get('change_index',0)))
    for (sequence,index),group in groupby(events,key=lambda e:(e[0],e[1])):
        group=list(group);kind=group[0][2];row=group[0][3]
        if kind=='market_status':
            if row['kind']=='member_connection_invalidated':
                key=protocol.instrument(row['market_id'],row['native_outcome_id']);state.invalidate([key]);_operation(digest,sequence,index,key,False,None)
            else:state.invalidate();_operation(digest,sequence,index)
            continue
        key=protocol.instrument(row['market_id'],row['native_outcome_id'])
        if any(protocol.instrument(r['market_id'],r['native_outcome_id'])!=key for _,_,_,r in group):
            raise ValueError('native_delta_group_instrument_changed')
        if kind=='book':
            if len(group)!=1:raise ValueError('duplicate_native_snapshot')
            state.snapshot(key,row['connection_epoch'],row['bids'],row['asks']);applied=True
        else:
            changes=dict(bids=[],asks=[])
            for position,(_,_,_,item) in enumerate(group):
                if item['change_index']!=position:raise ValueError('native_delta_level_order_changed')
                side={'BUY':'bids','SELL':'asks'}.get(item['side'])
                if side is None:raise ValueError('native_delta_side_changed')
                changes[side].append(dict(price=item['price'],size=item['size']))
            applied=state.delta(key,row['connection_epoch'],changes['bids'],changes['asks'])
            if any(r['applied']!=applied for _,_,_,r in group):raise ValueError('native_delta_readiness_changed')
        _operation(digest,sequence,index,key,applied,state.digest(key) if applied else None);count+=1
    if digest.hexdigest()!=expected:raise ValueError('typed_native_book_replay_changed')
    return dict(semantic_sha256=expected,book_operations=count,exact_snapshot_delta_replay=True,bounded_batch_rows=1024)


class NativeProjection:
    """Single-pass source decoder on an existing common StreamArchive.

    One instance owns reconstruction across supplied rows. Deltas which claim
    an applied state without their prior snapshots fail; no segment seed or
    exchange snapshot is fabricated. The caller supplies verified prior rows
    when a component depends on earlier native history.
    """
    def __init__(self,writer):
        self.writer=writer;self.state=ExactL2();self.validator=protocol.ArchiveValidator()
        self.markets={};self.bindings={};self.seen_markets=set();self.last_tick={}
        self.counts=Counter();self.source_hash=hashlib.sha256();self.gap_start=None
        self.ready=set();self.gaps=[];self.all_assets=set();self.window_source=None;self.scoped_gap_starts={}

    def write(self,kind,row):
        self.writer.event(event_folder('myriad',row['timestamp_received'])+'/'+kind+'.parquet',SCHEMAS[kind],row)
        self.counts[kind]+=1

    def quality(self,row,kind,detail,mid='__collector__'):
        self.writer.catalog('quality',quality_observation('myriad',mid,detail,kind=kind,
            available_at_ns=row['timestamp_received_ns'],source_ref=row.get('_source_ref','myriad_native:'+str(row['collector_sequence']))))

    def event(self,row):
        self.counts['native_envelopes']+=1;kind=row['event_type'];at=row['timestamp_received_ns'];sequence=row['collector_sequence']
        events=self.validator(row)
        if kind=='recording_window':self.window_source=exact(row['raw_payload'])
        if kind in ('metadata_input','metadata_version'):
            native=protocol.metadata(exact(row['raw_payload']))['native'];mid=protocol.identity(native)
            self.markets[mid]=native;self.all_assets.update(protocol.instrument(mid,o['id']) for o in native['outcomes'])
            version,fee=market_version('myriad',native,observed_at_ns=at,source_ref=row.get('_source_ref','myriad_native:'+str(sequence)))
            binding=(version['market_version_id'],fee['fee_version_id'])
            if self.bindings.get(mid)!=binding:
                self.writer.catalog('markets',version);self.writer.catalog('fees',fee)
                self.writer.catalog('market_observations',version_observation(version,fee));self.bindings[mid]=binding
            base=_base(row,0,dict(market_id=mid),self.markets)
            if mid not in self.seen_markets:
                self.seen_markets.add(mid)
                self.write('new_market',base|dict(origin='catalog_observation',creation_event_certified=False,
                    market_version_id=version['market_version_id']))
            tick=version['price_increment']
            if mid in self.last_tick and self.last_tick[mid] is not None and tick is not None and self.last_tick[mid]!=tick:
                self.write('tick_size_change',base|dict(origin='metadata_observation',old_tick_size=self.last_tick[mid],tick_size=tick,tick_size_certified=True))
            self.last_tick[mid]=tick
            if native.get('state')=='resolved':
                self.write('market_resolved',base|dict(origin='resolved_metadata_observation',status='resolved',
                    resolution_outcome_id=native.get('resolvedOutcomeId'),voided=native.get('voided'),payouts_certified=False))
            return
        for index,event in enumerate(events):
            event_kind=event['kind'];mid=event.get('market_id')
            if event_kind in ('trade','settlement','trade_page'):
                self.counts['trade_records_excluded']+=1;continue
            if event_kind=='invalidate':
                scope=event.get('instruments')
                if scope is not None:
                    if len(scope)!=1:raise ValueError('one_scoped_invalidation_per_native_event_required')
                    key=scope[0];mid=key.removeprefix('myriad:').rsplit(':',1)[0];outcome=int(key.rsplit(':',1)[1])
                    if key!=protocol.instrument(mid,outcome):raise ValueError('invalid_scoped_native_instrument')
                    self.state.invalidate(scope);self.ready.difference_update(scope);self.scoped_gap_starts.setdefault(key,at)
                    self.write('market_status',_base(row,index,event|dict(market_id=mid),self.markets,outcome)|dict(
                        kind='member_connection_invalidated',status=None,origin='collector_control',continuity_unavailable=True))
                    _operation(self.source_hash,sequence,index,key,False,None)
                else:
                    self.state.invalidate();self.ready.clear()
                    if self.gap_start is None:self.gap_start=at
                    self.write('market_status',_base(row,index,event,self.markets)|dict(kind='connection_invalidated',status=None,
                        origin='collector_control',continuity_unavailable=True))
                    _operation(self.source_hash,sequence,index)
            elif event_kind in ('snapshot','delta'):
                key=event['instrument'];outcome=int(key.rsplit(':',1)[1]);base=_base(row,index,event,self.markets,outcome)
                if event_kind=='snapshot':
                    self.state.snapshot(key,row['connection_epoch'],event['bids'],event['asks']);applied=True;self.ready.add(key)
                    scoped_start=self.scoped_gap_starts.pop(key,None)
                    if scoped_start is not None:self.gaps.append(dict(venue='myriad',start_ns=scoped_start,end_ns=at,
                        kind='observed_member_readiness_recovery_interval',reported_gap_seconds=str((at-scoped_start)/10**9),
                        affected_assets=1,asset_scope_known=True,instrument=key))
                    self.write('book',base|dict(bids=event['bids'],asks=event['asks'],boundary=event.get('boundary'),
                        continuity_unavailable=True))
                    if self.ready==self.all_assets and self.gap_start is not None:
                        self.gaps.append(dict(venue='myriad',start_ns=self.gap_start,end_ns=at,kind='observed_readiness_recovery_interval',
                            reported_gap_seconds=str((at-self.gap_start)/10**9),affected_assets=len(self.all_assets),asset_scope_known=True));self.gap_start=None
                else:
                    applied=self.state.delta(key,row['connection_epoch'],event['bids'],event['asks'])
                    change_index=0
                    for side in ('bids','asks'):
                        for change in event[side]:
                            self.write('price_change',base|dict(change_index=change_index,side='BUY' if side=='bids' else 'SELL',price=change['price'],size=change['size'],
                                applied=applied,continuity_unavailable=not applied));change_index+=1
                actual=self.state.digest(key) if applied else None
                if event['applied']!=applied or event['state_sha256']!=actual:raise ValueError('native_live_reconstruction_changed')
                _operation(self.source_hash,sequence,index,key,applied,actual)
            elif event_kind=='price':
                native=event['native']
                for offset,outcome_row in enumerate(native.get('outcomes') or []):
                    protocol.instrument(mid,outcome_row['outcome'])
                    outcome=int(outcome_row['outcome']);base=_base(row,index,event,self.markets,outcome)
                    def price(name):
                        value=outcome_row.get(name)
                        return level(scaled_integer(value,18),'0')['price'] if value is not None else None
                    self.write('best_bid_ask',base|dict(quote_index=offset,best_bid=price('bestBid'),best_ask=price('bestAsk'),
                        indicative_last_price=price('last')))
            elif event_kind=='market_status':
                native=event['native'];mid=f'{native.get("networkId")}:ob:{native.get("marketId")}'
                if mid not in self.markets:self.counts['unselected_lifecycle_events']+=1;continue
                base=_base(row,index,event|dict(market_id=mid),self.markets)
                if native.get('type')=='created':
                    self.write('new_market',base|dict(creation_event_certified=True,market_version_id=self.bindings[mid][0]))
                elif native.get('type')=='resolved':
                    self.write('market_resolved',base|dict(status='resolved',resolution_outcome_id=native.get('resolvedOutcomeId'),
                        voided=native.get('voided'),payouts_certified=False))
                else:self.write('market_status',base|dict(kind='native_lifecycle',status=native.get('type')))
            elif event_kind=='reconcile' and event['result'] in ('mismatched','inflight_delta_uncertain'):
                self.quality(row,'book_'+event['result'],dict(reasons=[event['result']],instrument=event['instrument']),mid)
        if 'error' in kind or 'gap' in kind or kind.endswith('rejection'):
            try:detail=exact(row['raw_payload'])
            except (ValueError,TypeError):detail={}
            self.quality(row,kind,dict(reasons=[kind],request_path=row.get('request_path') or detail.get('path')),
                row.get('native_market_id') or detail.get('market') or '__collector__')

def project_native(journal_root,destination,window=None):
    started=time.perf_counter();destination=Path(destination)
    if destination.exists():raise ValueError('new_native_archive_destination_required')
    stage=destination.with_name(destination.name+'-native-stage')
    if stage.exists():raise ValueError('new_owned_native_stage_required')
    writer=StreamArchive(stage);projection=NativeProjection(writer)
    try:
        for row in read_journal(journal_root):projection.event(row)
        writer.close()
    except BaseException:
        writer.abort();raise
    window_source=projection.window_source;gaps=projection.gaps;gap_start=projection.gap_start
    all_assets=projection.all_assets;counts=projection.counts;source_hash=projection.source_hash
    if window is None:window=window_source
    if not window or type(window['start_ns']) is not int or type(window['end_ns']) is not int or window['end_ns']<=window['start_ns']:
        raise ValueError('fixed_recording_window_required')
    if window_source and (window_source['start_ns'],window_source['end_ns'])!=(window['start_ns'],window['end_ns']):
        raise ValueError('recording_window_changed')
    gaps=[g for g in gaps if g['start_ns']<window['end_ns'] and g['end_ns']>window['start_ns']]
    if gap_start is not None and gap_start<window['end_ns']:
        gaps.append(dict(venue='myriad',start_ns=gap_start,end_ns=window['end_ns'],kind='right_censored_readiness_interval',
            reported_gap_seconds=None,affected_assets=len(all_assets),asset_scope_known=True))
    for key,start in projection.scoped_gap_starts.items():
        if start<window['end_ns']:gaps.append(dict(venue='myriad',start_ns=start,end_ns=window['end_ns'],
            kind='right_censored_member_readiness_interval',reported_gap_seconds=None,affected_assets=1,asset_scope_known=True,instrument=key))
    proof=compact_for_storage(stage,destination,window=window,source_gaps=gaps)
    replay=verify_events(destination,proof,source_hash.hexdigest())
    proof.update(collection_scope='books_prices_lifecycle_and_metadata',trade_collection_status='not_requested',
        trade_pagination_or_tail_requested=False,myriad_information_capabilities=CAPABILITIES,
        native_source_decode_replay=replay,adapter_counts=dict(counts),native_journal_retained=True,
        full_source_coverage_certified=False,adapter_projection_seconds=time.perf_counter()-started)
    atomic(destination/'manifest.json',proof);verify_storage(destination,proof)
    return proof
