"""Exact received Myriad L2 replay in common component/checkpoint acceptance."""
import heapq
from itertools import groupby
from pathlib import Path

from marketdata.l2 import ExactL2
from myriad.protocol import instrument
from task_control.contracts import digest,require


def replay(parts,*,state_sink=None,state=None):
    from mixed_recorder.archive_replay import rows
    streams=[];state=ExactL2() if state is None else state;latest={};counts=dict(snapshots=0,deltas=0,unapplied_deltas=0,checkpoints=0)
    def ordinary(root,entry):
        name=Path(entry['file']).stem
        for row in rows(Path(root)/entry['file'],16 if name=='book' else 1024):
            if name=='market_status' and row['kind'] not in ('connection_invalidated','member_connection_invalidated'):continue
            yield (row['sequence'],row['event_index'],row.get('change_index',0)),name,row
    for root,entries in parts:
        for entry in entries:
            name=entry['file']
            if '/events/venue=myriad/' in name and Path(name).stem in ('book','price_change','market_status'):
                streams.append(ordinary(root,entry))
            elif '/book_checkpoints/venue=myriad/' in name:
                values=list(rows(Path(root)/name,16));require(len(values)<=16384,'bounded_myriad_checkpoint_states_required')
                streams.append(iter([((r['sequence'],-1,r['native_outcome_id']),'checkpoint',r) for r in sorted(values,key=lambda r:(r['sequence'],r['native_outcome_id'],r['market_id']))]))
    if not streams:return None
    previous=None
    for (seq,index),values in groupby(heapq.merge(*streams,key=lambda e:e[0]),key=lambda e:e[0][:2]):
        group=list(values);name=group[0][1];row=group[0][2]
        # Checkpoint rows may share a source sequence while representing different books.
        if name=='checkpoint':
            for _,_,r in group:
                key=instrument(r['market_id'],r['native_outcome_id']);require(key not in state.books,'myriad_checkpoint_overlap')
                state.snapshot(key,r['connection_epoch'],r['bids'],r['asks']);latest[key]=r;counts['checkpoints']+=1
            continue
        require(previous is None or (seq,index)>previous,'myriad_replay_event_order_changed');previous=(seq,index)
        if name=='market_status':
            if row['kind']=='member_connection_invalidated':
                key=instrument(row['market_id'],row['native_outcome_id']);state.invalidate([key]);latest.pop(key,None)
            else:state.invalidate();latest.clear()
            continue
        key=instrument(row['market_id'],row['native_outcome_id'])
        require(all(instrument(r['market_id'],r['native_outcome_id'])==key and r['asset_id']==row['asset_id'] for _,_,r in group),
            'myriad_delta_native_identity_changed')
        if name=='book':
            require(len(group)==1,'duplicate_myriad_snapshot');state.snapshot(key,row['connection_epoch'],row['bids'],row['asks']);applied=True
            counts['snapshots']+=1
        else:
            changes=dict(bids=[],asks=[])
            for position,(_,_,r) in enumerate(group):
                require(r['change_index']==position and r['side'] in ('BUY','SELL'),'myriad_delta_level_order_or_side_changed')
                changes['bids' if r['side']=='BUY' else 'asks'].append(dict(price=r['price'],size=r['size']))
            applied=state.delta(key,row['connection_epoch'],changes['bids'],changes['asks'])
            require(all(r['applied']==applied for _,_,r in group),'myriad_delta_readiness_changed')
            counts['deltas']+=1;counts['unapplied_deltas']+=not applied
        if applied:latest[key]=row
    final=[]
    for key in sorted(state.books):
        row=latest[key];final.append([key,row['sequence'],row['connection_epoch'],state.digest(key)])
        if state_sink:
            state_sink('myriad',dict(condition_id=None,market_id=row['market_id'],asset_id=row['asset_id'],
                native_outcome_id=row['native_outcome_id'],connection_epoch=row['connection_epoch'],sequence=row['sequence'],
                **{side:[[r['price'],r['size']] for r in values] for side,values in state.levels(key).items()}))
    return dict(assets=len(state.books),final_state_sha256=digest(final),**counts)
