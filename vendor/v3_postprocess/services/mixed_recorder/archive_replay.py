"""Bounded typed-file readback and received L2 replay; no exchange completeness.

Native Poly exporter parts are ordered by market, asset, receive sequence.
Predict full snapshots need no inferred deltas or synthetic opposite-side events.
"""
from decimal import Decimal
import hashlib
import heapq
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from task_control.contracts import digest, require


def decimal(value):
    require(not isinstance(value, (float, bool)), 'exact_book_decimal_required')
    value = Decimal(value)
    require(value.is_finite(), 'finite_book_decimal_required')
    return value


def ladder(rows):
    out = {}
    for row in rows:
        p, q = decimal(row['price']), decimal(row['size'])
        require(0 <= p <= 1 and q >= 0 and p not in out, 'invalid_or_duplicate_book_level')
        out[p] = q
    return out


def state_wire(value):
    return {side:[[str(p), str(q)] for p,q in sorted(value[side].items())] for side in ('bids', 'asks')}


def rows(path, batch_size=128):
    with pq.ParquetFile(path, page_checksum_verification=True) as file:
        for batch in file.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            if 'timestamp_received' in table.column_names:
                table = table.set_column(table.schema.get_field_index('timestamp_received'),
                                         'timestamp_received_ns', table['timestamp_received'].cast(pa.int64()))
            yield from table.to_pylist()


def poly_rows(path, kind):
    previous = None
    for row in rows(path, 16 if kind == 'book' else 1024):
        if kind=='checkpoint':row=dict(row,market=row['condition_id'])
        require(isinstance(row['market'], bytes) and isinstance(row['asset_id'], bytes), 'native_book_identity_required')
        key = (row['market'], row['asset_id'], row['sequence'])
        require(previous is None or key > previous, 'native_part_order_not_strict')
        previous = key
        yield key, kind, row


def replay_books(root, entries):
    return replay_book_parts([(root,entries)])


def replay_book_parts(parts, *, state_sink=None):
    """Replay immutable segments with explicit, separately stored dependencies."""
    parts=list(parts);streams = []; predict = {}; pred_count = 0;poly_checkpoints=pred_checkpoints=0
    for root,entry in ((Path(root),entry) for root,entries in parts for entry in entries):
        name = entry['file']; path = root/name
        if '/events/venue=polymarket/' in name and path.name in {'book.parquet', 'price_change.parquet'}:
            streams.append(poly_rows(path, path.stem))
        checkpoint='/book_checkpoints/' in name
        if checkpoint and '/venue=polymarket/' in name:streams.append(poly_rows(path,'checkpoint'))
        if ('/events/venue=predictfun/' in name and path.name == 'book.parquet') or (checkpoint and '/venue=predictfun/' in name):
            for row in rows(path, 16):
                key = row['market_id'], row['asset_id'].hex()
                state = dict(bids=ladder(row['bids']), asks=ladder(row['asks']))
                previous = predict.get(key)
                if checkpoint:require(previous is None,'checkpoint_must_precede_new_events')
                if previous and previous[3]:require(row['sequence']>previous[0],'event_does_not_follow_checkpoint')
                if previous is None or row['sequence'] > previous[0]:
                    predict[key] = row['sequence'],state,row.get('condition_id'),checkpoint
                if checkpoint:pred_checkpoints+=1
                else:pred_count += 1
    current = None; state = None; last = None; snapshots = deltas = assets = 0; h = hashlib.sha256()
    def finish():
        if current is not None:
            h.update(digest([current[0].hex(), current[1].hex(), last, state_wire(state)]).encode())
            if state_sink:state_sink('polymarket',dict(condition_id=current[0],asset_id=current[1],sequence=last,
                                                     market_id=None,**state_wire(state)))
    for key, kind, row in heapq.merge(*streams, key=lambda item:item[0]):
        if current != key[:2]:
            finish(); current = key[:2]; state = None; last = None; assets += 1
        require(last is None or key[2] > last, 'duplicate_native_receive_sequence')
        if kind in {'book','checkpoint'}:
            if kind=='checkpoint':require(last is None,'checkpoint_must_precede_new_events');poly_checkpoints+=1
            state = dict(bids=ladder(row['bids']), asks=ladder(row['asks'])); snapshots += 1
            if kind=='checkpoint':snapshots-=1
        else:
            require(state is not None, 'delta_requires_snapshot_or_explicit_dependency')
            p, q = decimal(row['price']), decimal(row['size'])
            require(0 <= p <= 1 and q >= 0 and row['side'] in {'BUY','SELL'}, 'invalid_book_delta')
            side = 'bids' if row['side'] == 'BUY' else 'asks'
            if q == 0:state[side].pop(p, None)
            else:state[side][p] = q
            deltas += 1
        last = key[2]
    finish()
    pred_final = [[list(key), value[0], state_wire(value[1])] for key,value in sorted(predict.items())]
    if state_sink:
        for key,(seq,state,condition,_) in sorted(predict.items()):
            state_sink('predictfun',dict(market_id=key[0],asset_id=bytes.fromhex(key[1]),condition_id=condition,sequence=seq,**state_wire(state)))
    proof=dict(polymarket=dict(assets=assets, snapshots=snapshots, deltas=deltas, final_state_sha256=h.hexdigest()),
                predictfun=dict(assets=len(predict), snapshots=pred_count, final_state_sha256=digest(pred_final)),
                received_data_replay_only=True, gap_free_exchange_history_certified=False)
    if poly_checkpoints or pred_checkpoints:
        proof['derived_checkpoint_states']=dict(polymarket=poly_checkpoints,predictfun=pred_checkpoints)
    from task5.replay import replay
    myriad=replay(parts,state_sink=state_sink)
    if myriad is not None:proof['myriad']=myriad
    return proof


def final_states(proof):
    return {venue:{k:proof[venue][k] for k in ('assets','final_state_sha256')} for venue in ('polymarket','predictfun','myriad') if venue in proof}


def verify_cold_archive(root):
    import json
    manifest=json.loads((Path(root)/'manifest.json').read_text(encoding='utf8'))
    if 'component_contract' in manifest:
        from .components import verify_component
        return verify_component(root)
    from native_archive.storage_profile import verify_storage
    proof = verify_storage(root); page_rows = 0
    for entry in proof['files']:
        file = Path(root)/entry['file']; count = 0
        with pq.ParquetFile(file, page_checksum_verification=True) as reader:
            for batch in reader.iter_batches(batch_size=16 if file.name == 'book.parquet' else 512):count += batch.num_rows
        expected = entry.get('rows', entry.get('row_count'))
        require(expected is not None and count == expected, 'cold_parquet_row_count_changed')
        page_rows += count
    return dict(verified=True, profile=proof['schema'], manifest_semantic_sha256=digest(proof),
                parquet_files=len(proof['files']), parquet_rows=page_rows,
                books=replay_books(root, proof['files']), financial_replay=proof['trades'],
                full_exchange_fill_coverage=False)
