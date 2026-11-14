"""Exact, venue-tagged book operations plus explicit coverage diagnostics."""
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

LEVEL = pa.struct([('price', pa.string()), ('size', pa.string())])
SCHEMA = pa.schema([('venue', pa.string()), ('market_id', pa.string()), ('instrument', pa.string()),
    ('sequence', pa.uint64()), ('event_index', pa.uint32()), ('received_ns', pa.int64()),
    ('source_timestamp', pa.string()), ('connection_epoch', pa.string()),
    ('operation', pa.string()), ('bids', pa.list_(LEVEL)), ('asks', pa.list_(LEVEL))])

def levels(items):
    out = []
    for x in items:
        p, q = (x['price'], x['size']) if isinstance(x, dict) else x[:2]
        p, q = str(p), str(q)
        if not Decimal(p).is_finite() or not Decimal(q).is_finite() or Decimal(q) < 0:
            raise ValueError('invalid_level')
        out.append(dict(price=p, size=q))
    return out

def operations(event, venue):
    if venue == 'polymarket':
        if event.get('event_type') == 'book':
            yield dict(instrument=event['asset_id'], operation='snapshot', bids=levels(event['bids']), asks=levels(event['asks']))
        elif event.get('event_type') == 'price_change':
            for c in event.get('price_changes', [event]):
                if c['side'] not in ('BUY', 'SELL'): raise ValueError('unknown_book_side')
                yield dict(instrument=c['asset_id'], operation='delta',
                    bids=levels([c]) if c['side']=='BUY' else [], asks=levels([c]) if c['side']=='SELL' else [])
    elif venue == 'predictfun':
        raw = event.get('raw_payload')
        obj = json.loads(raw, parse_float=Decimal) if isinstance(raw, str) else raw
        if isinstance(obj, dict) and str(obj.get('topic', '')).startswith('predictOrderbook/'):
            d = obj['data']
            yield dict(instrument='predictfun:'+str(event['native_market_id'])+':indexSet=1',
                       operation='snapshot', bids=levels(d['bids']), asks=levels(d['asks']))
    elif venue == 'myriad':
        for n in event.get('normalized_events', []):
            if n['kind'] in ('snapshot', 'delta'):
                yield dict(instrument=n['instrument'], operation=n['kind'], bids=levels(n['bids']), asks=levels(n['asks']))

def assess(root):
    root=Path(root); counts=Counter(); event_types=Counter(); state={}; missing=Counter(); rows=[]; evidence=Counter(); catalog={}
    target=root/'books.parquet'
    with pq.ParquetWriter(target, SCHEMA, compression='zstd', compression_level=9, write_page_checksum=True) as writer:
        for folder in sorted(root.glob('segment-*')):
            for file in sorted(folder.glob('*.parquet')):
                with pq.ParquetFile(file, page_checksum_verification=True) as pf:
                    for batch in pf.iter_batches(batch_size=2048):
                        for row in batch.to_pylist():
                            venue=row['venue']; e=json.loads(row['data'], parse_float=Decimal)
                            event_types[venue+':'+row['event_type']]+=1
                            if 'metadata' in row['event_type']: evidence[venue+':metadata_events']+=1
                            if row['event_type']=='market_metadata' and e.get('http_status')==200:
                                m=json.loads(e['raw_payload'],parse_float=Decimal); m=m.get('data',m)
                                catalog[(venue,row['market_id'])]=dict(
                                    fee_fields_present=m.get('feesEnabled') is False or any(m.get(k) is not None for k in ('fees','feeSchedule','feeRateBps','takerBaseFee')),
                                    explicit_tick_present=any(m.get(k) is not None for k in ('tickSize','orderPriceMinTickSize','priceIncrement')),
                                    decimal_precision_present=m.get('decimalPrecision') is not None)
                            if any(x in row['event_type'] for x in ('disconnect','gap','invalidat')) or any(n.get('kind')=='invalidate' for n in e.get('normalized_events', [])):
                                for k in list(state):
                                    if k[0]==venue: del state[k]
                            for i, op in enumerate(operations(e, venue)):
                                epoch=e.get('connection_epoch'); key=(venue,op['instrument'])
                                if op['operation']=='snapshot': state[key]=epoch
                                elif key not in state or state[key]!=epoch: missing[venue]+=1
                                counts[venue+':'+op['operation']]+=1
                                rows.append(dict(venue=venue, market_id=row['market_id'], sequence=row['sequence'],
                                    event_index=i, received_ns=row['received_ns'], source_timestamp=str(e['timestamp']) if e.get('timestamp') is not None else None,
                                    connection_epoch=str(epoch) if epoch is not None else None, **op))
                                if len(rows)>=2048:
                                    writer.write_table(pa.Table.from_pylist(rows,schema=SCHEMA)); rows.clear()
        if rows: writer.write_table(pa.Table.from_pylist(rows,schema=SCHEMA))
    result=dict(schema='local-backtest-readiness/1', book_operations=dict(counts),
        delta_without_observed_snapshot=dict(missing), metadata_evidence=dict(evidence), event_types=dict(event_types),
        exact_price_size_strings=True, receive_clock_nanoseconds=True,
        coverage_check='per_table_snapshot_presence_only_not_merged_semantic_replay',
        instrument_fee_precision_semantics_certified=False, direct_nautilus_catalog=False,
        trades_complete=False, exchange_continuity_certified=False,
        notes=['Predict snapshots preserve indexSet=1 only; complementary books are not independent observations.',
               'Per-table sequence is authoritative; no atomic cross-venue ordering is asserted.',
               'Source timestamps are preserved in event data; adapter must respect each venue unit.',
               'Observed reset coverage is not exchange completeness. Fees/precision need instrument-level validation.',
               'Books-only archives cannot provide complete TradeTicks or queue-position fills.'])
    result['catalog_field_coverage']={v:dict(markets=sum(1 for k in catalog if k[0]==v),
        **{field:sum(int(value[field]) for k,value in catalog.items() if k[0]==v)
           for field in ('fee_fields_present','explicit_tick_present','decimal_precision_present')})
        for v in sorted({k[0] for k in catalog})}
    result['field_presence_is_not_fee_or_tick_semantic_validation']=True
    (root/'readiness.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
    return result
