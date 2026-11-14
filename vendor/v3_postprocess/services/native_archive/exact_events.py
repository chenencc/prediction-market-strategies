"""Exact native L2 event schemas for adapters without Poly's fixed precision.

The archive envelope/partition/writer is shared. Native outcome IDs, optional
clocks and actual supplied cursors remain explicit instead of inventing CTF IDs.
"""
import pyarrow as pa

LEVEL=pa.struct([('price',pa.string()),('size',pa.string())])
BASE=[('timestamp_received',pa.timestamp('ns',tz='UTC')),('sequence',pa.uint64()),
      ('event_index',pa.uint32()),('timestamp',pa.timestamp('ms',tz='UTC')),
      ('market_id',pa.string()),('asset_id',pa.binary(32)),('native_outcome_id',pa.uint32()),
      ('connection_epoch',pa.string()),('source_offset',pa.uint64()),('source_epoch',pa.string()),
      ('source_transport',pa.string()),('origin',pa.string()),('continuity_unavailable',pa.bool_())]
SCHEMAS={
    'book':pa.schema(BASE+[('bids',pa.list_(LEVEL)),('asks',pa.list_(LEVEL)),('boundary',pa.string())]),
    'price_change':pa.schema(BASE+[('change_index',pa.uint32()),('side',pa.string()),
        ('price',pa.string()),('size',pa.string()),('applied',pa.bool_())]),
    'best_bid_ask':pa.schema(BASE+[('quote_index',pa.uint32()),('best_bid',pa.string()),('best_ask',pa.string()),
        ('indicative_last_price',pa.string())]),
    'new_market':pa.schema(BASE+[('market_version_id',pa.string()),('creation_event_certified',pa.bool_())]),
    'market_resolved':pa.schema(BASE+[('status',pa.string()),('resolution_outcome_id',pa.int32()),
        ('voided',pa.bool_()),('payouts_certified',pa.bool_())]),
    'market_status':pa.schema(BASE+[('kind',pa.string()),('status',pa.string()),
        ('resolution_outcome_id',pa.int32()),('voided',pa.bool_())]),
    'tick_size_change':pa.schema(BASE+[('old_tick_size',pa.string()),('tick_size',pa.string()),
        ('tick_size_certified',pa.bool_())]),
}
