"""Offline provenance resolution. Never reconstruct unavailable wire history.

Frame references are scoped to one native collection/table. A cut may split a
message and its projections: callers must load the hash-linked previous shard,
not invent a frame from a projected event. This is not an exchange gap audit.
"""
import json
from decimal import Decimal
from task_control.contracts import require

SCHEMA='poly-ws-source/1'


def resolve(row,lookup):
    """lookup(sequence) returns the retained native source row, or None."""
    body=json.loads(row['data'],parse_float=Decimal)
    source=body.get('wire_source')
    if source is None:return dict(status='legacy_or_non_ws_unknown',original_message=None)
    require(source.get('schema')==SCHEMA,'wire_source_schema')
    sequence=source['frame_sequence']
    require(type(sequence) is int and sequence<int(row['sequence']),'wire_source_order')
    require(sequence>>48==int(row['sequence'])>>48,'wire_source_generation')
    original=lookup(sequence)
    require(original is not None,'wire_source_dependency_missing')
    frame=json.loads(original['data'],parse_float=Decimal)
    parents=json.loads(frame['payload'],parse_float=Decimal) if frame.get('message_kind')=='text' else None
    return _resolve_parsed(row,body,original,frame,parents)


def _resolve_parsed(row,body,original,frame,parents):
    """Internal bounded-cache path; parsed objects must come from these rows."""
    source=body['wire_source'];sequence=source['frame_sequence']
    require(source.get('schema')==SCHEMA,'wire_source_schema')
    require(type(sequence) is int and sequence<int(row['sequence']),'wire_source_order')
    require(sequence>>48==int(row['sequence'])>>48,'wire_source_generation')
    require(int(original['sequence'])==sequence,'wire_source_identity')
    require(frame.get('event_type')=='ws_message' and frame.get('schema')==SCHEMA and
            frame.get('message_kind')=='text','wire_original_type')
    for field in ('connection_id','connection_epoch'):
        require(type(source[field]) is int and source[field]==frame[field],'wire_connection_mismatch')
    require(int(row['received_ns'])==int(original['received_ns']),'wire_receive_clock_mismatch')
    payload=frame['payload'];require(isinstance(payload,str),'wire_original_text')
    if not isinstance(parents,list):parents=[parents]
    pi=source['parent_index'];ci=source['child_index']
    require(type(pi) is int and 0<=pi<len(parents) and type(ci) is int and ci>=0,'wire_source_index')
    parent=parents[pi];require(isinstance(parent,dict),'wire_source_parent')
    kind=parent.get('event_type');require(kind==body.get('event_type'),'wire_event_type_mismatch')
    if kind=='price_change':
        children=parent['price_changes'];require(ci<len(children),'wire_child_index')
        child=children[ci]
    else:
        require(ci==0,'wire_single_child_index');child=parent
    for field in ('market','timestamp'):
        require(body.get(field)==parent.get(field),'wire_parent_field_mismatch')
    for field in ('asset_id','side','transaction_hash','fee_rate_bps'):
        if field in body:require(body[field]==child.get(field),'wire_child_field_mismatch')
    for field in ('price','size','best_bid','best_ask','spread','old_tick_size','new_tick_size'):
        if body.get(field) is not None:
            require(child.get(field) is not None and Decimal(str(body[field]))==Decimal(str(child[field])),
                    'wire_decimal_mismatch')
    if kind=='book':
        for side in ('bids','asks'):
            expected=[(Decimal(v['price']),Decimal(v['size'])) for v in child[side]]
            actual=[(Decimal(v[0]),Decimal(v[1])) for v in body[side]]
            require(actual==expected,'wire_book_mismatch')
    expected_hash=child.get('hash')
    if not isinstance(expected_hash,str):expected_hash=None
    require(source.get('official_hash')==expected_hash,'wire_official_hash_mismatch')
    return dict(status='verified_source_reference',original_message=payload,parent=parent,child=child,
                source=source,exchange_completeness_certified=False)
