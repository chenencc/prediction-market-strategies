"""Myriad native units and Centrifugo publications; no unified floats.

Independently implemented from the official public Order Book API. Reference
source pins (CCXT/Synpath) are documented in MYRIAD_OPINION_SOURCES.json.
"""
import hashlib
import json
import re

from marketdata.core import dumps, exact, iso_to_ns
from marketdata.l2 import level, scaled_integer

REST = 'https://api-v2.myriadprotocol.com'
WS = 'wss://ws.myriadprotocol.com/ws'


def identity(raw):
    if raw.get('tradingModel') != 'ob' or raw.get('executionMode') != 1:
        raise ValueError('myriad_orderbook_market_required')
    network, market = raw['networkId'], raw['id']
    if type(network) is not int or type(market) is not int or min(network, market) <= 0:
        raise ValueError('myriad_native_identity_required')
    token = raw.get('token') or {}
    decimals = token.get('decimals')
    if type(decimals) is not int or not 0 <= decimals <= 36:
        raise ValueError('myriad_token_decimals_required')
    outcomes = raw.get('outcomes') or []
    if any(type(o['id']) is not int for o in outcomes) or {o['id'] for o in outcomes} != {0, 1} or len(outcomes) != 2:
        raise ValueError('myriad_binary_native_outcomes_required')
    if not all(o.get('tokenId') is not None for o in outcomes):
        raise ValueError('myriad_outcome_token_identity_required')
    return f'{network}:ob:{market}'


def instrument(market, outcome):
    if type(outcome) is str and outcome in ('0','1'):
        outcome=int(outcome)
    if type(outcome) is not int or outcome not in (0, 1):
        raise ValueError('invalid_myriad_outcome')
    return f'myriad:{market}:{outcome}'


def metadata(raw):
    market = identity(raw)
    native = {k: v for k, v in raw.items() if k not in (
        'imageUrl', 'bannerImageUrl', 'ogImageUrl', 'topHolders', 'volume', 'volume24h',
        'volumeNotional', 'volumeNotional24h', 'liquidity', 'liquidityPrice', 'users', 'shares')}
    native['outcomes'] = [{k:v for k,v in outcome.items() if k not in
        ('shares','sharesHeld','price','priceChange24h','bestBid','bestAsk','holders','imageUrl','price_charts','priceCharts')}
        for outcome in raw['outcomes']]
    native['outcomes'].sort(key=lambda outcome:outcome['id'])
    return dict(kind='metadata', market_id=market, native=native,
                version_id=hashlib.sha256(json.dumps(json.loads(dumps(native)),sort_keys=True,
                    ensure_ascii=False,separators=(',',':')).encode()).hexdigest())


def snapshot(raw, market, outcome, decimals):
    def side(name):
        rows = raw[name]
        if not isinstance(rows, list):
            raise ValueError('myriad_snapshot_side_array_required')
        result = []
        for row in rows:
            if not isinstance(row, list) or len(row) != 2:
                raise ValueError('myriad_native_level_pair_required')
            result.append(level(scaled_integer(row[0], 18), scaled_integer(row[1], decimals)))
        return result
    return dict(kind='snapshot', market_id=market, instrument=instrument(market, outcome),
                bids=side('bids'), asks=side('asks'), source_timestamp_ms=None,
                boundary='unversioned_rest_snapshot')


def frames(raw):
    messages = [exact(line) for line in raw.split('\n') if line.strip()]
    if not all(isinstance(m, dict) for m in messages):
        raise ValueError('centrifugo_object_required')
    return messages


def channels(markets,include_trades=False):
    result = set()
    for raw in markets:
        identity(raw)
        result.add(f'markets:{raw["networkId"]}')
        for prefix in ('orderbook','prices')+ (('trades','settlements') if include_trades else ()):
            result.add(f'{prefix}:{raw["networkId"]}:{raw["id"]}')
    return sorted(result)


def publication(channel, pub, markets):
    parts = channel.split(':')
    if parts[0] == 'markets':
        data=pub['data']
        if len(parts)!=2 or str(data.get('networkId'))!=parts[1]:
            raise ValueError('myriad_lifecycle_network_mismatch')
        clock=data.get('ts')
        if type(clock) is str and re.fullmatch('[0-9]{1,16}',clock):clock=int(clock)
        if clock is not None and (type(clock) is not int or clock<0):
            raise ValueError('invalid_myriad_source_milliseconds')
        return [dict(kind='market_status', native=data, channel=channel,source_timestamp_ms=clock)]
    if len(parts) != 3:
        raise ValueError('unexpected_myriad_channel')
    prefix, network, mid = parts
    key = f'{network}:ob:{mid}'
    if key not in markets:
        raise ValueError('unsubscribed_myriad_market')
    raw, data = markets[key], pub['data']
    if str(data.get('networkId')) != network or str(data.get('marketId')) != mid:
        raise ValueError('myriad_channel_payload_identity_mismatch')
    stamp = data.get('ts')
    if type(stamp) is str and re.fullmatch('[0-9]{1,16}',stamp):
        stamp=int(stamp)
    if stamp is not None and (type(stamp) is not int or stamp < 0):
        raise ValueError('invalid_myriad_source_milliseconds')
    cursor = {k: pub[k] for k in ('offset', 'epoch') if k in pub}
    if prefix == 'orderbook':
        by_outcome = {}
        for change in data['changes']:
            outcome = change['outcome']
            asset = instrument(key, outcome)
            outcome = int(outcome)
            side = {'bid': 'bids', 'ask': 'asks'}.get(change['side'])
            if side is None:
                raise ValueError('invalid_myriad_native_side')
            target = by_outcome.setdefault(outcome, dict(kind='delta', market_id=key,
                instrument=asset, bids=[], asks=[], source_timestamp_ms=stamp,
                source_cursor=cursor or None))
            target[side].append(level(scaled_integer(change['price'], 18),
                                     scaled_integer(change['amount'], raw['token']['decimals'])))
        return list(by_outcome.values())
    kind = {'trades': 'trade', 'settlements': 'settlement', 'prices': 'price'}[prefix]
    return [dict(kind=kind, market_id=key, native=data, source_timestamp_ms=stamp,
                 source_cursor=cursor or None)]


def trade_time_ns(row):
    value = row.get('timestamp')
    if isinstance(value, str) and ('T' in value):
        return iso_to_ns(value)
    if type(value) is int and 0 <= value < 10**11:
        return value * 10**9  # REST documented Unix seconds, not WS ts ms.
    if isinstance(value, str) and re.fullmatch('[0-9]{1,11}', value):
        return int(value) * 10**9
    return None


class ArchiveValidator:
    """Decode preserved source bodies again, independently of live event fields."""
    def __init__(self, project_metadata=False):
        self.markets={}
        self.project_metadata=project_metadata
        self.seen_metadata=set()
        self.checked_rows=0

    def __call__(self,row):
        actual=row.get('normalized_events') or []
        expected=[]
        kind=row['event_type']
        if kind in ('metadata_input','metadata_version'):
            source=exact(row['raw_payload'])
            event=metadata(source)
            self.markets[event['market_id']]=source
            if not self.project_metadata:
                # Historical captures can use an older version-hash algorithm;
                # validate semantic source fields after the current projection.
                if len(actual)!=1 or dumps(metadata(actual[0]['native'])['native'])!=dumps(event['native']):
                    raise ValueError('metadata_projection_changed_from_native_source')
            else:
                if event['version_id'] in self.seen_metadata:
                    actual=None
                else:
                    self.seen_metadata.add(event['version_id']);actual=[event]
        elif kind=='ws_frame':
            for item in frames(row['raw_payload']):
                push=item.get('push',{})
                if 'pub' in push:
                    expected+=publication(push['channel'],push['pub'],self.markets)
            observed=[{k:v for k,v in e.items() if k not in ('applied','state_sha256')}
                      for e in actual if e['kind'] in ('delta','trade','settlement','price','market_status')]
            if dumps(expected)!=dumps(observed):
                raise ValueError('WS_projection_changed_from_native_source')
        elif kind=='http_response' and row.get('http_status')==200:
            if row['request_path'].endswith('/orderbook'):
                snapshots=[e for e in actual if e['kind']=='snapshot']
                if snapshots:
                    market=row['native_market_id'];native=self.markets[market]
                    expected=snapshot(exact(row['raw_payload']),market,row['request_params']['outcome'],native['token']['decimals'])
                    if len(snapshots)!=1 or any(dumps(snapshots[0].get(k))!=dumps(v)
                        for k,v in expected.items() if k!='boundary'):
                        raise ValueError('snapshot_projection_changed_from_native_source')
            elif row['request_path'].endswith('/trades'):
                source=exact(row['raw_payload']);rows=source if isinstance(source,list) else source['data']
                pages=[e for e in actual if e['kind']=='trade_page']
                if len(pages)!=1 or dumps(pages[0]['native'])!=dumps(dict(page=row['request_params']['page'],rows=rows)):
                    raise ValueError('trade_page_projection_changed_from_native_source')
        self.checked_rows+=1
        return actual
