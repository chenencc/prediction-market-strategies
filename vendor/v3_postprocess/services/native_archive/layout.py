"""Archive v2: shared UTC-hour events, Shanghai-start-date Pair indexes."""
import re
from datetime import datetime,timezone
from zoneinfo import ZoneInfo

ROOT='v2'
VENUES={'polymarket','predictfun','myriad'}
NATIVE_POLY_FILES=('book.parquet','price_change.parquet','last_trade_price.parquet',
    'tick_size_change.parquet','best_bid_ask.parquet','new_market.parquet','market_resolved.parquet')
WRITER_SETTINGS=dict(compression='zstd',compression_level=9,version='2.6',data_page_version='2.0',
    row_group_size=1048576,use_dictionary=False,dictionary_pagesize_limit=8*1024**2)
DEBUG_CAPTURE_SECONDS=300

def timestamp(ns):
    if type(ns) is not int or ns<0:raise ValueError('nonnegative_integer_nanoseconds_required')
    return datetime.fromtimestamp(ns//10**9,timezone.utc)

def venue(value):
    if value not in VENUES:raise ValueError('unknown_archive_venue')
    return value

def part_id(value):
    if not isinstance(value,str) or not re.fullmatch('[a-zA-Z0-9_-]{1,80}',value):raise ValueError('invalid_archive_part_id')
    return value

def event_folder(platform,received_ns):
    at=timestamp(received_ns)
    return f'{ROOT}/events/venue={venue(platform)}/date={at:%Y-%m-%d}/hour={at:%H}'

def extension_part(dataset,platform,available_at_ns,part):
    if dataset=='objects':
        if platform!='shared':raise ValueError('objects_are_shared_across_venues')
        at=timestamp(available_at_ns)
        return f'{ROOT}/objects/date={at:%Y-%m-%d}/part-{part_id(part)}.parquet'
    if dataset not in {'markets','fees','market_observations','api_trades','reported_fees','quality'}:raise ValueError('unknown_archive_extension')
    at=timestamp(available_at_ns)
    return f'{ROOT}/{dataset}/venue={venue(platform)}/date={at:%Y-%m-%d}/part-{part_id(part)}.parquet'

def pair_folder(group_id,actual_started_at_ns):
    if not isinstance(group_id,str) or not re.fullmatch('[0-9a-f]{64}',group_id):raise ValueError('pair_group_hash_required')
    day=timestamp(actual_started_at_ns).astimezone(ZoneInfo('Asia/Shanghai'))
    return f'{ROOT}/pairs/start_date={day:%Y-%m-%d}/pair_id={group_id}'

def debug_window(started_at_ns,seconds=DEBUG_CAPTURE_SECONDS):
    timestamp(started_at_ns)
    if type(seconds) is not int or not 0<seconds<=DEBUG_CAPTURE_SECONDS:raise ValueError('debug_capture_must_be_1_to_300_seconds')
    return dict(start_ns=started_at_ns,end_ns=started_at_ns+seconds*10**9,auto_extend=False)
