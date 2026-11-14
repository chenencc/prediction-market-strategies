"""Offline incremental Nautilus data catalogs over immutable standard releases.

This is a data replay profile, not execution specifications or a fee model.
No credentials, network clients, recorder imports or source mutations.
"""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from decimal import Decimal, localcontext
import gzip
import json
import os
from pathlib import Path
import shutil
import time

import pyarrow as pa
import pyarrow.parquet as pq
from mixed_recorder.publication import bound_file, sha_file
from mixed_recorder.store import durable_file, encoded, sync_directory
from native_archive.fees import digest
from pair_recorder.supervisor import OwnerLock
from .standard import verify_release

PROFILE = 'nautilus-standard-replay/1'


def decimal_key(value):
    with localcontext() as ctx:
        ctx.prec = 160
        return str(Decimal(value).normalize())


def rows(root, name):
    for path in sorted(Path(root).glob(name + '/**/*.parquet')):
        with pq.ParquetFile(path, page_checksum_verification=True) as pf:
            for batch in pf.iter_batches(batch_size=2048):
                yield from batch.to_pylist()


def number(value, cls, precision=None):
    """Trim insignificant zeros only; never round to engine or venue precision."""
    if not isinstance(value, (str, Decimal)):
        raise ValueError('exact_decimal_required')
    d = Decimal(value)
    if not d.is_finite():
        raise ValueError('finite_decimal_required')
    with localcontext() as ctx:
        ctx.prec = 160
        d = d.normalize()
        places = max(0, -d.as_tuple().exponent)
        if places > (18 if precision is None else precision):
            raise ValueError('engine_precision_overflow')
        return cls.from_str(format(d, 'f'))


def native_id(venue, market, asset):
    # Digest avoids collisions, dots/slashes in native IDs and duplicated Poly
    # streams per Pair. Original identity remains in instrument.info and index.
    return 'md-' + digest([venue, str(market), str(asset)])[:32] + '.' + venue.upper()


def book_asset(row, definitions):
    candidates = [d for d in definitions.values() if d['venue'] == row['venue']
                  and d['market_id'] == row['market_id']]
    if row['venue'] == 'predictfun':
        candidates = [d for d in candidates if d.get('index_set') == 1]
    elif row['venue'] == 'myriad':
        candidates = [d for d in candidates if str(d.get('native_outcome_id')) == row['instrument'].rsplit(':', 1)[-1]]
    else:
        candidates = [d for d in candidates if d['native_asset_id'] == row['instrument']]
    assets = {d['native_asset_id'] for d in candidates}
    if len(assets) != 1:
        raise ValueError('instrument_identity_unresolved')
    return next(iter(assets))


def make_instrument(venue, market, asset, at):
    from nautilus_trader.model import (BinaryOption, InstrumentId, Symbol,
        Currency, CurrencyType, AssetClass, Price, Quantity)
    # Dimensionless probability replay unit, explicitly NOT a collateral guess.
    # 1e-16 is a storage representation grid, NOT an exchange order increment.
    return BinaryOption(
        instrument_id=InstrumentId.from_str(native_id(venue, market, asset)),
        raw_symbol=Symbol(str(asset)), asset_class=AssetClass.ALTERNATIVE,
        currency=Currency('XPROB', 0, 0, 'Dimensionless probability replay', CurrencyType.CRYPTO),
        activation_ns=0, expiration_ns=0, price_precision=16, size_precision=16,
        price_increment=Price.from_str('0.0000000000000001'),
        size_increment=Quantity.from_str('0.0000000000000001'),
        ts_event=at, ts_init=at, outcome=str(asset),
        info=dict(profile=PROFILE, venue=venue, native_market_id=market,
                  native_asset_id=str(asset), dimensionless_data_replay=True,
                  increments_are_representation_grid=True,
                  execution_specifications_certified=False, fee_simulation_enabled=False,
                  metadata_policy='as-of version assignments in context sidecar'))


def book_batch(row, instrument, snapshot, levels, *, availability_ns=None):
    from nautilus_trader.model import (BookOrder, OrderBookDelta, OrderBookDeltas,
        BookAction, OrderSide, RecordFlag, Price, Quantity)
    te = row['source_ns'] if row.get('source_ns') is not None else row['received_ns']
    ti = row['received_ns'] if availability_ns is None else availability_ns
    if te <= 0 or ti <= 0:
        raise ValueError('positive_book_clock_required')
    out = []
    if snapshot:
        out.append(OrderBookDelta.clear(instrument.id, 0, te, ti))
        levels.clear()
    for side, key in ((OrderSide.BUY, 'bids'), (OrderSide.SELL, 'asks')):
        for level in row[key]:
            px, size = Decimal(level['price']), Decimal(level['size'])
            if not 0 <= px <= 1 or size < 0:
                raise ValueError('invalid_book_level')
            identity = (key, decimal_key(px))
            action = BookAction.DELETE if size == 0 else BookAction.UPDATE if identity in levels else BookAction.ADD
            if size == 0:
                levels.discard(identity)
            else:
                levels.add(identity)
            order = BookOrder(side, number(level['price'], Price), number(level['size'], Quantity), 0)
            out.append(OrderBookDelta(instrument.id, action, order,
                int(RecordFlag.F_SNAPSHOT) if snapshot else 0, 0, te, ti))
    if not out:
        return None
    last = out[-1]
    out[-1] = OrderBookDelta(last.instrument_id, last.action, last.order,
        int(last.flags) | int(RecordFlag.F_LAST) | (int(RecordFlag.F_SNAPSHOT) if snapshot else 0),
        0, te, ti)
    return OrderBookDeltas(instrument.id, out)


def clear_batch(instrument, at, *, availability_ns=None):
    from nautilus_trader.model import OrderBookDelta, OrderBookDeltas, RecordFlag
    available = at if availability_ns is None else availability_ns
    d = OrderBookDelta.clear(instrument.id, 0, at, available)
    return OrderBookDeltas(instrument.id, [OrderBookDelta(d.instrument_id, d.action, d.order,
        int(RecordFlag.F_LAST), 0, at, available)])


def book_availability(clocks, instrument_id, received_ns):
    """Do not expose a source-sequence successor before its predecessor."""
    previous = clocks.get(instrument_id, 0)
    return previous + 1 if received_ns < previous else received_ns


def fingerprint(event):
    # Object JSON keeps exact decimals including precision. Catalog may remove
    # insignificant trailing zeros, so compare numeric values independently.
    base = [type(event).__name__, str(event.instrument_id), event.ts_event, event.ts_init]
    if type(event).__name__ == 'OrderBookDeltas':
        return digest(base + [[[str(d.action), str(d.order.side),
            None if str(d.action) == 'CLEAR' else decimal_key(d.order.price.as_decimal()),
            None if str(d.action) == 'CLEAR' else decimal_key(d.order.size.as_decimal()),
            int(d.flags), d.sequence] for d in event.deltas]])
    return digest(base + [decimal_key(event.price.as_decimal()), decimal_key(event.size.as_decimal()),
                          str(event.aggressor_side), str(event.trade_id)])


def trade_values(payload):
    with localcontext() as ctx:
        ctx.prec = 160
        p, q = Decimal(payload['price']), Decimal(payload['size'])
        if payload['price_unit'] == 'native_wei' and payload['venue'] == 'predictfun':
            p = p.scaleb(-18)
        elif payload['price_unit'] != 'probability':
            raise ValueError('trade_price_unit_unresolved')
        if payload['size_unit'] == 'native_wei' and payload['venue'] == 'predictfun':
            q = q.scaleb(-18)
        elif payload['size_unit'] != 'shares':
            raise ValueError('trade_size_unit_unresolved')
        if not 0 <= p <= 1 or q <= 0:
            raise ValueError('invalid_trade_values')
        return format(p, 'f'), format(q, 'f')


def payload_projection(row):
    detail = json.loads(row['detail_json'])
    if 'compact_trade_id' in detail:
        return {k: v for k, v in row.items() if k != 'detail_json'} | dict(
            asset_id=detail['normalized'].get('asset_id'),
            content_key=digest(['v3', detail['compact_collection'], detail['compact_trade_id']]))
    native = dict(detail['native'])
    if row['venue'] == 'predictfun':
        # Historical endpoint embeds CURRENT market/BBO presentation. These
        # changes do not create new execution content. Keep amounts/fees/legs.
        native['market'] = {'id': native.get('market', {}).get('id')}
        def leg(value):
            value = dict(value)
            if isinstance(value.get('outcome'), dict):
                value['outcome'] = {k: v for k, v in value['outcome'].items()
                    if k not in ('bestBid', 'bestAsk', 'name', 'status', 'team', 'variantData')}
            return value
        native['taker'] = leg(native.get('taker') or {})
        native['makers'] = [leg(v) for v in native.get('makers') or []]
    elif row['venue'] == 'polymarket':
        for key in ('title', 'slug', 'icon', 'event_slug', 'outcome', 'name', 'pseudonym',
                    'bio', 'profile_image', 'profile_image_optimized'):
            native.pop(key, None)
    # The full native body/fees remain in the standard source and context.
    # Do not replicate a large embedded market response into every checkpoint.
    return {k: v for k, v in row.items() if k != 'detail_json'} | dict(
        asset_id=detail['normalized'].get('asset_id'),
        content_key=digest([row['venue'], row['market_id'], native]))


def fits_catalog(event):
    from nautilus_trader.model import Price, Quantity
    values = [(d.order.price, Price) for d in event.deltas if str(d.action) != 'CLEAR'] + [
        (d.order.size, Quantity) for d in event.deltas if str(d.action) != 'CLEAR'] if type(event).__name__ == 'OrderBookDeltas' else [(event.price, Price), (event.size, Quantity)]
    try:
        for value, cls in values:
            number(value.as_decimal(), cls, 16)
        return True
    except ValueError:
        return False


def catalog_number(value, cls):
    number(value, cls, 16)  # Reject any nonzero decimal past scale 16.
    with localcontext() as ctx:
        ctx.prec = 160
        return cls.from_str(format(Decimal(value), '.16f'))


def verify_files(folder, report):
    for f in report['files']:
        p = bound_file(folder, f['file'])
        if p.stat().st_size != f['bytes'] or sha_file(p) != f['sha256']:
            raise ValueError('nautilus_output_changed')


def read_release(folder):
    """Cold reader: native catalog + exact overflow, in saved availability order."""
    from nautilus_trader.persistence import ParquetDataCatalog
    from nautilus_trader.model import OrderBookDeltas, OrderBookDelta, TradeTick, RecordFlag
    folder = Path(folder)
    report = json.loads((folder / 'release.json').read_bytes())
    verify_files(folder, report)
    catalog = ParquetDataCatalog(str(folder / 'catalog'))
    found = defaultdict(list)
    pending = defaultdict(list)
    for d in catalog.query_order_book_deltas():
        key = str(d.instrument_id)
        pending[key].append(d)
        if int(d.flags) & int(RecordFlag.F_LAST):
            obj = OrderBookDeltas(d.instrument_id, pending.pop(key))
            found[fingerprint(obj)].append(obj)
    if pending:
        raise ValueError('catalog_unclosed_book_batch')
    for t in catalog.query_trade_ticks():
        found[fingerprint(t)].append(t)
    spill = folder / 'exact-overflow.parquet'
    if spill.exists():
        for r in pq.ParquetFile(spill, page_checksum_verification=True).read().to_pylist():
            raw = json.loads(r['event_json'])
            obj = OrderBookDeltas(OrderBookDelta.from_dict(raw[0]).instrument_id,
                [OrderBookDelta.from_dict(d) for d in raw]) if r['event_type'] == 'OrderBookDeltas' else TradeTick.from_dict(raw)
            found[fingerprint(obj)].append(obj)
    ordered = []
    for row in pq.ParquetFile(folder / 'event-index.parquet', page_checksum_verification=True).read().to_pylist():
        values = found.get(row['fingerprint'])
        if not values:
            raise ValueError('cold_catalog_event_missing')
        ordered.append(values.pop(0))
    if any(found.values()):
        raise ValueError('cold_catalog_extra_events')
    return ordered


def write_catalog_data(path, catalog, books, trades, workers):
    if workers == 1:
        for values in books.values():
            catalog.write_order_book_deltas(values)
        for values in trades.values():
            catalog.write_trade_ticks(values)
        return

    # Each task owns a distinct instrument and data-type directory. Catalog
    # objects are created inside their worker and never shared across threads.
    from nautilus_trader.persistence import ParquetDataCatalog

    def write_partition(task):
        method, values = task
        getattr(ParquetDataCatalog(str(path)), method)(values)

    tasks = [('write_order_book_deltas', values) for values in books.values()]
    tasks.extend(('write_trade_ticks', values) for values in trades.values())
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for _ in pool.map(write_partition, tasks):
            pass


def export(source, output, *, max_batches=1, trade_mode='observed_content', fail_before_commit=False,
           catalog_workers=1, on_cold_replay=None, prior_release_paths=None):
    """Append immutable per-source-release catalogs, checkpoint idempotently."""
    if not 1 <= catalog_workers <= 16:
        raise ValueError('catalog_workers_out_of_range')
    import nautilus_trader as nt
    from nautilus_trader.persistence import ParquetDataCatalog
    from nautilus_trader.model import (Price, Quantity, TradeTick, TradeId, AggressorSide)
    if not nt.__version__.startswith('2.'):
        raise ValueError('nautilus_2_runtime_required')
    if trade_mode not in ('exclude', 'observed_content') or not 1 <= max_batches <= 100:
        raise ValueError('bounded_export_arguments_required')
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError('independent_output_required')
    output.mkdir(parents=True, exist_ok=True)
    lock = OwnerLock(output / 'writer.lock')
    started = time.perf_counter()
    try:
        source_state = json.loads((source / 'state.json').read_bytes())
        path = output / 'state.json'
        code = sha_file(Path(__file__))
        identity = dict(profile=PROFILE, collection=source_state['collection'], repo=source_state['repo'],
                        converter_sha256=code, trade_mode=trade_mode, runtime=nt.__version__)
        state = json.loads(path.read_bytes()) if path.exists() else dict(identity=identity, commits=[], last=source_state.get('first', 1)-1)
        if state['identity'] != identity:
            raise ValueError('nautilus_profile_changed_use_new_output')
        commits = source_state['commits']
        prior_release_paths = prior_release_paths or {}
        if not prior_release_paths.keys() <= {c['identity'] for c in state['commits']}:
            raise ValueError('unknown_prior_release_reference')
        if len(commits) < len(state['commits']):
            raise ValueError('standard_history_removed')
        for i, old in enumerate(state['commits']):
            if commits[i]['identity'] != old['identity'] or commits[i]['release_sha256'] != old['source_sha256']:
                raise ValueError('standard_history_changed')
            prior_source, folder = prior_release_paths.get(
                old['identity'], (source / 'releases' / old['identity'], output / 'releases' / old['identity']))
            prior_receipt = prior_source / 'release.json'
            if sha_file(prior_receipt) != old['source_sha256']:
                raise ValueError('standard_release_receipt_changed')
            verify_release(prior_source, json.loads(prior_receipt.read_bytes()))
            if sha_file(folder / 'release.json') != old['release_sha256']:
                raise ValueError('nautilus_receipt_changed')
            verify_files(folder, json.loads((folder / 'release.json').read_bytes()))
        checkpoint = dict(definitions={}, payloads={}, emitted={}, levels={}, epochs={}, instruments={})
        if state['commits']:
            previous_id = state['commits'][-1]['identity']
            previous_folder = prior_release_paths.get(previous_id, (None, output / 'releases' / previous_id))[1]
            with gzip.open(previous_folder / 'checkpoint.json.gz', 'rb') as f:
                checkpoint = json.load(f)
            if 'book_available_ns' not in checkpoint:
                clocks = checkpoint.setdefault('book_available_ns', {})
                index = previous_folder / 'event-index.parquet'
                with pq.ParquetFile(index, page_checksum_verification=True) as reader:
                    for batch in reader.iter_batches(columns=['instrument_id', 'ts_init', 'policy']):
                        for item in batch.to_pylist():
                            if item['policy'] in ('baseline_seed', 'snapshot', 'delta', 'uncertainty_clear'):
                                key = item['instrument_id']
                                clocks[key] = max(clocks.get(key, 0), item['ts_init'])
        results = []
        for commit in commits[len(state['commits']):len(state['commits']) + max_batches]:
            src = (source / 'releases' / commit['identity']).resolve()
            if not src.is_relative_to(source / 'releases') or not src.is_dir():
                raise ValueError('bound_standard_release_required')
            receipt = json.loads((src / 'release.json').read_bytes())
            if sha_file(src / 'release.json') != commit['release_sha256']:
                raise ValueError('standard_receipt_changed')
            verify_release(src, receipt)
            if receipt['first'] != state['last'] + 1:
                raise ValueError('standard_prefix_gap')
            final = output / 'releases' / commit['identity']
            if final.exists():
                report = json.loads((final / 'release.json').read_bytes())
                if report['identity'] != identity or report['source_sha256'] != commit['release_sha256']:
                    raise ValueError('adoption_identity_changed')
                verify_files(final, report)
                read_release(final)
                with gzip.open(final / 'checkpoint.json.gz', 'rb') as f:
                    checkpoint = json.load(f)
            else:
                work = output / 'work' / (commit['identity'] + '-' + str(time.time_ns()))
                work.mkdir(parents=True)
                for r in rows(src, 'definitions'):
                    if r['kind'] == 'instrument':
                        checkpoint['definitions'][r['definition_id']] = json.loads(r['definition_json'])
                for r in rows(src, 'trade_payloads'):
                    checkpoint['payloads'][r['payload_id']] = payload_projection(r)
                definitions_by_market = defaultdict(dict)
                for definition_id, definition in checkpoint['definitions'].items():
                    definitions_by_market[definition['venue'], definition['market_id']][definition_id] = definition

                @lru_cache(maxsize=None)
                def asset_for_book(venue, market, source_instrument):
                    return book_asset(
                        {'venue': venue, 'market_id': market, 'instrument': source_instrument},
                        definitions_by_market[venue, market],
                    )

                instruments = {}
                @lru_cache(maxsize=None)
                def instrument_key(venue, market, asset):
                    return native_id(venue, market, asset)

                def instrument(v, m, a, at):
                    key = instrument_key(v, m, a)
                    spec = checkpoint['instruments'].setdefault(key, [v, m, a, at])
                    if key not in instruments:
                        instruments[key] = make_instrument(*spec)
                    return instruments[key]
                events = []
                counts = Counter()
                page_ranks = Counter()
                diagnostics = []
                book_clocks = checkpoint.setdefault('book_available_ns', {})

                def emit_book(event, row, policy):
                    key = str(event.instrument_id)
                    book_clocks[key] = event.ts_init
                    if event.ts_init != row['received_ns']:
                        diagnostics.append(dict(kind='book_availability_delayed_for_sequence',
                                                instrument_id=key, sequence=row['sequence'],
                                                received_ns=row['received_ns'], ts_init=event.ts_init))
                    events.append((event, row, policy))

                agenda = []
                for name in ('book_snapshots', 'book_updates'):
                    for r in rows(src, name):
                        agenda.append((r['sequence'], r['event_index'], name, r))
                for r in rows(src, 'book_baselines'):
                    # Imported state is not a new official snapshot. Its original
                    # clock remains visible; seed only when no prior valid base.
                    r=dict(r, sequence=receipt['first']-1)
                    agenda.append((r['sequence'], r['event_index'], 'baseline', r))
                for r in rows(src, 'observations'):
                    if r['kind'] == 'connection_boundary':
                        agenda.append((r['sequence'], -1, 'boundary', r))
                if trade_mode == 'observed_content':
                    for r in rows(src, 'trade_occurrences'):
                        if r['window_membership'] == 'inside':
                            agenda.append((r['sequence'], r['row_index'], 'trade', r))
                for _, _, name, r in sorted(agenda, key=lambda x: (x[0], x[1], x[2])):
                    if name == 'boundary':
                        detail=json.loads(r['detail_json'])
                        scope={native_id(b['venue'],b['market_id'],b['asset_id']) for b in detail.get('instrument_bindings',[])} if 'instrument_bindings' in detail else None
                        for key in list(checkpoint['epochs']):
                            spec = checkpoint['instruments'][key]
                            if (key in scope if scope is not None else spec[0] == r['venue'] and (r['connection_epoch'] is None or checkpoint['epochs'][key] == r['connection_epoch'])):
                                obj = instrument(*spec)
                                available = book_availability(book_clocks, key, r['received_ns'])
                                emit_book(clear_batch(obj, r['received_ns'], availability_ns=available),
                                          r, 'uncertainty_clear')
                                checkpoint['epochs'].pop(key)
                                checkpoint['levels'].pop(key, None)
                                counts['uncertainty_clears'] += 1
                        continue
                    if name == 'trade':
                        p = checkpoint['payloads'][r['payload_id']]
                        # Native content identity removes metadata-version duplicates,
                        # while same-page multiplicity remains separate ranks.
                        page_key = (r['sequence'], p['content_key'])
                        page_ranks[page_key] += 1
                        tid = digest([p['content_key'], page_ranks[page_key]])
                        if tid in checkpoint['emitted']:
                            counts['repeated_trade_observations'] += 1
                            continue
                        try:
                            if str(p['action']).lower() not in ('buy', 'sell', 'bid', 'ask'):
                                raise ValueError('supply_or_unknown_action_not_trade_tick')
                            px, q = trade_values(p)
                            asset = p['asset_id']
                            if not asset:
                                raise ValueError('trade_asset_unresolved')
                            obj = instrument(p['venue'], p['market_id'], asset, r['received_ns'])
                            side = AggressorSide.NO_AGGRESSOR if p['venue'] == 'myriad' else AggressorSide.BUY if str(p['action']).lower() in ('buy', 'bid') else AggressorSide.SELL
                            event = TradeTick(obj.id, number(px, Price), number(q, Quantity), side,
                                TradeId('md-' + tid[:32]), p['execution_ns'], r['received_ns'])
                            if event.ts_event > event.ts_init:
                                raise ValueError('trade_execution_after_availability')
                            events.append((event, r, 'observed_content_not_unique_fill'))
                            checkpoint['emitted'][tid] = True
                            counts[p['venue'] + ':trade_ticks'] += 1
                        except (ValueError, KeyError) as exc:
                            diagnostics.append(dict(sequence=r['sequence'], kind='trade_not_exported', reason=str(exc), payload_id=r['payload_id']))
                        continue
                    asset = asset_for_book(r['venue'], r['market_id'], r['instrument'])
                    obj = instrument(r['venue'], r['market_id'], asset, r['received_ns'])
                    key = str(obj.id)
                    snapshot = name in ('book_snapshots', 'baseline')
                    if name == 'baseline' and checkpoint['epochs'].get(key)==r['connection_epoch']:
                        continue
                    if not snapshot and (not r['applied'] or checkpoint['epochs'].get(key) != r['connection_epoch']):
                        diagnostics.append(dict(sequence=r['sequence'], kind='delta_not_exported', reason='no_valid_snapshot', event_index=r['event_index']))
                        continue
                    levels = {tuple(x) for x in checkpoint['levels'].get(key, [])}
                    available = book_availability(book_clocks, key, r['received_ns'])
                    event = book_batch(r, obj, snapshot, levels, availability_ns=available)
                    checkpoint['levels'][key] = sorted(levels)
                    checkpoint['epochs'][key] = r['connection_epoch']
                    if event is not None:
                        emit_book(event, r, 'baseline_seed' if name=='baseline' else 'snapshot' if snapshot else 'delta')
                        counts[r['venue'] + ':' + name] += 1
                # ts_init is availability, never replace it with source time to
                # make historical API trades appear available earlier.
                events.sort(key=lambda x: (x[0].ts_init, x[1]['sequence'], x[1].get('event_index', x[1].get('row_index', -1))))
                (work / 'catalog').mkdir()
                catalog = ParquetDataCatalog(str(work / 'catalog'))
                if instruments:
                    catalog.write_instruments(list(instruments.values()))
                native_books, native_trades, overflow, index = defaultdict(list), defaultdict(list), [], []
                for i, (e, r, policy) in enumerate(events):
                    fits = fits_catalog(e)
                    index.append(dict(ordinal=i, fingerprint=fingerprint(e), sequence=r['sequence'],
                        event_index=r.get('event_index'), row_index=r.get('row_index'), event_id=r['event_id'],
                        instrument_id=str(e.instrument_id), ts_event=e.ts_event, ts_init=e.ts_init,
                        policy=policy, storage='catalog' if fits else 'exact_overflow',
                        source_clock_missing=r.get('source_ns') is None if policy in ('snapshot', 'delta') else False))
                    if fits:
                        if type(e).__name__ == 'OrderBookDeltas':
                            # Normalize precision ONLY when exact (trailing zero removal).
                            from nautilus_trader.model import BookOrder, OrderBookDelta
                            for d in e.deltas:
                                order = d.order if str(d.action) == 'CLEAR' else BookOrder(d.order.side,
                                    catalog_number(d.order.price.as_decimal(), Price), catalog_number(d.order.size.as_decimal(), Quantity), 0)
                                native_books[str(e.instrument_id)].append(OrderBookDelta(d.instrument_id, d.action, order, d.flags, 0, d.ts_event, d.ts_init))
                        else:
                            native_trades[str(e.instrument_id)].append(TradeTick(e.instrument_id,
                                catalog_number(e.price.as_decimal(), Price), catalog_number(e.size.as_decimal(), Quantity),
                                e.aggressor_side, e.trade_id, e.ts_event, e.ts_init))
                    else:
                        raw = [d.to_dict() for d in e.deltas] if type(e).__name__ == 'OrderBookDeltas' else e.to_dict()
                        overflow.append(dict(event_type=type(e).__name__, event_json=json.dumps(raw, separators=(',', ':'))))
                write_catalog_data(work / 'catalog', catalog, native_books, native_trades, catalog_workers)
                index_schema = pa.schema([('ordinal', pa.uint64()), ('fingerprint', pa.string()), ('sequence', pa.uint64()),
                    ('event_index', pa.uint32()), ('row_index', pa.uint32()), ('event_id', pa.string()), ('instrument_id', pa.string()),
                    ('ts_event', pa.uint64()), ('ts_init', pa.uint64()), ('policy', pa.string()), ('storage', pa.string()), ('source_clock_missing', pa.bool_())])
                pq.write_table(pa.Table.from_pylist(index, schema=index_schema), work / 'event-index.parquet', compression='zstd', compression_level=9, write_page_checksum=True)
                if overflow:
                    pq.write_table(pa.Table.from_pylist(overflow), work / 'exact-overflow.parquet', compression='zstd', compression_level=9, write_page_checksum=True)
                for name in ('definitions', 'observations', 'quotes', 'trade_payloads', 'book_baselines'):
                    for p in src.glob(name + '/**/*.parquet'):
                        dest = work / 'context' / p.relative_to(src)
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(p, dest)
                durable_file(work / 'checkpoint.json.gz', gzip.compress(encoded(checkpoint), compresslevel=6, mtime=0))
                durable_file(work / 'diagnostics.json', encoded(diagnostics))
                files = []
                for p in sorted(work.rglob('*')):
                    if p.is_file():
                        with p.open('r+b') as f:
                            f.flush(); os.fsync(f.fileno())
                        files.append(dict(file=p.relative_to(work).as_posix(), bytes=p.stat().st_size, sha256=sha_file(p)))
                report = dict(schema=PROFILE, identity=identity, source_sha256=commit['release_sha256'],
                    first=receipt['first'], last=receipt['last'], files=files, counts=dict(counts),
                    events=len(events), exact_overflow_events=len(overflow), diagnostics=len(diagnostics),
                    standalone_native_catalog_complete=not overflow,
                    execution_ready=False, fees_modeled=False, fills_complete=False, source_modified=False,
                    temporal_policy='ts_init=observed_availability; ts_event=source_or_explicit_receive_fallback',
                    trade_policy='execution content excluding mutable market/quote presentation, plus same-page rank; not certified unique fills',
                    source_engine_pin_verified=False, runtime_version=nt.__version__)
                durable_file(work / 'release.json', encoded(report))
                cold = read_release(work)
                if [fingerprint(e) for e in cold] != [fingerprint(e) for e, _, _ in events]:
                    raise ValueError('cold_semantic_roundtrip_changed')
                if on_cold_replay is not None:
                    on_cold_replay(cold)
                final.parent.mkdir(exist_ok=True)
                os.replace(work, final); sync_directory(final.parent)
            if fail_before_commit:
                raise RuntimeError('injected_nautilus_checkpoint_interruption')
            state['commits'].append(dict(identity=commit['identity'], source_sha256=commit['release_sha256'],
                release_sha256=sha_file(final / 'release.json')))
            state['last'] = receipt['last']
            durable_file(path, encoded(state), replace=True)
            results.append(dict(identity=commit['identity'], events=report['events'], counts=report['counts'],
                                exact_overflow_events=report['exact_overflow_events'], diagnostics=report['diagnostics']))
        return dict(schema='nautilus-standard-pass/1', new_batches=len(results), through_sequence=state['last'],
                    releases=results, wall_seconds=time.perf_counter() - started,
                    source_modified=False, upload_performed=False, execution_ready=False)
    finally:
        lock.close()


def iter_events(output):
    """Private replay entry point; never query just catalog/ if overflow exists."""
    output = Path(output)
    state = json.loads((output / 'state.json').read_bytes())
    for commit in state['commits']:
        folder = output / 'releases' / commit['identity']
        if sha_file(folder / 'release.json') != commit['release_sha256']:
            raise ValueError('nautilus_receipt_changed')
        yield from read_release(folder)
