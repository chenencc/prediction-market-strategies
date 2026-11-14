"""V3 compact projection -> standard profile 2. Never reconstruct omitted raw data."""
from collections import Counter
from copy import deepcopy
import gzip
import json
import os
from pathlib import Path
import time
import pyarrow as pa
import pyarrow.parquet as pq
from mixed_recorder.backtest_spool import COLUMNS, FEE_COLUMNS, BookReplay, iter_archive
from mixed_recorder.file_backtest_publication import validate
from mixed_recorder.publication import sha_file
from mixed_recorder.store import durable_file, encoded, sync_directory
from native_archive.fees import canonical, digest
from pair_recorder.supervisor import OwnerLock
from .spool_source import Source
from .standard import Tables, SCHEMAS, verify_release
from .instruments import fee_policy

SCHEMAS_V2 = dict(SCHEMAS, book_baselines=SCHEMAS['book_snapshots'])


class CompactSource(Source):
    def __init__(self, *args, first_sequence, **kwargs):
        super().__init__(*args, **kwargs)
        if type(first_sequence) is not int or first_sequence < 1:
            raise ValueError('explicit_positive_v3_start_required')
        self.first_sequence = first_sequence

    def manifests(self):
        names = [p for p in sorted(self.manifest_paths()) if int(Path(p).stem.split('-')[0]) >= self.first_sequence]
        if len(names) > 1000:
            raise ValueError('v3_discovery_window_bound')
        result = []
        for name in names:
            p = self.download(name)
            if p.stat().st_size > 8 * 1024**2:
                raise ValueError('manifest_size_bound')
            m = json.loads(p.read_bytes()); validate(m, self.prefix)
            if name != self.prefix + '/segments/' + m['identity'] + '.json':
                raise ValueError('manifest_identity_changed')
            result.append(dict(manifest=m, path=name, sha256=sha_file(p)))
        return result

    def records(self, m):
        validate(m, self.prefix)
        f = m['files'][0]
        yield from iter_archive(self.download(f['remote_path'], f), m['archive'])


class CompactTables(Tables):
    def flush(self, key):
        buf = self.buffers[key]
        if not buf:
            return
        if key not in self.writers:
            name, venue, dayhour = key
            folder = self.root / name / ('venue=' + venue) / dayhour
            folder.mkdir(parents=True, exist_ok=True)
            self.writers[key] = pq.ParquetWriter(folder / 'part.parquet', SCHEMAS_V2[name],
                compression='zstd', compression_level=9, write_page_checksum=True)
        self.writers[key].write_table(pa.Table.from_pylist(buf, schema=SCHEMAS_V2[key[0]])); buf.clear()


def asset_alias(venue, mid, asset, index):
    return 'predictfun:' + mid + ':indexSet=1' if venue == 'predictfun' and index == 1 else str(asset)


def baseline_key(venue, alias):
    return venue + ':' + alias


class Convert:
    def __init__(self, collection, manifest, header, state, tables):
        self.collection, self.manifest, self.header, self.state, self.out = collection, manifest, header, state, tables
        if header['collection'] != 'raw-collections/' + collection + '/v3':
            raise ValueError('v3_header_collection_changed')
        self.markets = {x[0]: x for x in header['markets']}
        self.instruments = {x[0]: x for x in header['instruments']}
        self.versions = {x[0]: x for x in header['metadata_versions']}
        self.epoch = lambda e: 'v3:' + collection + ':' + str(e)
        self.stats = Counter(); self.replay = BookReplay(header)
        if len(self.markets) != len(header['markets']) or len(self.instruments) != len(header['instruments']):
            raise ValueError('duplicate_v3_identity')

    def ids(self, market=None, instrument=None):
        i = self.instruments[instrument] if instrument is not None else None
        m = self.markets[i[1] if i else market] if i or market is not None else None
        return m, i

    def common(self, r, ordinal, *, market=None, instrument=None, at=None):
        m, i = self.ids(market if market is not None else r.get('market'), instrument if instrument is not None else r.get('instrument'))
        return dict(event_id=digest([self.collection, self.manifest['identity'], ordinal]), sequence=r.get('sequence'),
            received_ns=at if at is not None else r.get('received_ns', r.get('available_at_ns', 0)),
            venue=m[1] if m else 'shared', market_id=m[2] if m else None,
            connection_epoch=self.epoch(r['epoch']) if r.get('epoch') is not None else None)

    def observation(self, base, kind, detail):
        self.out.put('observations', dict(base, kind=kind, detail_json=canonical(detail)))

    def definition(self, kind, ident, value):
        key = kind + ':' + ident
        if key not in self.state['definitions']:
            self.state['definitions'][key] = True
            self.out.put('definitions', dict(kind=kind, definition_id=ident, definition_json=canonical(value)))

    def metadata(self):
        # Identity mappings may precede any time-qualified financial metadata.
        # Preserve identity without inventing a market version, currency or tick.
        versioned_markets={r[1] for r in self.header['metadata_versions']}
        for i in self.header['instruments']:
            if i[1] in versioned_markets:continue
            m=self.markets[i[1]]
            value=dict(venue=m[1],market_id=m[2],native_asset_id=i[2],index_set=i[3],
                native_outcome_id=None,book_origin=i[4],market_version_id=None,
                price_increment=None,declared_decimal_precision=None,size_increment=None,
                quote_token=None,compact_instrument_id=i[0],identity_only=True)
            self.definition('instrument',digest(value),value)
        for version, market, at, native in self.header['metadata_versions']:
            m = self.markets[market]; mid = m[2]
            ident = digest([self.collection, 'metadata', version])
            self.definition('market', ident, dict(venue=m[1], market_id=mid, native=native,
                available_at_ns=at, compact_version=version, source='v3_compact_financial_projection'))
            policy = fee_policy(m[1], native)
            self.definition('fee_policy', digest(policy), policy)
            for i in self.header['instruments']:
                if i[1] != market:
                    continue
                value = dict(venue=m[1], market_id=mid, native_asset_id=i[2], index_set=i[3],
                    native_outcome_id=None, book_origin=i[4], market_version_id=ident,
                    price_increment=native.get('orderPriceMinTickSize', native.get('tickSize')),
                    declared_decimal_precision=native.get('decimalPrecision'), size_increment=native.get('sizeIncrement'),
                    quote_token=native.get('token'), compact_instrument_id=i[0])
                self.definition('instrument', digest(value), value)
        base = self.common({}, 'header')
        self.observation(base, 'compact_header', self.header)
        # Header assignments are pre-chunk state, not newly observed metadata.
        self.observation(base, 'metadata_baseline', self.header['metadata_asof_start'])

    def book(self, r, ordinal, name, *, baseline=False):
        base = self.common(r, 'baseline:' + str(ordinal) if baseline else ordinal)
        m, i = self.ids(instrument=r['instrument'])
        value = dict(base, event_index=ordinal, instrument=asset_alias(m[1], m[2], i[2], i[3]),
            source_ns=r['source_ns'], source_precision_ns=r['source_precision_ns'], source_hash=None,
            source_cursor_json=canonical(r.get('source_version')) if r.get('source_version') is not None else None,
            origin='chunk_baseline_not_new_snapshot' if baseline else 'v3_compact_projection',
            bids=[dict(price=p, size=q) for p, q in r['bids']], asks=[dict(price=p, size=q) for p, q in r['asks']])
        if name in ('book_snapshots', 'book_baselines'):
            value['boundary'] = 'starting_state_only' if baseline else 'official_snapshot_in_compact_source'
        else:
            value['applied'] = r.get('valid', True)
        self.out.put(name, value)

    def seed(self):
        self.metadata()
        start_books = {}
        for ordinal, (i, epoch, received, source, precision, version, bids, asks) in enumerate(self.header['book_checkpoints']):
            m, inst = self.ids(instrument=i)
            key=baseline_key(m[1], asset_alias(m[1],m[2],inst[2],inst[3]))
            start_books[key]=dict(epoch=self.epoch(epoch),valid=True,
                bids=[dict(price=p,size=q) for p,q in sorted(bids)], asks=[dict(price=p,size=q) for p,q in sorted(asks)])
            previous=self.state['books'].get(key)
            if previous is not None and previous!=start_books[key]:
                raise ValueError('v3_cross_chunk_book_baseline_changed')
            r = dict(instrument=i, epoch=epoch, received_ns=received, source_ns=source, source_precision_ns=precision,
                     source_version=version, bids=bids, asks=asks, sequence=None)
            self.book(r, ordinal, 'book_baselines', baseline=True)
        if any(b['valid'] and k not in start_books for k,b in self.state['books'].items()):
            raise ValueError('v3_valid_book_missing_from_next_header')
        for dependency in self.header.get('fee_trade_definitions', []):
            r = dict(zip(COLUMNS['t'], dependency['record'][1:]))
            self.trade(r, 'fee-dependency:' + str(r['id']), occurrence=False)
            if 'fees_at_start' not in dependency or 'fees_available_at_ns' not in dependency:
                raise ValueError('v3_fee_baseline_asof_required')
            base = self.common(r, 'fee-baseline:' + str(r['id']), at=dependency['fees_available_at_ns'])
            self.observation(base, 'fee_baseline', dict(payload_id=self.payload_id(r['id']),
                fees=[dict(zip(FEE_COLUMNS, x)) for x in dependency['fees_at_start']],
                baseline_only=True, does_not_emit_trade=True))
            self.state.setdefault('fees',{})[self.payload_id(r['id'])]=dict(at=dependency['fees_available_at_ns'],fees=dependency['fees_at_start'])

    def payload_id(self, ident):
        return digest([self.collection, 'v3-trade', ident])

    def membership(self, r):
        stamp = r['execution_ns']
        if stamp is None:
            return 'unknown'
        windows = [p[3:5] for p in self.header['pairs'] if r['market'] in p[1]]
        if any(start <= stamp < (end or 2**63 - 1) for start, end in windows):
            return 'inside'
        return 'outside' if windows else 'unknown'

    def trade(self, r, ordinal, occurrence=True):
        base = self.common(r, ordinal)
        m, i = self.ids(market=r['market'], instrument=r['instrument'])
        pid = self.payload_id(r['id']); known = self.state['trades'].get(pid)
        economics = {k: r[k] for k in ('market', 'instrument', 'execution_ns', 'precision_ns', 'price', 'size', 'price_unit', 'size_unit', 'side', 'rank', 'identified')}
        if known is not None and known != economics:
            raise ValueError('v3_short_trade_id_changed')
        if known is None:
            self.state['trades'][pid] = economics
            self.state.setdefault('fees',{})[pid]=dict(at=r['available_at_ns'],fees=r['fees'])
            fees = [dict(zip(FEE_COLUMNS, x)) for x in r['fees']]
            detail = dict(compact_trade_id=r['id'], compact_collection=self.collection,
                normalized=dict(asset_id=i[2] if i else None, index_set=i[3] if i else None),
                reported_fees=fees, unique_fill_certified=False,
                input_identity_policy=self.header['trade_identity'], native_raw_identity_unavailable=True,
                original_first_available_ns=r['available_at_ns'])
            self.out.put('trade_payloads', dict(payload_id=pid, venue=m[1], market_id=m[2], execution_ns=r['execution_ns'],
                source_precision_ns=r['precision_ns'], price=r['price'], size=r['size'], price_unit=r['price_unit'],
                size_unit=r['size_unit'], action=r['side'], detail_json=canonical(detail)))
        if occurrence:
            self.out.put('trade_occurrences', dict(base, row_index=ordinal, payload_id=pid, page_sha256=None,
                occurrence_rank=r['rank'], window_membership=self.membership(r),
                market_version_id=digest([self.collection, 'metadata', r['metadata_version']])))

    def event(self, record, ordinal):
        tag = record[0]; r = dict(zip(COLUMNS[tag], record[1:])); base = self.common(r, ordinal)
        if r.get('sequence') is not None and not self.manifest['first'] <= r['sequence'] <= self.manifest['last']:
            raise ValueError('v3_record_source_sequence_out_of_chunk')
        self.out.put('source_events', dict(base, event_type='compact:' + tag, transport='compact_projection',
            raw_sha256=digest(record), envelope_json=canonical(dict(schema='market-backtest/2', compact_record=record,
                official_raw_event_envelope=False, record_ordinal=ordinal))))
        self.stats[tag] += 1
        if tag in ('s', 'd', 'u'):
            self.book(r, ordinal, 'book_snapshots' if tag == 's' else 'book_updates')
        elif tag == 't':
            self.trade(r, ordinal)
        elif tag == 'f':
            pid = self.payload_id(r['id'])
            if pid not in self.state['trades']:
                raise ValueError('v3_fee_trade_dependency_missing')
            self.observation(base, 'trade_fee_update', dict(payload_id=pid,
                fees=[dict(zip(FEE_COLUMNS, x)) for x in r['fees']], does_not_emit_trade=True))
            self.state.setdefault('fees',{})[pid]=dict(at=r['available_at_ns'],fees=r['fees'])
        elif tag == 'r':
            bindings = []
            for ident in r['instruments']:
                m, i = self.ids(instrument=ident)
                bindings.append(dict(venue=m[1], market_id=m[2], asset_id=i[2]))
            self.observation(base, 'connection_boundary', dict(instrument_bindings=bindings, compact_record=record))
        elif tag == 'q':
            m, i = self.ids(instrument=r['instrument'])
            self.out.put('quotes', dict(base, event_index=ordinal,
                instrument=asset_alias(m[1], m[2], i[2], i[3]), source_ns=r['source_ns'], best_bid=r['bid'], best_ask=r['ask']))
        else:
            self.observation(base, {'h': 'book_refresh', 'm': 'version_assignment', 'l': 'last_trade_price',
                'k': 'tick_size_change', 'g': 'readiness_evidence', 'a': 'historical_price_anchor',
                'n': 'settlement_notification', 'e': 'lifecycle'}[tag], r)
        self.replay.apply(record)

    def finish(self):
        result = {k:b for k,b in self.state['books'].items() if not b['valid']}
        for ident, b in self.replay.books.items():
            m, i = self.ids(instrument=ident)
            result[baseline_key(m[1], asset_alias(m[1], m[2], i[2], i[3]))] = dict(
                epoch=self.epoch(b['epoch']), valid=b['valid'], bids=[dict(price=p, size=q) for p, q in sorted(b['bids'].items())],
                asks=[dict(price=p, size=q) for p, q in sorted(b['asks'].items())])
        self.state['books'] = result


def run(source, output, *, max_batches=2, fail_before_commit=False):
    if not 1 <= max_batches <= 100:
        raise ValueError('bounded_batch_limit_required')
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    owner = OwnerLock(output / 'writer.lock'); started = time.perf_counter()
    code = digest([sha_file(Path(__file__)), sha_file(Path(__file__).with_name('standard.py')),
        sha_file(Path(__file__).with_name('instruments.py'))])
    try:
        path = output / 'state.json'
        state = json.loads(path.read_bytes()) if path.exists() else dict(schema='market-standard-state/2',
            input_profile='file-spool-publication/3', repo=source.repo, collection=source.collection,
            first=source.first_sequence, last=source.first_sequence - 1, commits=[], converter_sha256=code)
        if (state['schema'], state.get('converter_sha256'), state['repo'], state['collection'], state['first']) != (
                'market-standard-state/2', code, source.repo, source.collection, source.first_sequence):
            raise ValueError('v3_standard_identity_changed_use_new_output')
        manifests = source.manifests(); by_id = {x['manifest']['identity']: x for x in manifests}
        for c in state['commits']:
            if c['identity'] not in by_id or by_id[c['identity']]['sha256'] != c['source_sha256']:
                raise ValueError('source_history_changed')
            folder = output / 'releases' / c['identity']
            if sha_file(folder / 'release.json') != c['release_sha256']:
                raise ValueError('release_receipt_changed')
            verify_release(folder, json.loads((folder / 'release.json').read_bytes()))
        semantic = dict(definitions={}, trades={}, books={})
        if state['commits']:
            with gzip.open(output / 'releases' / state['commits'][-1]['identity'] / 'checkpoint.json.gz', 'rb') as f:
                semantic = json.load(f)
        results = []
        for d in [x for x in manifests if x['manifest']['last'] > state['last']][:max_batches]:
            m = d['manifest']; validate(m, source.prefix)
            if m['first'] != state['last'] + 1:
                raise ValueError('v3_source_prefix_gap')
            final = output / 'releases' / m['identity']
            if final.exists():
                receipt = json.loads((final / 'release.json').read_bytes())
                if receipt['source_sha256'] != d['sha256'] or receipt['converter_sha256'] != code:
                    raise ValueError('v3_recovery_source_changed')
                verify_release(final, receipt)
                with gzip.open(final / 'checkpoint.json.gz', 'rb') as f:
                    semantic = json.load(f)
            else:
                work = output / 'work' / (m['identity'] + '-' + str(time.time_ns())); work.mkdir(parents=True)
                tables = CompactTables(work)
                iterator = source.records(m); header = next(iterator)
                engine = Convert(source.collection, m, header, deepcopy(semantic), tables)
                try:
                    engine.seed()
                    for ordinal, record in enumerate(iterator): engine.event(record, ordinal)
                    engine.finish()
                finally:
                    tables.close()
                semantic = engine.state
                durable_file(work / 'checkpoint.json.gz', gzip.compress(encoded(semantic), compresslevel=6, mtime=0))
                files = []
                for p in sorted(work.rglob('*')):
                    if p.is_file():
                        if p.suffix == '.parquet':
                            with pq.ParquetFile(p, page_checksum_verification=True) as pf:
                                for _ in pf.iter_batches(batch_size=2048): pass
                        with p.open('r+b') as f: f.flush(); os.fsync(f.fileno())
                        files.append(dict(file=p.relative_to(work).as_posix(), bytes=p.stat().st_size, sha256=sha_file(p)))
                receipt = dict(schema='market-standard-release/2', input_profile='file-spool-publication/3',
                    converter_sha256=code, collection=source.collection, first=m['first'], last=m['last'],
                    source_revision=source.revision, source_repo=source.repo, source_manifest=d['path'], source_sha256=d['sha256'],
                    source_files=m['files'], counts=dict(tables.counts), compact_record_counts=dict(engine.stats), files=files,
                    start_scope=state['first'], external_prefix_requires_history=header['external_prefix_requires_history'],
                    raw_official_envelope_available=False, compact_projection_lossless=True,
                    baseline_rows_are_not_new_observations=True, full_trade_completeness_certified=False,
                    server_mutated=False, upload_performed=False)
                durable_file(work / 'release.json', encoded(receipt)); final.parent.mkdir(exist_ok=True)
                os.replace(work, final); sync_directory(final.parent)
            if fail_before_commit: raise RuntimeError('injected_v3_checkpoint_interruption')
            state['commits'].append(dict(identity=m['identity'], last=m['last'], source_sha256=d['sha256'], release_sha256=sha_file(final / 'release.json')))
            state['last'] = m['last']; state['last_revision'] = source.revision
            durable_file(path, encoded(state), replace=True); results.append(receipt)
        return dict(schema='v3-standard-pass/1', new_batches=len(results), through_sequence=state['last'],
            releases=results, wall_seconds=time.perf_counter()-started, source_modified=False, upload_performed=False)
    finally:
        owner.close()
