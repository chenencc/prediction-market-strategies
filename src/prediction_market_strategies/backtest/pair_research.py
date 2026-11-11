"""Stream verified Nautilus events into the offline pair-convergence model.

All source events in the selected committed prefix are read, including overflow.
Pairs and fees come from retained original headers; no HF/network reads here.
"""
from bisect import bisect_right
from collections import defaultdict, Counter
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3

from ..data.hf_sync import read, verify_bundle, atomic_json, sha
from ..data.hf_backtest import merge_available
from ..strategies.pair_convergence import Config, Leg, Pair, PairConvergence, NS


class Metadata:
    def __init__(self):
        self.values = defaultdict(dict)
        self.clocks = {}

    def add(self, venue, market, at, raw):
        key = (venue, str(market))
        if at in self.values[key] and self.values[key][at] != raw:
            raise ValueError("conflicting_same_clock_metadata")
        self.values[key][at] = raw

    def finish(self):
        self.clocks = {key: sorted(v) for key, v in self.values.items()}

    def __call__(self, venue, market, at):
        key = (venue, str(market))
        clocks = self.clocks.get(key, [])
        index = bisect_right(clocks, at)-1
        return self.values[key][clocks[index]] if index >= 0 else None


def parse_pairs(header, known_ns):
    from local_postprocess.nautilus import native_id
    markets = {x[0]:x for x in header["markets"]}
    instruments = {x[0]:x for x in header["instruments"]}
    native_predict = {x[1]:x for x in header["instruments"]
                      if markets[x[1]][1] == "predictfun" and x[3] == 1 and x[4] == "native"}
    out, inventory = [], []
    for ident, mids, outcomes, start, end in header["pairs"]:
        if len(mids) != 2 or {markets[m][1] for m in mids} != {"polymarket", "predictfun"}:
            continue
        if len(outcomes) != 2:
            raise ValueError("binary_cross_pair_required")
        oriented = []
        for label, ids in outcomes:
            legs = {}
            for iid in ids:
                inst = instruments[iid]
                _, venue, mid = markets[inst[1]]
                mirror = inst[4] == "derived_complement"
                if mirror:
                    if venue != "predictfun" or inst[3] != 2 or inst[1] not in native_predict:
                        raise ValueError("unsupported_mirror_mapping")
                    base = native_predict[inst[1]]
                else:
                    base = inst
                legs[venue] = Leg(native_id(venue, mid, base[2]), venue, str(mid), mirror)
            if set(legs) != {"polymarket", "predictfun"}:
                raise ValueError("pair_leg_mapping_incomplete")
            oriented.append((label, legs))
        key = header["collection"] + ":" + str(ident)
        start = max(start, known_ns)
        for index in range(2):
            legs = (oriented[index][1]["polymarket"], oriented[1-index][1]["predictfun"])
            out.append(Pair(key+":"+str(index), legs, start, end))
        inventory.append(dict(pair=key, start_ns=start, end_ns=end,
                              labels=[x[0] for x in oriented],
                              outcome_legs=[{v:asdict(l) for v,l in x[1].items()} for x in oriented],
                              rule_equivalence="unverified_mapping_only"))
    return out, inventory


def run(root, output, *, max_chunks=None, through_sequence=None, holds=(300, 900, 3600)):
    import pyarrow.parquet as pq
    from local_postprocess.nautilus import rows, read_release, fingerprint
    from nautilus_trader.model import OrderBook, BookType, Currency, CurrencyType
    Currency.register(Currency("XPROB", 0, 0, "Dimensionless probability replay", CurrencyType.CRYPTO), overwrite=True)
    root, output = Path(root).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    db = sqlite3.connect(f"file:{(root/'index.sqlite').as_posix()}?mode=ro", uri=True)
    selected = db.execute("SELECT identity,first_seq,last_seq,receipt_sha FROM chunks ORDER BY first_seq").fetchall()
    db.close()
    available_count = len(selected)
    if through_sequence is not None:
        selected = [row for row in selected if row[2] <= through_sequence]
        if not selected or selected[-1][2] != through_sequence:
            raise ValueError("requested_sequence_boundary_not_converted")
    if max_chunks is not None:
        if max_chunks < 1:
            raise ValueError("positive_chunk_bound")
        selected = selected[:max_chunks]
    if not selected:
        raise ValueError("no_converted_data")
    metadata, pair_map, memberships, descriptors = Metadata(), {}, {}, []
    baselines, source_pins = set(), []
    previous = None
    missing_at = {}
    for ident, first, last, receipt_sha in selected:
        if previous is not None and first != previous+1:
            raise ValueError("research_source_gap")
        previous = last
        folder = root / "chunks" / ident
        receipt = verify_bundle(folder, receipt_sha)
        source_pins.append(dict(identity=ident, receipt_sha256=receipt_sha, revision=receipt["revision"]))
        header = next(json.loads(r["detail_json"]) for r in rows(folder/"standard", "observations")
                      if r["kind"] == "compact_header")
        with pq.ParquetFile(folder/"nautilus/event-index.parquet") as pf:
            index = pf.read().to_pylist()
        active = [r["ts_init"] for r in index if r["policy"] != "baseline_seed"]
        if not active:
            continue
        known = min(active)
        markets = {r[0]:r for r in header["markets"]}
        for _, mid, at, raw in header["metadata_versions"]:
            metadata.add(markets[mid][1], markets[mid][2], at, raw)
        pairs, inventory = parse_pairs(header, known)
        current = {p.identity for p in pairs}
        for key in pair_map.keys()-current:
            missing_at.setdefault(key, known)
        for pair in pairs:
            old = pair_map.get(pair.identity)
            if old:
                if old.legs != pair.legs:
                    raise ValueError("pair_identity_mapping_changed")
                pair = Pair(pair.identity, pair.legs, min(old.start_ns, pair.start_ns),
                            min(x for x in (old.end_ns, pair.end_ns) if x is not None)
                            if old.end_ns is not None or pair.end_ns is not None else None)
            pair_map[pair.identity] = pair
        for item in inventory:
            memberships.setdefault(item["pair"], item)
        baselines.update(r["fingerprint"] for r in index if r["policy"] == "baseline_seed")
        descriptors.append(dict(folder=folder, first=first, events=len(index), min_ns=min(r["ts_init"] for r in index)))
    for key, end in missing_at.items():
        p = pair_map[key]
        pair_map[key] = Pair(p.identity, p.legs, p.start_ns, min(p.end_ns,end) if p.end_ns else end)
    metadata.finish()
    models = [PairConvergence(Config(hold_seconds=hold), metadata) for hold in holds]
    by_instrument = defaultdict(list)
    for pair in pair_map.values():
        for leg in pair.legs:
            by_instrument[leg.instrument].append(pair)
    native_books, counts = {}, Counter()
    first_ns, last_ns, next_tick = None, None, None
    progress_file = output / "progress.json"
    for event in merge_available(descriptors, lambda d: read_release(d["folder"]/"nautilus"), 1_000_000):
        at = event.ts_init
        first_ns = at if first_ns is None else first_ns
        last_ns = at
        next_tick = at if next_tick is None else next_tick
        # Independent timeout/arrival clock. No future book is applied before a timer.
        while next_tick <= at:
            for model in models:
                model.advance(next_tick)
            next_tick += NS
        for model in models:
            model.advance(at)
        counts[type(event).__name__] += 1
        iid = str(event.instrument_id)
        if type(event).__name__ != "OrderBookDeltas" or iid not in by_instrument:
            continue
        book = native_books.setdefault(iid, OrderBook(event.instrument_id, BookType.L2_MBP))
        book.apply_deltas(event)
        bids, asks = list(book.bids_to_dict().items()), list(book.asks_to_dict().items())
        baseline = fingerprint(event) in baselines
        for model in models:
            model.update(iid, bids, asks, at)
            if not baseline:
                for pair in by_instrument[iid]:
                    model.evaluate(pair, at)
        counts["paired_book_batches"] += 1
        if counts["paired_book_batches"] % 20000 == 0:
            atomic_json(progress_file, dict(status="running", event_counts=dict(counts), through_ns=at))
    results = []
    for model in models:
        report = model.report()
        report["config"] = {k:str(v) if isinstance(v, __import__("decimal").Decimal) else v
                            for k,v in asdict(model.config).items()}
        name = f"hold-{model.config.hold_seconds}s.json"
        atomic_json(output/name, report)
        results.append(dict(hold_seconds=model.config.hold_seconds, report=name,
                            closed_round_trips=report["closed_round_trips"],
                            realized_cash_equivalent_pnl=report["realized_cash_equivalent_pnl"],
                            open_positions=len(report["open_positions"]), stats=report["stats"]))
    atomic_json(output/"pairs.json", list(memberships.values()))
    summary = dict(schema="v3-native-pair-research/1", status="completed_research_not_acceptance",
                   selected_chunks=len(selected), available_chunks_at_start=available_count,
                   source_sequence=[selected[0][1], selected[-1][2]], interval_ns=[first_ns,last_ns],
                   physical_pair_definitions=len(memberships), oriented_pairs=len(pair_map),
                   native_book_instruments=len(native_books), event_counts=dict(counts), results=results,
                   source_pins=source_pins, entire_converted_prefix=max_chunks is None,
                   full_hf_history_verified=False,
                   requested_through_sequence=through_sequence,
                   native_orderbook_replay=True, matching_engine="independent_delayed_IOC_research_model",
                   nautilus_backtest_engine_orders=False, pair_rule_equivalence="not_certified",
                   quality="preserved_source_only_not_reaudited", profitability_certified=False)
    atomic_json(output/"summary.json", summary)
    atomic_json(progress_file, dict(status="complete", through_ns=last_ns, event_counts=dict(counts)))
    return summary
