"""Backtest-facing access to locally synced V3 data; never contacts HF."""
from contextlib import closing
import heapq
from pathlib import Path
import sqlite3

from .hf_sync import iter_events, iter_context, read, verify_bundle, sha


def merge_available(descriptors, load, max_buffered_events):
    """Open another chunk only when its earliest clock overlaps the heap head."""
    descriptors = sorted(descriptors, key=lambda d: (d["min_ns"], d["first"]))
    pending = iter(descriptors)
    next_chunk = next(pending, None)
    heap, active_sizes = [], {}
    last_clock = None
    while heap or next_chunk is not None:
        while next_chunk is not None and (not heap or next_chunk["min_ns"] <= heap[0][0]):
            descriptor = next_chunk
            if sum(active_sizes.values()) + descriptor["events"] > max_buffered_events:
                raise ValueError("overlapping_replay_memory_bound_increase_explicitly")
            events = load(descriptor)
            if len(events) != descriptor["events"] or not events or events[0].ts_init != descriptor["min_ns"]:
                raise ValueError("replay_interval_descriptor_changed")
            if any(a.ts_init > b.ts_init for a, b in zip(events, events[1:])):
                raise ValueError("chunk_not_sorted_by_availability")
            key = descriptor["first"]
            active_sizes[key] = len(events)
            iterator = iter(events)
            event = next(iterator)
            heapq.heappush(heap, (event.ts_init, key, 0, event, iterator))
            events = None
            next_chunk = next(pending, None)
        clock, key, ordinal, event, iterator = heapq.heappop(heap)
        if last_clock is not None and clock < last_clock:
            raise ValueError("cross_chunk_availability_reversed")
        last_clock = clock
        yield event
        event = next(iterator, None)
        if event is None:
            active_sizes.pop(key)
        else:
            heapq.heappush(heap, (event.ts_init, key, ordinal + 1, event, iterator))


class BacktestDataset:
    def __init__(self, root):
        self.root = Path(root).resolve()
        if not (self.root / "index.sqlite").is_file():
            raise ValueError("completed_local_dataset_required")

    def _bundles(self):
        with closing(sqlite3.connect(f"file:{(self.root/'index.sqlite').as_posix()}?mode=ro", uri=True)) as db:
            for identity, digest in db.execute("SELECT identity,receipt_sha FROM chunks ORDER BY first_seq"):
                folder = self.root / "chunks" / identity
                yield folder, verify_bundle(folder, digest)

    def events(self, *, order="availability", max_buffered_events=500000):
        """Native events with exact clocks; default merges cross-chunk overlap.

        Source order remains available for audit. Only overlapping native chunks
        are buffered; the limit fails explicitly instead of silently sampling.
        """
        if order == "source":
            yield from iter_events(self.root)
            return
        if order != "availability":
            raise ValueError("unknown_replay_order")
        import pyarrow.parquet as pq
        from local_postprocess.nautilus import read_release
        descriptors = []
        with closing(sqlite3.connect(f"file:{(self.root/'index.sqlite').as_posix()}?mode=ro", uri=True)) as db:
            previous = None
            for identity, first, last, digest in db.execute("SELECT identity,first_seq,last_seq,receipt_sha FROM chunks ORDER BY first_seq"):
                if previous is not None and first != previous + 1:
                    raise ValueError("local_index_sequence_gap")
                folder = self.root / "chunks" / identity
                if sha(folder / "receipt.json") != digest:
                    raise ValueError("bundle_receipt_changed")
                receipt = read(folder / "receipt.json")
                index = folder / "nautilus/event-index.parquet"
                if sha(index) != receipt["files"]["nautilus/event-index.parquet"]["sha256"]:
                    raise ValueError("event_index_changed")
                with pq.ParquetFile(index) as parquet:
                    if parquet.metadata.num_rows:
                        column = parquet.schema.names.index("ts_init")
                        minimum = min(parquet.metadata.row_group(i).column(column).statistics.min
                                      for i in range(parquet.metadata.num_row_groups))
                        descriptors.append(dict(min_ns=minimum, first=first, folder=folder,
                                                receipt_sha=digest, events=parquet.metadata.num_rows))
                previous = last
        def load(descriptor):
            verify_bundle(descriptor["folder"], descriptor["receipt_sha"])
            return read_release(descriptor["folder"] / "nautilus")
        yield from merge_available(descriptors, load, max_buffered_events)

    def instruments(self):
        """Replay instruments, NOT certified exchange execution specifications."""
        from nautilus_trader.persistence import ParquetDataCatalog
        from nautilus_trader.model import Currency, CurrencyType
        Currency.register(Currency("XPROB", 0, 0, "Dimensionless probability replay", CurrencyType.CRYPTO), overwrite=True)
        seen = set()
        for folder, _ in self._bundles():
            for instrument in ParquetDataCatalog(str(folder / "nautilus/catalog")).instruments():
                if str(instrument.id) not in seen:
                    seen.add(str(instrument.id))
                    yield instrument

    def context(self, table="definitions"):
        """Original as-of metadata/fees; never substitute today's fee schedule."""
        yield from iter_context(self.root, table)

    def quality_certificates(self, *, known_at_ns=None):
        """Preserved upstream results only; absence remains unknown.

        Supply known_at_ns when using quality as a strategy input to avoid using
        a certificate generated after the simulated decision. No recomputation.
        """
        seen = set()
        for folder, _ in self._bundles():
            for path in sorted((folder / "quality").glob("*.json")):
                if path.name in seen:
                    continue
                seen.add(path.name)
                certificate = read(path)
                if known_at_ns is None or certificate["generated_at_ns"] <= known_at_ns:
                    yield certificate

    def status(self):
        with closing(sqlite3.connect(f"file:{(self.root/'index.sqlite').as_posix()}?mode=ro", uri=True)) as db:
            count, first, last, deleted = db.execute(
                "SELECT COUNT(*),MIN(first_seq),MAX(last_seq),COALESCE(SUM(cache_deleted),0) FROM chunks").fetchone()
        return dict(chunks=count, first_sequence=first, through_sequence=last, local_zip_cache_retired=deleted,
                    quality_state="read_per_chunk_receipts_missing_is_unknown", execution_ready=False)
