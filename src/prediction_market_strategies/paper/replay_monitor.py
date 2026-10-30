"""Build inspectable Paper runs with the native engine, from saved or synthetic books."""

import argparse
from collections import defaultdict
from dataclasses import asdict
from decimal import Decimal as D
import json
from pathlib import Path
import time
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.adapters.polymarket import PolymarketFeeModel
from nautilus_trader.execution import StaticLatencyModel
from nautilus_trader.model import (
    AccountType,
    BinaryOption,
    BookType,
    Money,
    OmsType,
    OrderBookDeltas,
    RecordFlag,
    Venue,
)
from ..adapters.common import binary_instrument, book_snapshot
from ..strategies.single_market import SingleMarketPaperStrategy, SingleRules
from .monitor import wire
from .polymarket_pair import save_json, summarize, archive_inputs


class ReplayMonitor:
    error = None

    def __init__(self, metadata):
        self.metadata, self.events, self.snapshots = metadata, [], []

    def event(self, row):
        self.events.append(wire(dict(row, seq=len(self.events) + 1)))
        return True

    def snapshot(self, row):
        if len(self.snapshots) >= 10000:
            self.error = "replay_observation_limit"
            return
        self.snapshots.append(
            wire(dict(self.metadata, **row, persisted_event_seq=len(self.events)))
        )

    def save(self, root):
        # This is deliberately after engine.run: fast historical replay must
        # preserve every UI frame without synchronously writing during matching.
        for name, rows in (
            ("monitor-events.jsonl", self.events),
            ("monitor-history.jsonl", self.snapshots),
        ):
            with (root / name).open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        save_json(root / "monitor-state.json", self.snapshots[-1])


def synthetic_inputs():
    now = time.time_ns() // 10**9 * 10**9
    items = []
    for outcome, token in (("Yes", "111"), ("No", "222")):
        item = binary_instrument(
            venue="polymarket",
            market_id="synthetic-ui",
            condition_id="0xsynthetic-ui",
            token=token,
            outcome=outcome,
            observed_ns=now,
            revision="synthetic-ui/1",
            terms={"question": "合成场景 · 单市场买入与卖出"},
            spec=dict(
                currency="USDC",
                currency_precision=6,
                price_precision=2,
                size_precision=2,
                price_increment="0.01",
                size_increment="0.01",
            ),
            extra_info=dict(
                condition_id="0xsynthetic-ui",
                market_version="v1",
                min_order_size="5",
                fee_schedule=dict(
                    rate="0.03", exponent="1", takerOnly=True, rebateRate="0"
                ),
            ),
        )
        data = item.to_dict()
        data["expiration_ns"] = now + 86400 * 10**9
        items.append(BinaryOption.from_dict(data))
    batches = []
    for t in range(160):
        for i, item in enumerate(items):
            bid = D("0.48") if t < 12 else D("0.52") if t < 100 else D("0.51")
            if i:
                bid = 1 - bid - D("0.02")
            ts = now + t * 250_000_000 + i * 10_000_000
            bids = [(str(bid - D(j) * D("0.01")), str(100 + j * 45)) for j in range(6)]
            asks = [
                (str(bid + D(j + 1) * D("0.01")), str(90 + j * 35)) for j in range(6)
            ]
            batches.append(book_snapshot(item, bids, asks, ts_event=ts, ts_init=ts))
    return items, batches


def saved_inputs(source):
    from nautilus_trader.persistence import ParquetDataCatalog

    source = Path(source)
    items = [
        BinaryOption.from_dict(v)
        for v in json.loads(
            (source / "initial-instruments.json").read_text(encoding="utf-8")
        )
    ]
    pending, batches = defaultdict(list), []
    for row in ParquetDataCatalog(
        str((source / "catalog").resolve())
    ).query_order_book_deltas():
        pending[str(row.instrument_id)].append(row)
        if int(row.flags) & int(RecordFlag.F_LAST):
            batches.append(
                OrderBookDeltas(row.instrument_id, pending.pop(str(row.instrument_id)))
            )
    if pending:
        raise ValueError("incomplete source batch")
    return items, sorted(batches, key=lambda b: (b.ts_init, str(b.instrument_id)))


def replay(root, source=None, rules=None, inputs=None):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    items, batches = inputs or (saved_inputs(source) if source else synthetic_inputs())
    rules = rules or SingleRules()
    first = batches[0].ts_init
    monitor = ReplayMonitor(
        dict(
            run_id=root.name,
            mode="PAPER",
            source="saved_real_replay" if source else "synthetic_validation",
            strategy="single-market",
            title=items[0].info.get("question")
            or ("合成场景 · 单市场买入与卖出" if not source else "真实历史盘口回放"),
            slug=str(items[0].id),
            started_ns=first,
            rules=asdict(rules),
            starting_balance="1000",
        )
    )
    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
    engine.add_venue(
        venue=Venue("POLYMARKET"),
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        starting_balances=[Money(D(1000), items[0].currency)],
        book_type=BookType.L2_MBP,
        liquidity_consumption=True,
        trade_execution=False,
        bar_execution=False,
        queue_position=False,
        fee_model=PolymarketFeeModel(),
        latency_model=StaticLatencyModel(
            insert_latency_nanos=100_000_000, cancel_latency_nanos=100_000_000
        ),
    )
    strategy = SingleMarketPaperStrategy([i.id for i in items], rules)
    strategy.monitor = monitor
    try:
        for item in items:
            engine.add_instrument(item)
        engine.add_strategy(strategy)
        engine.add_data(batches)
        engine.run()
        result = summarize(strategy, engine.cache, currency=items[0].currency)
        status = (
            "failed"
            if result["halted"]
            or result["unresolved_orders"]
            or D(result["residual_quantity"])
            else "completed"
        )
        strategy.publish_monitor(
            status,
            "unresolved_or_residual_state" if status == "failed" else None,
            force=True,
        )
        monitor.save(root)
        save_json(root / "events.json", strategy.records)
        save_json(root / "initial-instruments.json", [i.to_dict() for i in items])
        report = dict(
            status=status,
            source=monitor.metadata["source"],
            real_execution_clients=0,
            result=result,
            frames=len(monitor.snapshots),
            source_directory=str(source) if source else None,
        )
        report.update(archive_inputs(root / "catalog", items, batches))
        save_json(root / "report.json", report)
        return report, monitor
    finally:
        engine.dispose()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--source")
    args = p.parse_args()
    r, _ = replay(args.output, args.source)
    print(json.dumps({k: r[k] for k in ("status", "source", "frames")}))


if __name__ == "__main__":
    main()
