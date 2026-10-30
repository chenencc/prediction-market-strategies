"""Public Polymarket L2 -> one binary-pair strategy -> local Nautilus Sandbox.

There is intentionally no real execution configuration, credential argument,
server transport or automatic deployment path. Imports do not open connections.
"""

import argparse
import asyncio
from dataclasses import asdict
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import time
from urllib.request import getproxies


def save_json(path, value):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    tmp.replace(path)


def runtime():
    import nautilus_trader

    return dict(
        nautilus_trader=nautilus_trader.__version__,
        python=platform.python_version(),
        platform=platform.platform(),
        desired_engine_source_verified=False,
        runtime_lock="runtime.compatibility.lock.json",
    )


def filesystem_path(path):
    """Use extended Windows paths without changing host registry/settings."""
    path = Path(path).resolve()
    if os.name == "nt" and not str(path).startswith("\\\\?\\"):
        return Path("\\\\?\\" + str(path))
    return path


def artifact_inventory(root):
    # pathlib.rglob on a plain Windows path silently misses directories whose
    # native outcome IDs take the path past MAX_PATH. Rust catalog reads work
    # there, so a successful semantic read is not a physical inventory check.
    scan = filesystem_path(root)
    return {
        p.relative_to(scan).as_posix(): dict(
            bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()
        )
        for p in scan.rglob("*")
        if p.is_file() and p.name not in {"report.json", "report.json.tmp"}
    }


async def public_preflight(factory, *, stage, receipts):
    """Bound only transient public GET failures; never retry an order command."""
    for attempt in range(1, 4):
        try:
            result = await asyncio.wait_for(factory(), timeout=25)
            receipts.append(dict(stage=stage, attempt=attempt, status="ok"))
            return result
        except Exception as exc:
            detail = str(exc).lower()
            permanent = any(
                s in detail
                for s in ("404", "400 bad", "401", "403", "invalid", "mismatch")
            )
            transient = isinstance(exc, TimeoutError) or any(
                s in detail
                for s in (
                    "tls",
                    "handshake",
                    "timeout",
                    "timed out",
                    "connection",
                    "transport",
                    "502",
                    "503",
                    "504",
                    "429",
                )
            )
            receipts.append(
                dict(
                    stage=stage,
                    attempt=attempt,
                    status="failed",
                    error=type(exc).__name__ + ": " + str(exc),
                )
            )
            if permanent or not transient or attempt == 3:
                raise
            await asyncio.sleep(0.2 * attempt)


def archive_inputs(root, instruments, batches):
    """Preserve native inputs and verify physical cold-read event semantics."""
    from collections import defaultdict
    from nautilus_trader.persistence import ParquetDataCatalog
    from nautilus_trader.model import OrderBookDeltas, RecordFlag
    from ..data.nautilus_exporter import event_hash, instrument_hash

    root.mkdir(exist_ok=False)
    catalog = ParquetDataCatalog(str(root))
    catalog.write_instruments(instruments)
    for instrument in instruments:
        values = [b for b in batches if b.instrument_id == instrument.id]
        if values:
            catalog.write_order_book_deltas(
                [d for batch in values for d in batch.deltas]
            )
    reader = ParquetDataCatalog(str(root))
    pending, restored = defaultdict(list), []
    for row in reader.query_order_book_deltas():
        pending[str(row.instrument_id)].append(row)
        if int(row.flags) & int(RecordFlag.F_LAST):
            values = pending.pop(str(row.instrument_id))
            if not all(
                v.ts_init == row.ts_init and v.ts_event == row.ts_event for v in values
            ):
                raise ValueError("catalog changed batch clocks")
            restored.append(OrderBookDeltas(row.instrument_id, values))
    if pending:
        raise ValueError("catalog incomplete batch")
    if sorted(event_hash(b) for b in batches) != sorted(
        event_hash(b) for b in restored
    ):
        raise ValueError("catalog changed book semantics")
    from nautilus_trader.model import Currency
    from ..data.nautilus_exporter import _currency_read_lock

    with _currency_read_lock:
        previous = {}
        try:
            for i in instruments:
                code = str(i.currency)
                if code not in previous:
                    try:
                        previous[code] = Currency.from_str(code)
                    except ValueError:
                        previous[code] = None
                    Currency.register(i.currency, overwrite=True)
            read_instruments = reader.instruments()
        finally:
            for value in previous.values():
                if value is not None:
                    Currency.register(value, overwrite=True)
    if sorted(instrument_hash(i) for i in instruments) != sorted(
        instrument_hash(i) for i in read_instruments
    ):
        raise ValueError("catalog changed instrument semantics")
    files = artifact_inventory(root)
    if len(files) != len(instruments) + len({str(b.instrument_id) for b in batches}):
        raise ValueError("catalog physical file inventory incomplete")
    return dict(
        catalog_readback_batches=len(restored),
        catalog_roundtrip_count_matches=len(batches) == len(restored),
        catalog_roundtrip_semantics_match=True,
        catalog_instrument_semantics_match=True,
        catalog_physical_files=files,
    )


def summarize(strategy, cache, *, currency):
    orders = cache.orders()
    positions = cache.positions()
    account = cache.account_for_venue(strategy.ids[0].venue)
    total = account.balance_total(currency) if account else None
    free = account.balance_free(currency) if account else None
    inventory = {str(i): Decimal(0) for i in strategy.ids}
    fees = Decimal(0)
    for r in strategy.records:
        if r["kind"] == "fill":
            inventory[r["instrument"]] += Decimal(r["quantity"]) * (
                1 if r["side"] == "BUY" else -1
            )
            if r["commission"] is not None:
                fees += Decimal(r["commission"])
    return dict(
        entered=strategy.entered,
        halted=strategy.halted,
        counts=dict(strategy.counts),
        evaluations=len(strategy.samples),
        orders=[o.to_dict() for o in orders],
        positions=[p.to_dict() for p in positions],
        cash_total=str(total.as_decimal()) if total else None,
        cash_free=str(free.as_decimal()) if free else None,
        simulated_commissions=str(fees),
        currency=str(currency),
        simulated_net_inventory={k: str(v) for k, v in inventory.items()},
        residual_quantity=str(
            abs(inventory[str(strategy.ids[0])] - inventory[str(strategy.ids[1])])
        ),
        unresolved_orders=[str(o.client_order_id) for o in orders if not o.is_closed],
        no_settlement_injected=True,
        profitability_verified=False,
    )


async def run(args):
    from nautilus_trader.adapters.polymarket import (
        PolymarketDataLoader,
        PolymarketDataClientConfig,
        PolymarketDataClientFactory,
        PolymarketInstrumentProviderConfig,
        PolymarketFeeModel,
    )
    from nautilus_trader.adapters.sandbox import (
        SandboxExecutionClientConfig,
        SandboxExecutionClientFactory,
    )
    from nautilus_trader.common import Environment, LoggerConfig, LogLevel
    from nautilus_trader.execution import StaticLatencyModel
    from nautilus_trader.live import LiveNode, LiveRiskEngineConfig
    from nautilus_trader.model import (
        AccountId,
        AccountType,
        BookType,
        Money,
        OmsType,
        TraderId,
    )
    from ..strategies.binary_pair_arbitrage import BinaryPairPaperStrategy, PairRules

    is_maker = getattr(args, "strategy", "binary-pair") == "depth-maker"
    is_cycle = getattr(args, "strategy", "binary-pair") == "continuous-market"
    is_single = getattr(args, "strategy", "binary-pair") in {
        "single-market",
        "continuous-market",
    }
    starting_balance = Decimal(str(getattr(args, "starting_balance", "1000")))
    if not starting_balance.is_finite() or starting_balance <= 0:
        raise ValueError("invalid virtual balance")

    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    started_ns = time.time_ns()
    report = dict(
        schema="polymarket-local-depth-maker-paper/1"
        if is_maker
        else "polymarket-local-pair-paper/1",
        status="initializing",
        started_ns=started_ns,
        slug=args.slug,
        local_only=True,
        real_execution_clients=0,
        virtual_starting_balance=str(starting_balance),
        runtime=runtime(),
        limits=dict(
            seconds=args.seconds, one_market=True, max_entries=None if is_cycle else 1
        ),
        simulation=dict(
            decision_delay_ms=250,
            insert_latency_ms=100,
            consume_visible_liquidity=True,
            trade_fills=False,
            bar_fills=False,
            book_type="L2_MBP",
            native_fee_model="PolymarketFeeModel",
            no_atomic_pair_fill=True,
        ),
        source_strategy=dict(
            repository="evan-kolberg/prediction-market-backtesting",
            commit="c76e77af00ef53472a9da8f66dae7fdd2d3e5928",
            license="LGPL-3.0-or-later",
        ),
    )
    if is_single:
        report["schema"] = "polymarket-single-market-paper/1"
        report["source_strategy"] = dict(
            implementation="bounded single-market threshold round trip"
        )
        report["simulation"]["decision_delay_ms"] = 0
    from .monitor import MonitorWriter

    monitor = MonitorWriter(
        root,
        dict(
            run_id=root.name,
            mode="PAPER",
            source="live_public_feed",
            slug=args.slug,
            strategy=getattr(args, "strategy", "binary-pair"),
            simulation=report["simulation"],
            starting_balance=str(starting_balance),
            session_id=getattr(args, "session_id", None),
            session_initial_balance=getattr(args, "session_initial_balance", "1000"),
            prior_realized_pnl=getattr(args, "prior_realized_pnl", "0"),
            prior_fees=getattr(args, "prior_fees", "0"),
            started_ns=started_ns,
        ),
    )
    monitor.snapshot(
        dict(
            status="initializing",
            published_ns=time.time_ns(),
            books=[],
            orders=[],
            events=[],
            fills=[],
            account={},
            phase="initializing",
            reason="fetching_market_metadata",
        )
    )
    from ..strategies import binary_pair_arbitrage
    from ..strategies import binary_market

    source_files = [
        Path(__file__),
        Path(binary_pair_arbitrage.__file__),
        Path(binary_market.__file__),
    ]
    if is_single:
        from ..strategies import single_market

        source_files.append(Path(single_market.__file__))
    if is_cycle:
        source_files.append(Path(__file__).with_name("continuous.py"))
    if is_maker:
        from ..strategies import depth_maker, depth_maker_nautilus
        from . import fees, rewards

        source_files += [
            Path(m.__file__) for m in [depth_maker, depth_maker_nautilus, fees, rewards]
        ]
        report["source_strategy"] = dict(
            repository="ccjingeth/predict-fun-marketmaker",
            commit="9346c7abefaff73758586e6cb8d7193fcbc79afb",
            implementation="independent Python depth-tier policy, not a wholesale bot port",
            upstream_code_executed=False,
        )
        report["limits"] = dict(
            seconds=args.seconds, one_market=True, max_quote_cycles=5, max_orders=30
        )
        report["simulation"].update(
            decision_delay_ms=None,
            min_requote_interval_ms=2000,
            passive_touch_fill_assumed=False,
            rewards_or_rebates_credited=False,
            native_fee_model="PolymarketNoRewardFeeModel",
        )

    report["local_source_sha256"] = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files
    }
    # Rust WebSocket transport does not necessarily inherit Python/system
    # proxies. Reuse local configuration without saving a proxy URL/credentials.
    proxies = getproxies()
    proxy = proxies.get("https") or proxies.get("http")
    report["system_proxy_configured"] = bool(proxy)
    save_json(root / "report.json", report)
    node, task, strategy, cache, handle = None, None, None, None, None
    try:
        instruments = []
        report["metadata_attempts"] = []
        for index in (0, 1):
            loader = await public_preflight(
                lambda: PolymarketDataLoader.from_market_slug(
                    args.slug, token_index=index, timeout_secs=15
                ),
                stage="native_outcome_" + str(index),
                receipts=report["metadata_attempts"],
            )
            instruments.append(loader.instrument)
        allowed = {"yes", "no", "up", "down"} if is_cycle else {"yes", "no"}
        if any(i.outcome.lower() not in allowed for i in instruments):
            raise ValueError("only complementary Yes/No markets supported")
        expected = getattr(args, "expected_condition_id", None)
        if expected and any(
            i.info.get("condition_id") != expected for i in instruments
        ):
            raise ValueError("discovery/native market mismatch")
        currency = instruments[0].currency
        if currency.precision > 16:
            raise ValueError(
                "installed wheel account path unsafe for this collateral precision"
            )
        save_json(root / "initial-instruments.json", [i.to_dict() for i in instruments])
        fee_model, fill_model, reward_terms = PolymarketFeeModel(), None, None
        if is_maker:
            from .fees import PolymarketNoRewardFeeModel, ThroughPriceFillModel
            from .rewards import fetch
            from ..strategies.depth_maker import MakerRules

            info = instruments[0].info
            reward_terms, reward_receipt = await public_preflight(
                lambda: asyncio.to_thread(
                    fetch,
                    args.slug,
                    condition_id=info["condition_id"],
                    market_id=info["market_id"],
                ),
                stage="public_reward_terms",
                receipts=report["metadata_attempts"],
            )
            save_json(root / "reward-preflight.json", reward_receipt)
            report["observed_reward_terms"] = (
                asdict(reward_terms) if reward_terms else None
            )
            rules = MakerRules(
                mode=args.maker_mode,
                quantity=Decimal(args.maker_quantity),
                hedge_mode=args.hedge_mode,
            )
            fee_model, fill_model = (
                PolymarketNoRewardFeeModel(),
                ThroughPriceFillModel(),
            )
        elif is_single:
            from ..strategies.single_market import SingleRules

            rules = SingleRules(
                quantity=Decimal(args.quantity),
                buy_below=Decimal(args.buy_below),
                take_profit=Decimal(args.take_profit),
                stop_loss=Decimal(args.stop_loss),
                hold_ns=args.hold_seconds * 10**9,
                repeat=is_cycle,
                cooldown_ns=getattr(args, "cooldown_seconds", 5) * 10**9,
                entry_cutoff_ns=getattr(args, "entry_cutoff_ns", 0),
                exit_cutoff_ns=getattr(args, "exit_cutoff_ns", 0),
            )
        else:
            rules = PairRules()
        report["rules"] = asdict(rules)
        monitor.metadata.update(
            rules=asdict(rules),
            title=getattr(args, "title", None)
            or instruments[0].info.get("question", args.slug),
            window_end_ns=getattr(args, "window_end_ns", None),
        )
        node = (
            LiveNode.builder(
                "LOCAL-POLY-PAIR", TraderId("PAPER-001"), Environment.SANDBOX
            )
            .with_reconciliation(False)
            .with_timeout_connection(30)
            .with_timeout_portfolio(15)
            .with_timeout_disconnection_secs(10)
            .with_delay_post_stop_secs(1)
            .with_delay_shutdown_secs(1)
            .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
            .with_risk_engine_config(
                LiveRiskEngineConfig(bypass=False, max_order_submit_rate="10/00:00:01")
            )
            .add_data_client(
                "POLYMARKET",
                PolymarketDataClientFactory(),
                PolymarketDataClientConfig(
                    instrument_config=PolymarketInstrumentProviderConfig(
                        market_slugs=[args.slug], use_gamma_markets=True
                    ),
                    http_timeout_secs=15,
                    ws_timeout_secs=15,
                    subscribe_new_markets=False,
                    auto_load_missing_instruments=False,
                    resolve_poll_enabled=False,
                    update_instruments_interval_mins=None,
                    compute_effective_deltas=False,
                    book_stale_check_interval_secs=5,
                    book_stale_threshold_secs=15,
                    proxy_url=proxy,
                ),
            )
            .add_simulated_exec_client(
                "POLYMARKET",
                SandboxExecutionClientFactory(),
                SandboxExecutionClientConfig(
                    venue=instruments[0].id.venue,
                    starting_balances=[Money(starting_balance, currency)],
                    account_id=AccountId("POLYMARKET-SANDBOX-001"),
                    oms_type=OmsType.NETTING,
                    account_type=AccountType.CASH,
                    book_type=BookType.L2_MBP,
                    bar_execution=False,
                    trade_execution=False,
                    liquidity_consumption=True,
                    queue_position=False,
                    fee_model=fee_model,
                    fill_model=fill_model,
                    latency_model=StaticLatencyModel(
                        insert_latency_nanos=100_000_000,
                        cancel_latency_nanos=100_000_000,
                    ),
                ),
            )
            .build()
        )
        if is_maker:
            from ..strategies.depth_maker_nautilus import DepthMakerPaperStrategy

            strategy = DepthMakerPaperStrategy(
                [i.id for i in instruments], rules, reward_terms
            )
        elif is_single:
            from ..strategies.single_market import SingleMarketPaperStrategy

            strategy = SingleMarketPaperStrategy([i.id for i in instruments], rules)
        else:
            strategy = BinaryPairPaperStrategy([i.id for i in instruments], rules)
        strategy.monitor = monitor
        if (
            is_cycle
            and getattr(args, "stop_request_file", None)
            and Path(args.stop_request_file).exists()
        ):
            strategy.exit_only = True
        node.add_strategy(strategy)
        cache, handle = node.cache, node.handle()
        task = asyncio.create_task(node.run_async())
        deadline = time.monotonic() + 50
        while not handle.is_running:
            if task.done():
                await task
                raise RuntimeError("node exited before startup")
            if time.monotonic() > deadline:
                raise TimeoutError("node startup deadline")
            await asyncio.sleep(0.1)
        report.update(status="running", node_running_ns=time.time_ns())
        save_json(root / "report.json", report)
        # Finite wall-clock duration from actual node start. No need to wait
        # indefinitely for a signal or invent one to exercise execution.
        deadline = time.monotonic() + args.seconds
        if getattr(args, "window_end_ns", 0):
            deadline = min(
                deadline,
                time.monotonic()
                + max(0, (args.window_end_ns - time.time_ns()) / 1e9 - 5),
            )
        last_progress = 0
        stop_requested_at = None
        while time.monotonic() < deadline:
            if task.done():
                await task
                raise RuntimeError("node exited during trial")
            if strategy.halted:
                raise RuntimeError("strategy protection halt; see event journal")
            if (
                is_cycle
                and getattr(args, "stop_request_file", None)
                and Path(args.stop_request_file).exists()
            ):
                strategy.exit_only = True
                stop_requested_at = stop_requested_at or time.monotonic()
                if not any(strategy.inventory().values()) and all(
                    o.is_closed for o in cache.orders()
                ):
                    report["user_stop_requested"] = True
                    break
                if time.monotonic() - stop_requested_at > 45:
                    raise RuntimeError(
                        "stop drain timeout; simulated residual retained"
                    )
            if time.monotonic() - last_progress >= 10:
                report.update(
                    observed_ns=time.time_ns(),
                    book_batches=len(strategy.raw_batches),
                    counts=dict(strategy.counts),
                    entered=strategy.entered,
                )
                save_json(root / "report.json", report)
                last_progress = time.monotonic()
            await asyncio.sleep(0.2)
        report["status"] = "completed"
    except Exception as e:
        report.update(status="failed", error=type(e).__name__ + ": " + str(e))
    finally:
        if strategy and hasattr(strategy, "currency"):
            strategy.publish_monitor("stopping", force=True)
        if handle:
            handle.stop()
        if task:
            try:
                await asyncio.wait_for(task, timeout=25)
                report["node_exited_cleanly"] = True
            except Exception as e:
                report.update(
                    status="failed",
                    shutdown_error=type(e).__name__ + ": " + str(e),
                    node_exited_cleanly=False,
                )
        if strategy and cache:
            try:
                report["result"] = summarize(strategy, cache, currency=currency)
                if (
                    report["result"]["cash_total"] is None
                    or report["result"]["cash_free"] is None
                ):
                    report.update(
                        status="failed", error="virtual account was not initialized"
                    )
                if is_maker:
                    report["maker_result"] = dict(
                        quote_cycles=strategy.quote_cycles,
                        submitted_orders=strategy.total_orders,
                        cooldown_until_ns=strategy.protection.cooldown_until_ns,
                        blacklist_until_ns=strategy.protection.blacklist_until_ns,
                        rewards_earned=None,
                        reward_eligibility_not_exchange_verified=True,
                        passive_queue_position_not_verified=True,
                    )
                save_json(root / "events.json", strategy.records)
                save_json(root / "observations.json", strategy.samples)
                save_json(
                    root / "final-instruments.json",
                    [i.to_dict() for i in strategy.instruments.values()],
                )
                # Small bounded native input capture, independently readable.
                report.update(
                    archive_inputs(
                        root / "catalog",
                        list(strategy.instruments.values()),
                        strategy.raw_batches,
                    )
                )
                if not all(strategy.counts[str(i)] > 0 for i in strategy.ids):
                    report.update(status="failed", error="missing real two-leg L2 feed")
                report["book_batches"] = len(strategy.raw_batches)
                if report["result"]["unresolved_orders"] or strategy.halted:
                    report.update(
                        status="failed",
                        error="strategy stopped with unresolved/protected state",
                    )
                if Decimal(report["result"]["residual_quantity"]) != 0:
                    report.update(
                        status="failed", error="unpaired simulated inventory retained"
                    )
            except Exception as e:
                report.update(
                    status="failed", report_error=type(e).__name__ + ": " + str(e)
                )
        if node:
            if strategy and hasattr(strategy, "currency"):
                strategy.publish_monitor(
                    report["status"], report.get("error"), force=True
                )
            node.dispose()
        if not strategy or not hasattr(strategy, "currency"):
            monitor.snapshot(
                dict(
                    status="failed",
                    error=report.get("error"),
                    published_ns=time.time_ns(),
                    books=[],
                    orders=[],
                    events=[],
                    fills=[],
                    account={},
                    phase="failed",
                    reason="initialization_failed",
                )
            )
        try:
            monitor.close()
        except Exception as exc:
            report.update(status="failed", observation_error=str(exc))
        report.update(
            finished_ns=time.time_ns(),
            elapsed_s=(time.time_ns() - started_ns) / 1e9,
            no_real_orders_sent=True,
            no_server_access=True,
        )
        report["artifacts"] = artifact_inventory(root)
        save_json(root / "report.json", report)
    print(
        json.dumps(
            dict(
                status=report["status"],
                report=str(root / "report.json"),
                error=report.get("error"),
                book_batches=report.get("book_batches"),
                result=report.get("result"),
            ),
            default=str,
        )
    )
    return 0 if report["status"] == "completed" else 1


def main(*, default_strategy="binary-pair"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slug", required=True)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--strategy",
        choices=["binary-pair", "depth-maker", "single-market", "continuous-market"],
        default=default_strategy,
    )
    parser.add_argument(
        "--maker-mode", choices=["conservative", "aggressive"], default="conservative"
    )
    parser.add_argument("--maker-quantity", default="10")
    parser.add_argument("--quantity", default="10")
    parser.add_argument("--buy-below", default="0.50")
    parser.add_argument("--take-profit", default="0.02")
    parser.add_argument("--stop-loss", default="0.02")
    parser.add_argument("--hold-seconds", type=int, default=15)
    parser.add_argument(
        "--hedge-mode", choices=["FLATTEN", "BUY_OPPOSITE"], default="FLATTEN"
    )
    args = parser.parse_args()
    if not 30 <= args.seconds <= 600:
        parser.error("finite local trials must last 30..600 seconds")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
