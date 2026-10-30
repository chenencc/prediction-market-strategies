"""Local, paper-only BTC 15-minute observation session with sequential windows."""

import argparse
import asyncio
from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace
from urllib.request import urlopen

from .polymarket_pair import run, save_json


def choose_market(event, start, now):
    """Validate the actual official response; never treat a guessed slug as proof."""
    slug = f"btc-updown-15m-{start}"
    if event.get("slug") != slug or now < start or start + 900 - now < 120:
        return None
    candidates = [m for m in event.get("markets", []) if m.get("slug") == slug]
    if len(candidates) != 1:
        return None
    market = candidates[0]
    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    end = datetime.fromisoformat(market["endDate"].replace("Z", "+00:00")).timestamp()
    if (
        outcomes != ["Up", "Down"]
        or not market.get("active")
        or market.get("closed")
        or market.get("acceptingOrders") is not True
        or market.get("enableOrderBook") is not True
        or abs(end - (start + 900)) > 1
        or not market.get("conditionId")
    ):
        return None
    return dict(
        slug=slug,
        title=market.get("question") or event["title"],
        condition_id=market["conditionId"],
        window_start=start,
        window_end=int(end),
        market_id=market["id"],
        observed_volume=market.get("volume"),
        observed_liquidity=market.get("liquidity"),
        fee_schedule=market.get("feeSchedule"),
    )


def next_balance(report, state, previous_currency=None):
    """Carry money only after flat, acknowledged native shutdown. No reset on loss."""
    result = report.get("result", {})
    if (
        report.get("status") != "completed"
        or not report.get("node_exited_cleanly")
        or result.get("halted")
        or result.get("unresolved_orders")
        or not result.get("simulated_net_inventory")
        or any(Decimal(v) != 0 for v in result["simulated_net_inventory"].values())
        or state.get("account", {}).get("fees") is None
    ):
        raise ValueError("window_not_safely_flat; retain original simulated account")
    currency = result["currency"]
    if previous_currency and currency != previous_currency:
        raise ValueError("collateral_changed; cannot transfer virtual balance")
    value = Decimal(result["cash_total"])
    if not value.is_finite() or value <= 0:
        raise ValueError("virtual balance exhausted")
    return value, currency


class SessionLock:
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.file = self.path.open("a+b")
        self.file.seek(0, 2)
        if not self.file.tell():
            self.file.write(b"0")
            self.file.flush()
        self.file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError("another local BTC paper session already owns this root")
        return self

    def __exit__(self, *args):
        self.file.close()


def supervise(root, session_id):
    root = Path(root).resolve()
    control = root / "_sessions"
    control.mkdir(parents=True, exist_ok=True)
    with SessionLock(control / "btc-paper.lock"):
        directory = control / session_id
        directory.mkdir(exist_ok=False)
        stop = directory / "STOP"
        status_path = directory / "session.json"
        status = dict(
            schema="continuous-btc-paper/1",
            session_id=session_id,
            pid=os.getpid(),
            started_ns=time.time_ns(),
            status="discovering",
            local_only=True,
            real_execution_clients=0,
            starting_balance="1000",
            balance="1000",
            fees="0",
            runs=[],
            stop_request_file=str(stop),
        )
        balance, fees, currency, last_slug, attempt = (
            Decimal(1000),
            Decimal(0),
            None,
            None,
            0,
        )
        child = None
        try:
            while not stop.exists():
                status["observed_ns"] = time.time_ns()
                save_json(status_path, status)
                if shutil.disk_usage(root).free < 2 * 1024**3:
                    raise RuntimeError(
                        "insufficient free disk for retained paper archives"
                    )
                now = time.time()
                start = int(now) // 900 * 900
                slug = f"btc-updown-15m-{start}"
                if slug == last_slug or start + 900 - now < 120:
                    status.update(status="waiting_next_window")
                    time.sleep(2)
                    continue
                try:
                    with urlopen(
                        "https://gamma-api.polymarket.com/events/slug/" + slug,
                        timeout=20,
                    ) as response:
                        event = json.load(response)
                    market = choose_market(event, start, time.time())
                    if not market:
                        raise ValueError("current BTC market not ready")
                except Exception as exc:
                    status.update(
                        status="discovery_retry",
                        last_error=type(exc).__name__ + ": " + str(exc),
                    )
                    save_json(status_path, status)
                    time.sleep(10)
                    continue
                attempt += 1
                run_id = f"{session_id}-{attempt:04d}"
                args = dict(
                    strategy="continuous-market",
                    slug=market["slug"],
                    title=market["title"],
                    seconds=900,
                    output=str(root / run_id),
                    quantity="10",
                    buy_below="0.95",
                    take_profit="0.02",
                    stop_loss="0.02",
                    hold_seconds=10,
                    cooldown_seconds=5,
                    starting_balance=str(balance),
                    session_initial_balance="1000",
                    session_id=session_id,
                    prior_realized_pnl=str(balance - 1000),
                    prior_fees=str(fees),
                    window_end_ns=market["window_end"] * 10**9,
                    entry_cutoff_ns=(market["window_end"] - 60) * 10**9,
                    exit_cutoff_ns=(market["window_end"] - 45) * 10**9,
                    expected_condition_id=market["condition_id"],
                    stop_request_file=str(stop),
                )
                config_path = directory / f"{run_id}-config.json"
                save_json(config_path, args)
                save_json(
                    directory / f"{run_id}-discovery.json",
                    dict(observed_ns=time.time_ns(), market=market, event=event),
                )
                status.pop("last_error", None)
                status.update(status="running", current_run=run_id, market=market)
                with (directory / f"{run_id}.log").open("w", encoding="utf8") as log:
                    child = subprocess.Popen(
                        [
                            sys.executable,
                            "-B",
                            "-X",
                            "utf8",
                            "-m",
                            "prediction_market_strategies.paper.continuous",
                            "--child-config",
                            str(config_path),
                        ],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        creationflags=subprocess.CREATE_NO_WINDOW
                        if os.name == "nt"
                        else 0,
                    )
                    status["child_pid"] = child.pid
                    while child.poll() is None:
                        status["observed_ns"] = time.time_ns()
                        save_json(status_path, status)
                        time.sleep(1)
                report = json.loads(
                    (root / run_id / "report.json").read_text(encoding="utf8")
                )
                state = json.loads(
                    (root / run_id / "monitor-state.json").read_text(encoding="utf8")
                )
                if child.returncode != 0:
                    raise RuntimeError(
                        "paper window failed; original report retained: " + run_id
                    )
                balance, currency = next_balance(report, state, currency)
                fees += Decimal(state["account"]["fees"])
                status["runs"].append(
                    dict(
                        run_id=run_id,
                        balance=str(balance),
                        fees=str(state["account"]["fees"]),
                        completed_ns=time.time_ns(),
                    )
                )
                status.update(
                    balance=str(balance),
                    fees=str(fees),
                    currency=currency,
                    cumulative_pnl=str(balance - 1000),
                    status="waiting_next_window",
                    child_pid=None,
                )
                save_json(status_path, status)
                last_slug = slug
                child = None
            status.update(status="stopped", finished_ns=time.time_ns())
        except KeyboardInterrupt:
            stop.touch(exist_ok=True)
            status.update(status="stop_requested", observed_ns=time.time_ns())
            if child and child.poll() is None:
                child.wait()  # Child drains via STOP; never terminate an account with orders.
        except Exception as exc:
            status.update(
                status="failed",
                error=type(exc).__name__ + ": " + str(exc),
                finished_ns=time.time_ns(),
            )
        finally:
            save_json(status_path, status)
        return 0 if status["status"] == "stopped" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="results")
    parser.add_argument(
        "--session-id", default=time.strftime("btc-paper-%Y%m%d-%H%M%S", time.gmtime())
    )
    parser.add_argument("--child-config")
    parser.add_argument("--stop-session")
    args = parser.parse_args()
    import re

    for value in (args.session_id, args.stop_session):
        if value and not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", value):
            parser.error("invalid session ID")
    if args.child_config:
        config = json.loads(Path(args.child_config).read_text(encoding="utf8"))
        if config.get("strategy") != "continuous-market":
            parser.error("child must remain paper continuous-market")
        return asyncio.run(run(SimpleNamespace(**config)))
    if args.stop_session:
        directory = Path(args.root).resolve() / "_sessions" / args.stop_session
        if not (directory / "session.json").is_file():
            parser.error("session not found")
        (directory / "STOP").touch(exist_ok=True)
        print(
            "Graceful stop requested; waiting for native paper orders and position drain."
        )
        return 0
    return supervise(args.root, args.session_id)


if __name__ == "__main__":
    raise SystemExit(main())
