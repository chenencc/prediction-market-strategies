"""Bounded public reward-metadata preflight, separate from strategy decisions."""

from datetime import datetime, timezone
import json
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..strategies.depth_maker import D, RewardTerms


def _date_ns(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp()) * 10**9


def from_gamma(market, *, condition_id, market_id, received_ns, source):
    if (
        str(market.get("id")) != str(market_id)
        or market.get("conditionId") != condition_id
    ):
        raise ValueError("reward response market/condition mismatch")
    minimum, distance = (
        D(market.get("rewardsMinSize", "0")),
        D(market.get("rewardsMaxSpread", "0")) / 100,
    )
    if minimum <= 0 or distance <= 0:
        return None
    active = False
    for config in market.get("clobRewards") or []:
        if config.get("conditionId", condition_id) != condition_id:
            raise ValueError("reward allocation condition mismatch")
        if D(config.get("rewardsDailyRate", "0")) > 0 and _date_ns(
            config["startDate"]
        ) <= received_ns < _date_ns(config["endDate"]):
            active = True
    if market.get("closed") is True or market.get("acceptingOrders") is not True:
        active = False
    return RewardTerms(
        "polymarket",
        str(market_id),
        condition_id,
        received_ns,
        minimum,
        distance,
        "midpoint_distance",
        active,
        source,
    )


def fetch(slug, *, condition_id, market_id):
    url = "https://gamma-api.polymarket.com/markets?" + urlencode({"slug": slug})
    started_ns = time.time_ns()
    with urlopen(
        Request(url, headers={"User-Agent": "PMS-local-paper/0.1"}), timeout=15
    ) as response:
        body = response.read(2_000_001)
    received_ns = time.time_ns()
    if len(body) > 2_000_000:
        raise ValueError("reward metadata response cap")
    rows = json.loads(body, parse_float=str)
    if not isinstance(rows, list) or len(rows) != 1:
        raise ValueError("reward query must return exactly one market")
    market = rows[0]
    return from_gamma(
        market,
        condition_id=condition_id,
        market_id=market_id,
        received_ns=received_ns,
        source=url,
    ), dict(
        url=url,
        request_started_ns=started_ns,
        received_ns=received_ns,
        response=market,
        no_authenticated_endpoint=True,
    )
