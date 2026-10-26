"""Synthetic native response examples following Predict's published schemas."""

from copy import deepcopy
import json

KNOWN = 1792497600123000000


def market():
    return dict(
        success=True,
        data=dict(
            id=517145,
            title="Synthetic public-policy event",
            decimalPrecision=3,
            feeRateBps=200,
            tradingStatus="OPEN",
            resolution=None,
            outcomes=[
                dict(name="Higher", indexSet=1, onChainId="10001"),
                dict(name="Lower", indexSet=2, onChainId="10002"),
            ],
        ),
    )


def definition():
    return dict(
        schema="predictfun-instrument-definition/1",
        market_id="517145",
        known_at_ns=KNOWN - 1,
        kind="research_model",
        source_ref="synthetic-test-definition",
        fields=dict(
            pricePrecision=12,
            sizePrecision=8,
            sizeIncrement="0.00000001",
            collateralCurrency="USDT",
            collateralDecimals=18,
            chainId=56,
        ),
    )


def book():
    return json.dumps(
        dict(
            success=True,
            data=dict(
                marketId=517145,
                updateTimestampMs=KNOWN // 1_000_000,
                bids=[["0.491", "303518.1"], ["0.49", "1365.44"]],
                asks=[["0.492", "30192.26"], ["0.493", "20003"]],
            ),
        )
    )


def match(*, sid="settlement-test", token="10001", side="Ask"):
    return dict(
        market=dict(id=517145),
        taker=dict(
            quoteType=side,
            outcome=dict(onChainId=token),
            hash="synthetic-taker",
            fee=dict(amount="2500000000000000", type="COLLATERAL"),
        ),
        makers=[
            dict(
                hash="synthetic-maker",
                quoteType="Bid",
                outcome=dict(onChainId=token),
                fee=dict(amount="1000000000000000", type="SHARES"),
            )
        ],
        amountFilled="100000000000000000000",
        priceExecuted="121369800000000000",
        transactionHash="synthetic-transaction",
        settlementId=sid,
        executedAt="2026-10-20T12:00:00.123456789Z",
    )


def page(*rows, cursor=None):
    return dict(success=True, data=deepcopy(list(rows)), cursor=cursor)
