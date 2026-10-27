"""Explicit simulated commissions; daily rebates/rewards are never cash here."""

from decimal import Decimal
from nautilus_trader.execution import FeeModel, FillModel
from nautilus_trader.model import Money


class PolymarketNoRewardFeeModel(FeeModel):
    def __init__(self):
        from nautilus_trader.adapters.polymarket import PolymarketFeeModel

        self.native = PolymarketFeeModel()

    def get_commission(self, order, fill_quantity, fill_px, instrument):
        from ..strategies.binary_market import FeeTerms

        FeeTerms.from_instrument(
            instrument
        )  # explicit observed takerOnly=True required
        if getattr(order, "is_post_only", False):
            return Money(Decimal(0), instrument.currency)
        return self.native.get_commission(order, fill_quantity, fill_px, instrument)


class ThroughPriceFillModel(FillModel):
    """No assumed passive fill from a mere touch or an inside-spread quote."""

    def is_limit_filled(self):
        return False

    def is_slipped(self):
        return False

    def fill_limit_inside_spread(self):
        return False

    def get_orderbook_for_fill_simulation(
        self, instrument, order, best_bid=None, best_ask=None
    ):
        from nautilus_trader.model import OrderBook, BookType

        if getattr(order, "is_post_only", False):
            price = getattr(order, "price", None)
            touch = best_ask if str(order.side) == "BUY" else best_bid
            if price is not None and touch is not None and price == touch:
                # The current native L2 matcher does not consult the touch
                # probability for all passive paths. Return an explicit empty
                # executable book at equality; do not infer queue priority.
                return OrderBook(instrument.id, BookType.L2_MBP)
        return None
