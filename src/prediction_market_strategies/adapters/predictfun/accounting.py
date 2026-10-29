"""Exact native-unit fee reconciliation, separate from Nautilus cash commissions.

BUY fees reduce received shares. SELL fees reduce collateral proceeds. A cash
equivalent fee is useful for comparisons, but must not replace that inventory
debit. This module makes no API/chain completeness or execution claims.
"""

from decimal import Decimal, localcontext
from ...data.recording_group import require

ONE = 10**18


def estimate_fee_atoms(price_atoms, quantity_atoms, bps, side):
    """CTF fee curve, integer floor per observed aggregate (not each maker leg).

    Per-maker matching/rounding can differ. Results remain estimates until
    compared to the reported fee for the same taker aggregate.
    """
    p, q, rate = int(price_atoms), int(quantity_atoms), int(bps)
    require(0 < p < ONE and q > 0 and 0 <= rate <= 10_000, "INVALID_FEE_INPUT")
    require(side in {"Bid", "Ask"}, "UNKNOWN_PREDICT_SIDE")
    numerator = rate * min(p, ONE - p) * q
    return (
        (numerator // (p * 10_000), "SHARES")
        if side == "Bid"
        else (numerator // (ONE * 10_000), "COLLATERAL")
    )


def recorded_taker_flow(row, fee_legs):
    require(
        row["price_unit"] == row["size_unit"] == "native_wei", "PREDICT_ATOMS_REQUIRED"
    )
    p, q = int(row["price_native"]), int(row["size_native"])
    require(0 <= p <= ONE and q > 0, "INVALID_PREDICT_TRADE")
    taker = [f for f in fee_legs if f["leg"] == "taker"]
    require(len(taker) == 1, "ONE_REPORTED_TAKER_FEE_REQUIRED")
    f = taker[0]
    require(
        f["fee_status"] == "amount_reported"
        and f["amount_is_estimate"] is False
        and f["fee_amount_native"] is not None,
        "REPORTED_FEE_REQUIRED",
    )
    fee = int(f["fee_amount_native"])
    require(fee >= 0, "NEGATIVE_REPORTED_FEE")
    side = row["side"]
    unit = "SHARES" if side == "Bid" else "COLLATERAL" if side == "Ask" else None
    require(unit is not None and f["fee_unit"] == unit, "SIDE_FEE_UNIT_MISMATCH")
    with localcontext() as ctx:
        ctx.prec = 100
        px, size, amount = Decimal(p) / ONE, Decimal(q) / ONE, Decimal(fee) / ONE
        notional = (
            px * size
        )  # exact rational display value; no guessed on-chain rounding
        shares = size - amount if side == "Bid" else -size
        cash = -notional if side == "Bid" else notional - amount
        equivalent = amount * px if unit == "SHARES" else amount
        require(
            size >= amount if unit == "SHARES" else notional >= amount,
            "FEE_EXCEEDS_PROCEEDS",
        )
        # Mark-to-fill value changes by exactly the fee's cash equivalent.
        require(cash + shares * px == -equivalent, "CASH_SHARE_CONSERVATION")
        return dict(
            asset_id=row["asset_id"],
            side=side,
            fee_unit=unit,
            fee_amount=str(amount),
            cash_delta=str(cash),
            share_delta=str(shares),
            cash_equivalent_fee=str(equivalent),
            gross_quantity=str(size),
            unit_scale=18,
            reported_not_chain_verified=True,
            notional_rounding="exact_rational_not_actual_collateral_transfer",
        )


def reconcile_reported_fees(matches, fee_bundles, bps):
    fees = {r["fee_bundle_id"]: r["legs"] for r in fee_bundles}
    results, errors, comparisons = [], [], []
    for entry in matches:
        row = entry["row"]
        try:
            legs = fees[row["fee_bundle_id"]]
            flow = recorded_taker_flow(row, legs)
            flow["identity"] = entry["identity"]
            results.append(flow)
            estimate, unit = estimate_fee_atoms(
                row["price_native"], row["size_native"], bps, row["side"]
            )
            reported = int(
                next(f for f in legs if f["leg"] == "taker")["fee_amount_native"]
            )
            comparisons.append(
                dict(
                    identity=entry["identity"],
                    fee_unit=unit,
                    estimated_atoms=str(estimate),
                    reported_atoms=str(reported),
                    delta_atoms=str(reported - estimate),
                )
            )
        except (ValueError, KeyError, StopIteration) as e:
            errors.append(dict(identity=entry["identity"], reason=str(e)))
    return dict(
        flows=results,
        errors=errors,
        conservation_passed=len(results),
        formula_comparisons=comparisons,
        formula_exact_matches=sum(r["delta_atoms"] == "0" for r in comparisons),
        hypothetical_fee_curve_certified=False,
        engine_cash_commission_substitution_allowed=False,
        why="Nautilus Money commission alone cannot represent BUY share inventory fees",
    )
