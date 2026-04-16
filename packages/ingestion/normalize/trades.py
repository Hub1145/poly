from datetime import datetime
from typing import Any, Dict


def _parse_timestamp(raw: Any) -> datetime:
    """Parse a CLOB timestamp — handles Unix-ms int, Unix-s int, and ISO strings."""
    if raw is None:
        return datetime.utcnow()
    if isinstance(raw, (int, float)):
        ts = float(raw)
        if ts > 32503680000:   # Unix-ms if suspiciously large (> year 3000 in seconds)
            ts /= 1000.0
        return datetime.utcfromtimestamp(ts)
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                pass
        try:
            return _parse_timestamp(float(raw))
        except (ValueError, TypeError):
            pass
    return datetime.utcnow()


def normalize_clob_trade(
    raw_trade: Dict[str, Any], market_id: str, outcome_id: int
) -> Dict[str, Any]:
    """
    Normalize a CLOB trade payload into a plain dict ready for DB insertion.

    Returned keys match the `trades` table columns exactly:
        market_id, outcome_id, trader_address, side, price, size,
        notional, transaction_hash, timestamp (ISO string)
    """
    trader = (
        raw_trade.get("maker_address")
        or raw_trade.get("trader_address")
        or raw_trade.get("proxyWallet")   # data-api.polymarket.com field
        or raw_trade.get("transactor")
        or "unknown"
    )

    side_raw = raw_trade.get("side", "BUY")
    side = side_raw.lower() if isinstance(side_raw, str) else "buy"

    try:
        price = float(raw_trade.get("price", 0.0) or 0.0)
    except (TypeError, ValueError):
        price = 0.0

    try:
        size = float(raw_trade.get("size", 0.0) or 0.0)
    except (TypeError, ValueError):
        size = 0.0

    ts = _parse_timestamp(raw_trade.get("timestamp"))

    return {
        "market_id":        market_id,
        "outcome_id":       outcome_id,
        "trader_address":   trader,
        "side":             side,
        "price":            price,
        "size":             size,
        "notional":         price * size,
        "transaction_hash": raw_trade.get("transaction_hash")
                            or raw_trade.get("transactionHash", ""),
        "timestamp":        ts.strftime("%Y-%m-%d %H:%M:%S"),
    }
