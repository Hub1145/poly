import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from packages.db.database import get_db

logger = logging.getLogger(__name__)


def calculate_pnl(entry_price: float, exit_price: float, size: float, side: str) -> float:
    if side.lower() == "yes":
        return (exit_price - entry_price) * size
    else:
        return (entry_price - exit_price) * size


async def simulate_alpha(
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """Replay historical signals and calculate their precision (profitability)."""
    logger.info(f"Simulating alpha from {start_date} to {end_date}...")
    db = get_db()

    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = end_date.strftime("%Y-%m-%d %H:%M:%S")

    snapshots = await db.fetchall(
        "SELECT * FROM market_signal_snapshots"
        " WHERE created_at >= ? AND created_at <= ?"
        " ORDER BY created_at ASC",
        (start_str, end_str),
    )

    results = []
    for snap in snapshots:
        lookback_limit = (snap.created_at + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        created_str    = snap.created_at.strftime("%Y-%m-%d %H:%M:%S")

        post_trades = await db.fetchall(
            "SELECT price FROM trades"
            " WHERE market_id=? AND outcome_id=?"
            "   AND timestamp > ? AND timestamp <= ?"
            " ORDER BY timestamp ASC",
            (snap.market_id, snap.outcome_id, created_str, lookback_limit),
        )

        if not post_trades:
            continue

        entry_price = float(post_trades[0].price)
        exit_price  = float(post_trades[-1].price)

        pnl = calculate_pnl(entry_price, exit_price, 100, snap.directional_bias)

        results.append({
            "timestamp":  snap.created_at,
            "market_id":  snap.market_id,
            "strength":   snap.signal_strength,
            "bias":       snap.directional_bias,
            "pnl":        pnl,
            "is_correct": pnl > 0,
        })

    return results
