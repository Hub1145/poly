import logging
from typing import List

import pandas as pd

from packages.db.database import get_db

logger = logging.getLogger(__name__)


async def compute_clv(
    market_id: str,
    outcome_id: int,
    entry_price: float,
    entry_time: pd.Timestamp,
    horizons: List[str] = ["1h", "4h", "24h"],
) -> dict:
    """
    Compute Closing Line Value (CLV) for a specific trade.
    CLV = Later Price - Entry Price  (absolute probability change, not percentage).
    """
    db = get_db()
    entry_ts = entry_time.strftime("%Y-%m-%d %H:%M:%S")

    rows = await db.fetchall(
        "SELECT timestamp, mid_price FROM price_snapshots"
        " WHERE market_id=? AND outcome_id=? AND timestamp > ?"
        " ORDER BY timestamp ASC",
        (market_id, outcome_id, entry_ts),
    )

    if not rows:
        return {}

    df = pd.DataFrame([
        {"timestamp": r.timestamp, "mid_price": r.mid_price}
        for r in rows
    ])
    df.set_index("timestamp", inplace=True)

    results = {}
    for horizon in horizons:
        target_time = entry_time + pd.Timedelta(horizon)
        idx = df.index.get_indexer([target_time], method="nearest")[0]
        if idx != -1:
            later_price = df.iloc[idx]["mid_price"]
            results[f"clv_{horizon}"] = float(later_price) - float(entry_price)

    return results


def compute_lateness_penalty(
    entry_price: float, previous_price: float, max_move: float = 0.05
) -> float:
    """Penalty for entering after price has already moved significantly."""
    move = abs(entry_price - previous_price)
    if move > max_move:
        return min(1.0, (move - max_move) / max_move)
    return 0.0
