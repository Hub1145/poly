import logging
from typing import Dict

from packages.db.database import get_db

logger = logging.getLogger(__name__)


async def aggregate_topic_skill(trader_address: str) -> Dict[str, float]:
    """
    Compute average CLV per Gamma tag for a specific trader.
    Used to identify topic specialists (e.g., 'Politics expert').
    """
    db = get_db()
    rows = await db.fetchall(
        """
        SELECT mt.tag, AVG(t.clv_score) AS avg_clv
        FROM trades t
        JOIN markets m  ON t.market_id  = m.id
        JOIN market_tags mt ON m.id = mt.market_id
        WHERE t.trader_address = ?
          AND t.clv_score IS NOT NULL
        GROUP BY mt.tag
        """,
        (trader_address,),
    )
    return {row["tag"]: float(row["avg_clv"]) for row in rows}
