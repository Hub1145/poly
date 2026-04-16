import asyncio
import logging

from packages.db.database import get_db
from packages.services.trader_service import TraderService
from packages.ingestion.clients.polymarket_http import clob_creds_available

logger = logging.getLogger(__name__)


async def refresh_trades():
    """
    Refresh trade data using three complementary paths:

    1. Public global feed (data-api.polymarket.com) — no auth required.
       Fetches the latest 500 trades globally, matched to known DB outcomes.

    2. Public per-market feed — no auth required.
       Fetches up to 100 trades per asset for the top 200 active market
       outcomes in the DB.  This is the main source of per-market trader
       histories that drive classification and signal generation.

    3. Authenticated per-market CLOB sync — only when API credentials available.
       Provides L2 order-level data unavailable from the public feed.
    """
    logger.info("Refreshing trades...")
    service = TraderService()
    db = get_db()

    # Path 1: Public global feed (always runs, no credentials needed)
    new_global = await service.sync_global_trade_feed()
    logger.info(f"Global feed ingested {new_global} new trades.")

    # Path 2: Public per-market feed — fetch trades for each active outcome.
    # Prioritise outcomes of recently-scored markets (most likely to have signals).
    outcome_rows = await db.fetchall(
        """
        SELECT o.asset_id
        FROM outcomes o
        JOIN markets m ON m.id = o.market_id
        WHERE o.asset_id IS NOT NULL
          AND m.active = 1
          AND m.closed = 0
        ORDER BY m.id DESC
        LIMIT 400
        """
    )
    asset_ids = [r["asset_id"] for r in outcome_rows if r["asset_id"]]
    if asset_ids:
        logger.info(f"[PerMarketFeed] Fetching trades for {len(asset_ids)} outcomes...")
        new_per_market = await service.sync_per_market_trade_feed(
            asset_ids, limit_per_market=100
        )
        logger.info(f"[PerMarketFeed] Stored {new_per_market} new trades.")
    else:
        logger.info("[PerMarketFeed] No active outcomes in DB — skipping.")

    # Path 3: Authenticated per-market CLOB sync (requires credentials).
    has_creds = clob_creds_available()
    if has_creds:
        rows = await db.fetchall(
            "SELECT id FROM markets WHERE active=1 AND closed=0 LIMIT 200"
        )
        for row in rows:
            logger.debug(f"Authenticated sync for market: {row['id']}")
            await service.sync_trades_for_market(row["id"])
    else:
        logger.info("Skipping CLOB per-market sync — no API credentials configured.")

    await service.cleanup_ghost_positions()
    await service.close()
    logger.info("Trade refresh complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(refresh_trades())
