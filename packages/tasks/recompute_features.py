import asyncio
import logging
import statistics
from datetime import datetime

import pandas as pd

from packages.db.database import get_db
from packages.features.price_relative import compute_clv
from packages.features.behavior import compute_directional_purity
from packages.features.topic_features import aggregate_topic_skill

from packages.tasks.classify_traders import classify_all_traders

logger = logging.getLogger(__name__)


async def refresh_trader_profiles():
    """
    Recompute ALL features for every tracked wallet and update trader_profiles.

    Per-wallet pipeline:
      1. Fetch full trade rows (needed for directional_purity)
      2. Compute CLV (1h) for each trade; write back clv_score via UPDATE
      3. Compute directional_purity
      4. Compute topic-level skill → gamma_score = best tag CLV
      5. Compute win_rate from closed_positions
      6. Bayesian update + shrinkage → composite skill score
      7. Upsert trader_profiles row

    After all profiles are refreshed, re-classify all traders.
    """
    logger.info("Refreshing trader profiles and features...")
    db = get_db()

    address_rows = await db.fetchall("SELECT address FROM trader_wallets")
    addresses = [r["address"] for r in address_rows]

    for address in addresses:
        trades = await db.fetchall(
            "SELECT * FROM trades WHERE trader_address=?"
            " ORDER BY timestamp DESC LIMIT 500",
            (address,),
        )

        if not trades:
            continue

        # ------------------------------------------------------------------ #
        # 2. CLV per trade (1-hour horizon); write back to clv_score column   #
        # ------------------------------------------------------------------ #
        clv_scores     = []
        total_notional = 0.0

        for trade in trades:
            clv_data = await compute_clv(
                trade.market_id,
                trade.outcome_id,
                float(trade.price),
                pd.Timestamp(trade.timestamp),
                horizons=["1h"],
            )
            val = clv_data.get("clv_1h")
            if val is not None:
                clv_scores.append(float(val))
                await db.execute(
                    "UPDATE trades SET clv_score=? WHERE id=?",
                    (float(val), trade.id),
                )
            total_notional += float(trade.notional or 0.0)

        # ------------------------------------------------------------------ #
        # 3. Directional purity                                                #
        # ------------------------------------------------------------------ #
        purity = compute_directional_purity(trades)

        # ------------------------------------------------------------------ #
        # 4. Topic-level skill                                                 #
        # ------------------------------------------------------------------ #
        topic_skills = await aggregate_topic_skill(address)
        gamma_score  = float(max(topic_skills.values())) if topic_skills else 0.0

        # ------------------------------------------------------------------ #
        # 5. Win rate from closed_positions                                    #
        # ------------------------------------------------------------------ #
        pnl_rows = await db.fetchall(
            "SELECT realized_pnl FROM closed_positions WHERE trader_address=?",
            (address,),
        )

        if pnl_rows:
            wins     = sum(1 for r in pnl_rows if (r.realized_pnl or 0) > 0)
            win_rate = wins / len(pnl_rows)
        else:
            win_rate = 0.0

        # ------------------------------------------------------------------ #
        # 6. Bayesian update + shrinkage → composite score                    #
        # ------------------------------------------------------------------ #
        avg_clv    = sum(clv_scores) / len(clv_scores) if clv_scores else 0.0
        median_clv = statistics.median(clv_scores) if clv_scores else 0.0

        # ------------------------------------------------------------------ #
        # 7. Upsert trader_profiles                                            #
        # ------------------------------------------------------------------ #
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        exists = await db.fetchval(
            "SELECT address FROM trader_profiles WHERE address=? LIMIT 1",
            (address,),
        )
        if exists:
            await db.execute(
                """
                UPDATE trader_profiles
                SET total_trades=?, profit_loss=?, avg_clv=?, median_clv=?,
                    directional_purity=?, gamma_score=?, win_rate=?, last_updated=?
                WHERE address=?
                """,
                (
                    len(trades), total_notional, avg_clv, median_clv,
                    purity, gamma_score, win_rate, now,
                    address,
                ),
            )
        else:
            await db.execute(
                """
                INSERT INTO trader_profiles
                (address, total_trades, profit_loss, avg_clv, median_clv,
                 directional_purity, gamma_score, win_rate, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    address, len(trades), total_notional, avg_clv, median_clv,
                    purity, gamma_score, win_rate, now,
                ),
            )

        await db.commit()

    logger.info(f"Refreshed {len(addresses)} trader profiles.")

    await classify_all_traders()
    logger.info("Trader classification complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(refresh_trader_profiles())
