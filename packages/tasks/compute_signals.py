import json
import logging
from datetime import datetime, timedelta

from packages.db.database import get_db
from packages.core.config import settings
from packages.scoring.market_aggregation import aggregate_market_signals
from packages.scoring.strategies.weather_probability import precompute_temperature_bucket_winners

logger = logging.getLogger(__name__)

_STRATEGY_TAG_FILTER = {
    "laddering":          {"Weather"},
    "weather_prediction": {"Weather"},
    "disaster":           {"Natural Disasters", "Weather"},
    "seismic":            {"Earthquakes", "Natural Disasters"},
    "no_bias":            {"Politics", "Pop Culture", "Entertainment", "Business"},
}

_STRATEGY_KEYWORDS = {
    "laddering":          ["temperature", "heat", "cold", "degrees", "highest", "lowest"],
    "weather_prediction": ["temperature", "heat", "cold", "degrees", "highest", "lowest"],
    "disaster":           ["hurricane", "flood", "storm", "warning", "watch", "emergency"],
    "seismic":            ["earthquake", "magnitude", "seismic", "richter", "tsunami"],
}


async def _get_markets_for_strategy(strategy: str) -> list:
    """Return markets to scan for a given strategy."""
    db = get_db()
    now_utc = datetime.utcnow()
    grace   = timedelta(hours=12)

    tag_labels = _STRATEGY_TAG_FILTER.get(strategy, set())
    keywords   = _STRATEGY_KEYWORDS.get(strategy, [])

    base_sql = (
        "SELECT * FROM markets"
        " WHERE active=1 AND closed=0"
        " AND (LOWER(market_type) IN ('binary','') OR market_type IS NULL)"
    )

    if not tag_labels and not keywords:
        markets = await db.fetchall(base_sql)
    else:
        market_ids: set = set()

        if tag_labels:
            placeholders = ",".join("?" * len(tag_labels))
            tag_ids = await db.fetchall(
                f"""
                SELECT DISTINCT m.id FROM markets m
                JOIN market_tags mt ON mt.market_id = m.id
                WHERE m.active=1 AND m.closed=0
                  AND (LOWER(m.market_type) IN ('binary','') OR m.market_type IS NULL)
                  AND LOWER(mt.tag) IN ({placeholders})
                """,
                tuple(t.lower() for t in tag_labels),
            )
            market_ids.update(r["id"] for r in tag_ids)

        if keywords:
            kw_clauses = " OR ".join(
                "LOWER(question) LIKE ?" for _ in keywords
            )
            kw_ids = await db.fetchall(
                f"""
                SELECT id FROM markets
                WHERE active=1 AND closed=0
                  AND (LOWER(market_type) IN ('binary','') OR market_type IS NULL)
                  AND ({kw_clauses})
                """,
                tuple(f"%{kw}%" for kw in keywords),
            )
            market_ids.update(r["id"] for r in kw_ids)

        if not market_ids:
            return []

        placeholders = ",".join("?" * len(market_ids))
        markets = await db.fetchall(
            f"SELECT * FROM markets WHERE id IN ({placeholders})",
            tuple(market_ids),
        )

    return [
        m for m in markets
        if m.end_date_iso is None
        or m.end_date_iso + grace > now_utc
    ]


async def refresh_market_signals() -> int:
    """Refresh alpha signals for all relevant markets using the current strategy. Returns signal count."""
    db = get_db()
    logger.info("Refreshing market signals...")
    strategy = settings.strategy
    logger.info(f"Using Strategy Mode: {strategy}")

    now = datetime.utcnow()

    # Prune old signals (keep 6 hours)
    signal_cutoff = (now - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
    await db.execute(
        "DELETE FROM market_signal_snapshots WHERE created_at < ?",
        (signal_cutoff,),
    )

    # Prune stale price snapshots (keep 3 days)
    price_cutoff = (now - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    await db.execute(
        "DELETE FROM price_snapshots WHERE timestamp < ?",
        (price_cutoff,),
    )

    markets = await _get_markets_for_strategy(strategy)
    logger.info(f"[Signals] Found {len(markets)} markets for strategy '{strategy}'")

    # For temperature strategies: precompute which bucket wins per city+date so
    # that compute_ensemble_weather_alpha can force NO on competing buckets.
    if strategy in ("weather_prediction", "laddering"):
        await precompute_temperature_bucket_winners(markets)

    new_signals = 0
    skipped     = 0
    errors      = 0

    for market in markets:
        market_q = market.question or ""
        try:
            signal = await aggregate_market_signals(
                db, market.id, strategy=strategy, market_question=market_q
            )

            if signal:
                await db.execute(
                    "INSERT INTO market_signal_snapshots"
                    " (market_id, outcome_id, signal_type, signal_strength,"
                    "  directional_bias, explanation, top_traders, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        signal.market_id, signal.outcome_id, signal.signal_type,
                        signal.signal_strength, signal.directional_bias,
                        signal.explanation,
                        json.dumps(signal.top_traders),
                        signal.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
                new_signals += 1
                logger.debug(
                    f"[Signals] + {signal.directional_bias} signal "
                    f"(strength={signal.signal_strength:.3f}) -> {market_q[:60]}"
                )
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            logger.warning(f"[Signals] Error on market {market.id}: {e}")

    logger.info(
        f"[Signals] Done — {new_signals} generated, {skipped} below threshold, "
        f"{errors} errors (strategy='{strategy}', markets={len(markets)})"
    )
    await db.commit()
    return new_signals
