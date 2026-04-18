import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any

from packages.db.database import get_db
from packages.core.config import settings
from packages.scoring.market_aggregation import aggregate_market_signals
from packages.scoring.strategies.weather_probability import precompute_temperature_bucket_winners

logger = logging.getLogger(__name__)

_STRATEGY_TAG_FILTER = {
    "laddering":          {"Weather"},
    "weather_prediction": {"Weather"},
    "disaster":           {"Natural Disasters"},
    "seismic":            {"Earthquakes", "Natural Disasters"},
    "no_bias":            {"Politics", "Pop Culture", "Entertainment", "Business"},
}

_STRATEGY_KEYWORDS = {
    "laddering":          ["temperature", "heat", "cold", "degrees", "highest", "lowest"],
    "weather_prediction": ["temperature", "heat", "cold", "degrees", "highest", "lowest"],
    "disaster":           ["hurricane", "cyclone", "typhoon", "flood", "wildfire", "blizzard", "avalanche", "landslide", "tsunami"],
    "seismic":            ["earthquake", "magnitude", "seismic", "richter", "tsunami"],
}

# Strategies that are scored purely from trader DB data (no external API calls)
_TRADER_STRATEGIES = frozenset({
    "bayesian_ensemble", "conservative_snw", "aggressive_whale",
    "specialist_precision", "long_range", "volatility",
})
_PRICE_ZONE_STRATEGIES = frozenset({"no_bias", "black_swan"})
_EXTERNAL_STRATEGIES   = frozenset({"laddering", "disaster", "seismic", "weather_prediction"})

# Scoring constants (must match market_aggregation.py)
_COEFF_GLOBAL_SKILL = 0.30
_COEFF_CONVERGENCE  = 0.25
_COEFF_EARLY_ENTRY  = 0.10
_COEFF_CONVICTION   = 0.10

_BASE_WEIGHTS: Dict[str, float] = {
    "topic_specialist":  3.0,
    "serious_non_whale": 2.5,
    "whale":             1.0,
}

_MIN_STRENGTH: Dict[str, float] = {
    "bayesian_ensemble":    1.0,
    "conservative_snw":     1.5,
    "aggressive_whale":     0.8,
    "specialist_precision": 1.0,
    "no_bias":              0.8,
    "black_swan":           0.5,
    "long_range":           1.0,
    "volatility":           0.8,
    "laddering":            0.35,
    "disaster":             0.35,
    "seismic":              0.25,
}

_MIN_TRADERS: Dict[str, int] = {
    "bayesian_ensemble":    2,
    "conservative_snw":     1,
    "aggressive_whale":     1,
    "specialist_precision": 1,
    "long_range":           1,
    "volatility":           1,
}

_STRATEGY_LABEL_FILTERS: Dict[str, tuple] = {
    "bayesian_ensemble":    ("whale", "serious_non_whale", "topic_specialist"),
    "conservative_snw":     ("serious_non_whale",),
    "aggressive_whale":     ("whale",),
    "specialist_precision": ("whale", "serious_non_whale", "topic_specialist"),
    "long_range":           ("whale", "serious_non_whale", "topic_specialist"),
    "volatility":           ("whale",),
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


async def _bulk_score_trader_markets(
    db, markets: list, strategy: str, now: datetime
) -> List[Dict[str, Any]]:
    """
    Score all markets for a trader-based strategy using 1-2 bulk DB queries
    instead of N×queries.  Returns list of signal dicts ready for executemany.
    """
    market_ids = [m.id for m in markets]
    if not market_ids:
        return []

    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # ── long_range: filter to markets with 30+ days remaining ──────────────
    if strategy == "long_range":
        ph = ",".join("?" * len(market_ids))
        end_rows = await db.fetchall(
            f"SELECT id, end_date_iso FROM markets WHERE id IN ({ph})",
            tuple(market_ids),
        )
        valid_ids = set()
        for r in end_rows:
            ed = r["end_date_iso"]
            if ed is not None and (ed - now).days >= 30:
                valid_ids.add(r["id"])
        market_ids = [mid for mid in market_ids if mid in valid_ids]
        markets    = [m for m in markets if m.id in valid_ids]
        if not market_ids:
            return []

    # ── Bulk fetch all skilled trades for all markets in one query ──────────
    label_filters = _STRATEGY_LABEL_FILTERS.get(
        strategy, ("whale", "serious_non_whale", "topic_specialist")
    )
    id_ph    = ",".join("?" * len(market_ids))
    label_ph = ",".join("?" * len(label_filters))

    trade_rows = await db.fetchall(
        f"""
        SELECT t.trader_address, t.side, t.size, t.market_id,
               t.timestamp, tc.label, tp.gamma_score, tp.avg_clv
        FROM trades t
        JOIN trader_classifications tc ON t.trader_address = tc.address
        JOIN trader_profiles        tp ON t.trader_address = tp.address
        WHERE t.market_id IN ({id_ph})
          AND tc.label IN ({label_ph})
        """,
        tuple(market_ids) + label_filters,
    )

    # Group trades by market
    trades_by_market: Dict[str, list] = defaultdict(list)
    for r in trade_rows:
        trades_by_market[r["market_id"]].append(r)

    # ── Score each market in pure Python (no DB calls) ──────────────────────
    signals: List[Dict[str, Any]] = []
    min_strength = _MIN_STRENGTH.get(strategy, 0.5)
    required_traders = _MIN_TRADERS.get(strategy, 1)

    for market in markets:
        mid     = market.id
        results = trades_by_market.get(mid, [])
        if not results:
            continue

        # Strategy-specific label/gamma filtering
        if strategy == "conservative_snw":
            filtered = [r for r in results if r["label"] == "serious_non_whale"]
        elif strategy in ("aggressive_whale", "volatility"):
            filtered = [r for r in results if r["label"] == "whale"]
        elif strategy == "specialist_precision":
            filtered = [r for r in results
                        if (r["gamma_score"] or 0) > 0.6
                        or r["label"] == "topic_specialist"]
        else:
            filtered = results

        if not filtered:
            continue

        # Section-6 formula
        yes_global = no_global = 0.0
        yes_conviction = no_conviction = 0.0
        yes_traders: set = set()
        no_traders:  set = set()

        timestamps = [r["timestamp"] for r in filtered if r["timestamp"]]
        earliest_ts = min(timestamps) if timestamps else now
        time_window  = max((now - earliest_ts).total_seconds(), 1.0)
        early_cutoff = earliest_ts + timedelta(seconds=time_window * 0.20)
        max_size     = max(float(r["size"] or 0.0) for r in filtered) or 1.0

        for r in filtered:
            base_w         = _BASE_WEIGHTS.get(r["label"], 1.0)
            skill          = base_w * (1.0 + float(r["avg_clv"] or 0.0))
            size_norm      = float(r["size"] or 0.0) / max_size
            early_bonus    = 1.0 if (r["timestamp"] and r["timestamp"] <= early_cutoff) else 0.0
            side           = (r["side"] or "").lower()

            if side in ("buy", "yes"):
                yes_global    += skill
                yes_conviction += size_norm + early_bonus * 0.5
                yes_traders.add(r["trader_address"])
            else:
                no_global    += skill
                no_conviction += size_norm + early_bonus * 0.5
                no_traders.add(r["trader_address"])

        yes_score = (_COEFF_GLOBAL_SKILL * yes_global
                     + _COEFF_CONVERGENCE * len(yes_traders)
                     + (_COEFF_EARLY_ENTRY + _COEFF_CONVICTION) * yes_conviction)
        no_score  = (_COEFF_GLOBAL_SKILL * no_global
                     + _COEFF_CONVERGENCE * len(no_traders)
                     + (_COEFF_EARLY_ENTRY + _COEFF_CONVICTION) * no_conviction)

        bias            = "YES" if yes_score >= no_score else "NO"
        strength        = round(abs(yes_score - no_score), 4)
        winning_traders = yes_traders if bias == "YES" else no_traders

        if len(winning_traders) < required_traders or strength < min_strength:
            continue

        # Staleness gate: newest trade > 48 h old → reduce strength 30%
        if timestamps:
            newest_ts   = max(timestamps)
            hours_since = (now - newest_ts).total_seconds() / 3600
            if hours_since > 48:
                strength = round(strength * 0.70, 4)
        if strength < min_strength:
            continue

        signals.append({
            "market_id":        mid,
            "outcome_id":       None,
            "signal_type":      strategy,
            "signal_strength":  strength,
            "directional_bias": bias,
            "explanation": (
                f"[{strategy}] {len(winning_traders)} skilled trader(s) "
                f"converging {bias}. Strength={strength:.3f}"
            ),
            "top_traders":  "[]",
            "created_at":   now_str,
        })

    return signals


async def _bulk_score_price_zone_markets(
    db, markets: list, strategy: str, now: datetime
) -> List[Dict[str, Any]]:
    """
    Score no_bias / black_swan using a single bulk YES-price query.
    """
    market_ids = [m.id for m in markets]
    if not market_ids:
        return []

    ph = ",".join("?" * len(market_ids))
    price_rows = await db.fetchall(
        f"""
        SELECT ps.market_id, ps.mid_price
        FROM price_snapshots ps
        JOIN outcomes o ON ps.outcome_id = o.id
        WHERE ps.market_id IN ({ph})
          AND LOWER(o.name) = 'yes'
          AND ps.rowid IN (
              SELECT MAX(ps2.rowid)
              FROM price_snapshots ps2
              JOIN outcomes o2 ON ps2.outcome_id = o2.id
              WHERE ps2.market_id = ps.market_id
                AND LOWER(o2.name) = 'yes'
          )
        """,
        tuple(market_ids),
    )
    yes_prices = {r["market_id"]: float(r["mid_price"]) for r in price_rows}

    now_str      = now.strftime("%Y-%m-%d %H:%M:%S")
    min_strength = _MIN_STRENGTH.get(strategy, 0.5)
    signals: List[Dict[str, Any]] = []

    for market in markets:
        mid       = market.id
        yes_price = yes_prices.get(mid)
        if yes_price is None:
            continue

        if strategy == "no_bias":
            if not (0.20 <= yes_price <= 0.50):
                continue
            strength = round(1.5 + (0.50 - yes_price) * 2.0, 4)
            if strength < min_strength:
                continue
            signals.append({
                "market_id": mid, "outcome_id": None,
                "signal_type": "no_bias", "signal_strength": strength,
                "directional_bias": "NO",
                "explanation": f"[No-Bias] Retail overbuy zone: YES={yes_price:.3f}",
                "top_traders": "[]", "created_at": now_str,
            })

        elif strategy == "black_swan":
            if not (0.005 <= yes_price <= 0.05):
                continue
            tail_factor = 1.0 + (0.05 - yes_price) / 0.045 * 1.5
            strength    = min(2.5, round(tail_factor, 4))
            if strength < min_strength:
                continue
            signals.append({
                "market_id": mid, "outcome_id": None,
                "signal_type": "black_swan", "signal_strength": strength,
                "directional_bias": "YES",
                "explanation": f"[Black Swan] Deep tail, floor: YES={yes_price:.3f}",
                "top_traders": "[]", "created_at": now_str,
            })

    return signals


async def refresh_market_signals() -> int:
    """Refresh alpha signals for all relevant markets using the current strategy. Returns signal count."""
    db = get_db()
    strategy = settings.strategy
    logger.info(f"[Signals] Refreshing — strategy='{strategy}'")

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
    logger.info(f"[Signals] {len(markets)} markets found for '{strategy}'")

    signals: List[Dict[str, Any]] = []

    # ── Fast bulk path: trader strategies (2 DB queries total) ─────────────
    if strategy in _TRADER_STRATEGIES:
        signals = await _bulk_score_trader_markets(db, markets, strategy, now)

    # ── Fast bulk path: price-zone strategies (1 DB query total) ───────────
    elif strategy in _PRICE_ZONE_STRATEGIES:
        signals = await _bulk_score_price_zone_markets(db, markets, strategy, now)

    # ── External strategies: per-market loop (must hit weather/seismic APIs) ─
    else:
        if strategy in ("weather_prediction", "laddering"):
            await precompute_temperature_bucket_winners(markets)

        errors = 0
        for market in markets:
            market_q = market.question or ""
            try:
                signal = await aggregate_market_signals(
                    db, market.id, strategy=strategy, market_question=market_q
                )
                if signal:
                    signals.append({
                        "market_id":        signal.market_id,
                        "outcome_id":       signal.outcome_id,
                        "signal_type":      signal.signal_type,
                        "signal_strength":  signal.signal_strength,
                        "directional_bias": signal.directional_bias,
                        "explanation":      signal.explanation,
                        "top_traders":      json.dumps(signal.top_traders),
                        "created_at":       signal.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    })
            except Exception as e:
                errors += 1
                logger.warning(f"[Signals] Error on market {market.id}: {e}")

        if errors:
            logger.warning(f"[Signals] {errors} errors during external scoring")

    # ── Bulk insert all signals in one executemany call ─────────────────────
    if signals:
        await db.executemany(
            "INSERT INTO market_signal_snapshots"
            " (market_id, outcome_id, signal_type, signal_strength,"
            "  directional_bias, explanation, top_traders, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    s["market_id"], s["outcome_id"], s["signal_type"],
                    s["signal_strength"], s["directional_bias"],
                    s["explanation"], s["top_traders"], s["created_at"],
                )
                for s in signals
            ],
        )

    await db.commit()
    logger.info(
        f"[Signals] Done — {len(signals)} signals generated "
        f"(strategy='{strategy}', markets={len(markets)})"
    )
    return len(signals)
