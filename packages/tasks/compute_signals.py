import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from packages.db.database import get_db
from packages.core.config import settings
from packages.scoring.market_aggregation import aggregate_market_signals
from packages.scoring.strategies.weather_probability import (
    precompute_temperature_bucket_winners,
    precompute_precipitation_bucket_winners,
)

logger = logging.getLogger(__name__)

_STRATEGY_TAG_FILTER = {
    "laddering":          {"Weather"},
    "weather_prediction": {"Weather"},
    "disaster":           {"Natural Disasters"},
    "no_bias":            {"Politics", "Pop Culture", "Entertainment", "Business"},
}

_STRATEGY_KEYWORDS = {
    # Temperature keywords for city-level daily forecast markets.
    # "highest temperature" / "daily high" / "degrees" / degree-symbol forms all
    # match standard Polymarket temperature bucket phrasing.
    "laddering":          ["highest temperature", "daily high temperature",
                           "degrees fahrenheit", "degrees celsius",
                           "daily high", "°f", "°c",
                           "precipitation", "rainfall", "inches of rain", "mm of rain"],
    "weather_prediction": ["highest temperature", "daily high temperature",
                           "degrees fahrenheit", "degrees celsius",
                           "daily high", "°f", "°c",
                           "precipitation", "rainfall", "inches of rain", "mm of rain"],
    # Disaster + Seismic (merged): broad natural-event terms covering weather disasters
    # and geological events.  Sports-team names (Hurricanes, Avalanche, Earthquakes, etc.)
    # are stripped by _STRATEGY_EXCLUDE_PATTERNS below.
    "disaster":           ["hurricane", "tornado", "tropical storm", "cyclone", "typhoon",
                           "flood", "wildfire", "blizzard", "landslide", "tsunami",
                           "volcanic eruption", "eruption", "named storm", "natural disaster",
                           "drought", "heatwave", "heat wave",
                           # seismic terms (merged)
                           "earthquake of magnitude", "earthquakes of magnitude",
                           "magnitude 6", "magnitude 7", "magnitude 8", "magnitude 9",
                           "seismic", "richter", "aftershock"],
}

# SQL LIKE exclusion patterns applied AFTER the keyword match.
# Strips out sports/entertainment contexts that share vocabulary with weather/seismic.
_STRATEGY_EXCLUDE_PATTERNS: Dict[str, list] = {
    # Disaster (incl. seismic): strip sports teams and non-event contexts
    "disaster": ["%nhl%", "%nba%", "%nfl%", "%mls%", "%mlb%",
                 "% stanley cup%", "%playoffs%", "%championship%",
                 "%solar storm%", "%geomagnetic%",
                 "%hurricanes win%", "%hurricanes lose%", "%hurricanes beat%",
                 "%avalanche win%", "%avalanche lose%", "%flood of%",
                 "%earthquakes win%", "%earthquakes lose%", "%earthquakes beat%"],
}

# Strategies that use skilled-trader data (whale / SNW / topic-specialist trades)
_TRADER_STRATEGIES = frozenset({
    "bayesian_ensemble", "conservative_snw", "aggressive_whale", "specialist_precision",
})
# Price-structure strategies — no trader data, no external API
_PRICE_ZONE_STRATEGIES = frozenset({"no_bias", "black_swan", "long_range", "volatility"})
# External-data strategies — weather / seismic APIs
_EXTERNAL_STRATEGIES   = frozenset({"laddering", "disaster", "weather_prediction"})

# Scoring constants for the trader bulk scorer
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
    # Trader strategies
    "bayesian_ensemble":    1.0,
    "conservative_snw":     1.5,
    "aggressive_whale":     0.8,
    "specialist_precision": 1.0,
    # Price-structure strategies
    "no_bias":              0.8,
    "black_swan":           0.5,
    "long_range":           0.4,
    "volatility":           0.3,
    # External strategies
    "laddering":            0.35,
    "disaster":             0.35,
}

_MIN_TRADERS: Dict[str, int] = {
    "bayesian_ensemble":    2,
    "conservative_snw":     1,
    "aggressive_whale":     1,
    "specialist_precision": 1,
}

_STRATEGY_LABEL_FILTERS: Dict[str, tuple] = {
    "bayesian_ensemble":    ("whale", "serious_non_whale", "topic_specialist"),
    "conservative_snw":     ("serious_non_whale",),
    "aggressive_whale":     ("whale",),
    "specialist_precision": ("whale", "serious_non_whale", "topic_specialist"),
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

        if keywords:
            # Keywords are more precise than Polymarket's noisy tagging.
            # When keywords are defined, use keywords ONLY — tags are used only
            # as a fallback when no keyword list exists for the strategy.
            # (Reason: Polymarket applies the "Weather" tag to volcanoes/hurricanes,
            # "Natural Disasters" to earthquakes, etc., making tag-only matches
            # return hundreds of irrelevant markets.)
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
        elif tag_labels:
            # Tag-only path: used for strategies that have no keyword list.
            # For no_bias, the tag filter scopes to politics/pop-culture topics.
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

        if not market_ids:
            return []

        placeholders = ",".join("?" * len(market_ids))

        # Apply exclusion patterns (e.g. strip sports teams matching weather/seismic keywords)
        exclude_patterns = _STRATEGY_EXCLUDE_PATTERNS.get(strategy, [])
        if exclude_patterns:
            excl_clauses = " AND ".join(
                "LOWER(question) NOT LIKE ?" for _ in exclude_patterns
            )
            markets = await db.fetchall(
                f"SELECT * FROM markets WHERE id IN ({placeholders}) AND {excl_clauses}",
                tuple(market_ids) + tuple(exclude_patterns),
            )
        else:
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

    # Bulk fetch YES and NO outcome_ids for all markets (needed for signal INSERT)
    outcome_rows = await db.fetchall(
        f"""
        SELECT market_id, id, LOWER(name) AS name
        FROM outcomes
        WHERE market_id IN ({id_ph}) AND LOWER(name) IN ('yes', 'no')
        """,
        tuple(market_ids),
    )
    yes_oids: Dict[str, int] = {}
    no_oids:  Dict[str, int] = {}
    for r in outcome_rows:
        if r["name"] == "yes":
            yes_oids[r["market_id"]] = r["id"]
        else:
            no_oids[r["market_id"]] = r["id"]

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

        if strategy == "long_range":
            yes_score = (_COEFF_GLOBAL_SKILL * yes_global
                         + 0.45 * yes_conviction
                         + 0.35 * len(yes_traders)
                         + _COEFF_EARLY_ENTRY * yes_conviction)
            no_score  = (_COEFF_GLOBAL_SKILL * no_global
                         + 0.45 * no_conviction
                         + 0.35 * len(no_traders)
                         + _COEFF_EARLY_ENTRY * no_conviction)
        else:
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

        outcome_id = yes_oids.get(mid) if bias == "YES" else no_oids.get(mid)
        if outcome_id is None:
            continue  # no outcome row — cannot record a tradeable signal

        signals.append({
            "market_id":        mid,
            "outcome_id":       outcome_id,
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
    Score price-structure strategies (no_bias, black_swan, long_range, volatility)
    using bulk price queries only — no skilled-trader data.
    """
    market_ids = [m.id for m in markets]
    if not market_ids:
        return []

    ph = ",".join("?" * len(market_ids))

    # Bulk fetch outcome_ids for both YES and NO sides
    oid_rows = await db.fetchall(
        f"""
        SELECT market_id, id, LOWER(name) AS name
        FROM outcomes
        WHERE market_id IN ({ph}) AND LOWER(name) IN ('yes', 'no')
        """,
        tuple(market_ids),
    )
    _yes_oids: Dict[str, int] = {}
    _no_oids:  Dict[str, int] = {}
    for r in oid_rows:
        if r["name"] == "yes":
            _yes_oids[r["market_id"]] = r["id"]
        else:
            _no_oids[r["market_id"]] = r["id"]

    # Bulk fetch latest YES price AND bid/ask spread for each market
    price_rows = await db.fetchall(
        f"""
        SELECT ps.market_id, ps.mid_price, ps.best_bid, ps.best_ask
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
    price_data: Dict[str, dict] = {
        r["market_id"]: {
            "mid":  float(r["mid_price"]),
            "bid":  float(r["best_bid"]),
            "ask":  float(r["best_ask"]),
        }
        for r in price_rows
    }

    # For long_range: pre-filter to markets with 30+ days remaining
    long_range_valid: set = set()
    if strategy == "long_range":
        end_rows = await db.fetchall(
            f"SELECT id, end_date_iso, question FROM markets WHERE id IN ({ph})",
            tuple(market_ids),
        )
        for r in end_rows:
            ed_raw = r["end_date_iso"]
            ed: Optional[datetime] = None
            if ed_raw is not None:
                if isinstance(ed_raw, datetime):
                    ed = ed_raw
                else:
                    try:
                        ed = datetime.fromisoformat(str(ed_raw).split(".")[0])
                    except ValueError:
                        ed = None
            if ed is None:
                # Fall back to parsing date from question text
                from packages.scoring.market_aggregation import _parse_end_date_from_question
                mq = r["question"] or ""
                if mq:
                    ed = _parse_end_date_from_question(mq)
            if ed is not None and (ed - now).days >= 30:
                long_range_valid.add(r["id"])

    now_str      = now.strftime("%Y-%m-%d %H:%M:%S")
    min_strength = _MIN_STRENGTH.get(strategy, 0.5)
    signals: List[Dict[str, Any]] = []

    for market in markets:
        mid   = market.id
        pdata = price_data.get(mid)
        if pdata is None:
            continue
        yes_price = pdata["mid"]

        # ── no_bias: bet NO against retail YES overpricing ────────────────
        if strategy == "no_bias":
            if not (0.20 <= yes_price <= 0.50):
                continue
            strength   = round(1.5 + (0.50 - yes_price) * 2.0, 4)
            bias       = "NO"
            outcome_id = _no_oids.get(mid)
            reason     = f"[No-Bias] Retail YES overpricing: YES={yes_price:.3f}"

        # ── black_swan: deep tail YES floor ──────────────────────────────
        elif strategy == "black_swan":
            if not (0.005 <= yes_price <= 0.05):
                continue
            strength   = min(2.5, round(1.0 + (0.05 - yes_price) / 0.045 * 1.5, 4))
            bias       = "YES"
            outcome_id = _yes_oids.get(mid)
            reason     = f"[Black Swan] Deep tail YES floor: {yes_price:.3f}"

        # ── long_range: price-structure edge in 30+ day markets ──────────
        elif strategy == "long_range":
            if mid not in long_range_valid:
                continue
            if 0.15 <= yes_price <= 0.42:
                # Underpriced long-dated uncertainty → YES
                strength   = round((0.42 - yes_price) / 0.27 * 1.2 + 0.3, 4)
                bias       = "YES"
                outcome_id = _yes_oids.get(mid)
                reason     = f"[Long-Range] Underpriced YES on 30d+ market: {yes_price:.3f}"
            elif 0.58 <= yes_price <= 0.85:
                # Overpriced long-dated event → NO
                strength   = round((yes_price - 0.58) / 0.27 * 1.2 + 0.3, 4)
                bias       = "NO"
                outcome_id = _no_oids.get(mid)
                reason     = f"[Long-Range] Overpriced YES on 30d+ market: {yes_price:.3f}"
            else:
                continue

        # ── volatility: wide bid-ask spread = high market disagreement ───
        elif strategy == "volatility":
            spread = pdata["ask"] - pdata["bid"]
            if spread < 0.06 or not (0.25 <= yes_price <= 0.75):
                continue
            # Trade toward cheaper side of the spread
            if yes_price <= 0.50:
                bias       = "YES"
                outcome_id = _yes_oids.get(mid)
            else:
                bias       = "NO"
                outcome_id = _no_oids.get(mid)
            strength = round(spread * 6.0, 4)   # 0.06 spread→0.36, 0.20 spread→1.2
            reason   = f"[Volatility] Wide spread {spread:.3f} — {bias} at {yes_price:.3f}"

        else:
            continue

        if strength < min_strength or outcome_id is None:
            continue

        signals.append({
            "market_id":        mid,
            "outcome_id":       outcome_id,
            "signal_type":      strategy,
            "signal_strength":  strength,
            "directional_bias": bias,
            "explanation":      reason,
            "top_traders":      "[]",
            "created_at":       now_str,
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
            # Precompute winners for both temperature and precipitation bucket groups
            # before the scoring loop so mutual-exclusivity logic has the data it needs.
            await precompute_temperature_bucket_winners(markets)
            await precompute_precipitation_bucket_winners(markets)

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
