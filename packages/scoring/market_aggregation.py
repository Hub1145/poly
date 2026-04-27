import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional

from packages.db.database import DB
from packages.scoring.strategies.weather_probability import (
    compute_weather_alpha, compute_ensemble_weather_alpha, compute_ladder_weather_alpha,
    compute_precipitation_alpha,
)
from packages.explanation.engine import generate_signal_explanation
from packages.ingestion.clients.polymarket_http import ClobClient
from packages.core.config import settings

logger = logging.getLogger(__name__)

# ── End-date inference helper ─────────────────────────────────────────────────

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_end_date_from_question(question: str) -> Optional[datetime]:
    """
    Try to extract an end/resolution date from the question text.
    Handles ISO dates, 'Month Day Year', 'by Month Day', 'end of Year' patterns.
    Returns None if no date can be found.
    """
    # ISO: 2026-04-30
    m = re.search(r"\d{4}-\d{2}-\d{2}", question)
    if m:
        try:
            return datetime.strptime(m.group(0), "%Y-%m-%d")
        except ValueError:
            pass

    # "Month Day, Year" or "Month Day Year"
    m = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        question, re.IGNORECASE,
    )
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%B %d %Y")
        except ValueError:
            pass

    # "by Month Day" (no year — pick nearest future)
    m = re.search(
        r"by\s+(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2})",
        question, re.IGNORECASE,
    )
    if m:
        month = _MONTH_MAP.get(m.group(1).lower())
        day = int(m.group(2))
        if month:
            today = datetime.utcnow()
            for yr in (today.year, today.year + 1):
                try:
                    candidate = datetime(yr, month, day)
                    if candidate > today:
                        return candidate
                except ValueError:
                    pass

    # "end of 2026" / "in 2026"
    m = re.search(r"\b(20\d{2})\b", question)
    if m:
        try:
            return datetime(int(m.group(1)), 12, 31)
        except ValueError:
            pass

    return None


# ── Research-derived weights (Section 6 signal formula) ───────────────────────
COEFF_GLOBAL_SKILL = 0.30
COEFF_TOPIC_SKILL  = 0.25
COEFF_CONVERGENCE  = 0.25
COEFF_EARLY_ENTRY  = 0.10
COEFF_CONVICTION   = 0.10

BASE_WEIGHTS = {
    "topic_specialist":  3.0,
    "serious_non_whale": 2.5,
    "whale":             1.0,
}

MIN_STRENGTH = {
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
    "weather_prediction":   0.10,
}

_EXTERNAL_DATA_STRATEGIES = {"laddering", "disaster", "weather_prediction"}
_TRADER_SIGNAL_STRATEGIES = {
    "bayesian_ensemble", "conservative_snw", "aggressive_whale",
    "specialist_precision", "long_range", "volatility",
}


@dataclass
class SignalResult:
    """Plain data object returned by aggregate_market_signals."""
    market_id:        str
    outcome_id:       int
    signal_type:      str
    signal_strength:  float
    directional_bias: str
    explanation:      str
    top_traders:      List[Dict[str, Any]] = field(default_factory=list)
    created_at:       datetime = field(default_factory=datetime.utcnow)


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _get_yes_outcome_id(db: DB, market_id: str) -> Optional[int]:
    return await db.fetchval(
        "SELECT id FROM outcomes WHERE market_id=? AND LOWER(name)='yes' LIMIT 1",
        (market_id,),
    )


async def _get_yes_price(
    db: DB, market_id: str, cutoff_time: Optional[datetime] = None
) -> Optional[float]:
    if cutoff_time:
        return await db.fetchval(
            """
            SELECT ps.mid_price
            FROM price_snapshots ps
            JOIN outcomes o ON ps.outcome_id = o.id
            WHERE ps.market_id=? AND LOWER(o.name)='yes'
              AND ps.timestamp <= ?
            ORDER BY ps.timestamp DESC
            LIMIT 1
            """,
            (market_id, cutoff_time.strftime("%Y-%m-%d %H:%M:%S")),
        )
    return await db.fetchval(
        """
        SELECT ps.mid_price
        FROM price_snapshots ps
        JOIN outcomes o ON ps.outcome_id = o.id
        WHERE ps.market_id=? AND LOWER(o.name)='yes'
        ORDER BY ps.timestamp DESC
        LIMIT 1
        """,
        (market_id,),
    )


async def _build_external_signal(
    db: DB,
    market_id: str,
    market_q: str,
    strategy: str,
    cutoff_time: Optional[datetime] = None,
) -> Optional[SignalResult]:
    now_utc    = cutoff_time or datetime.utcnow()
    live_price = await _get_yes_price(db, market_id, cutoff_time=cutoff_time)
    yes_price  = live_price if live_price is not None else 0.5
    outcome_id = await _get_yes_outcome_id(db, market_id)
    if outcome_id is None:
        return None

    edge: float = 0.0
    ext_narrative: str = ""
    source_label: str  = ""

    # weather_prediction — temperature: 40-member ensemble empirical probability
    if edge == 0.0 and strategy == "weather_prediction":
        _wx = await compute_ensemble_weather_alpha(market_q, yes_price)
        if isinstance(_wx, tuple):
            edge, ext_narrative = _wx
        source_label = "weather_prediction"

    # weather_prediction — precipitation: monthly accumulation vs bucket threshold
    if edge == 0.0 and strategy == "weather_prediction":
        _wx = await compute_precipitation_alpha(market_q, yes_price)
        if isinstance(_wx, tuple):
            edge, ext_narrative = _wx
        source_label = "weather_prediction"

    # laddering — temperature: ensemble scorer with adjacent YES bucket signals
    if edge == 0.0 and strategy == "laddering":
        _wx = await compute_ladder_weather_alpha(market_q, yes_price)
        if isinstance(_wx, tuple):
            edge, ext_narrative = _wx
        source_label = "weather_laddering"

    # laddering — precipitation: same monthly accumulation scorer
    if edge == 0.0 and strategy == "laddering":
        _wx = await compute_precipitation_alpha(market_q, yes_price)
        if isinstance(_wx, tuple):
            edge, ext_narrative = _wx
        source_label = "weather_laddering"

    # disaster (merged with seismic) — routes to seismic or weather scorer by content
    if edge == 0.0 and strategy == "disaster":
        _seismic_kw = ("earthquake", "seismic", "magnitude", "richter", "aftershock",
                       "volcanic eruption", "eruption")
        _temp_kw    = ("temperature", "degrees", "celsius", "fahrenheit",
                       "heat index", "feels like", "high of", "low of",
                       "record high", "record low")
        q_low = market_q.lower()

        if any(kw in q_low for kw in _seismic_kw):
            # ── Seismic sub-path: USGS NSHM base-rate + live catalog Omori boost ──
            _REGION_RATES = {
                "japan":         0.30, "indonesia":  0.25, "turkey":      0.22,
                "chile":         0.20, "california": 0.18, "new zealand": 0.17,
                "greece":        0.15, "san francisco": 0.14, "los angeles": 0.12,
                "nepal":         0.12, "iran":       0.10, "philippines": 0.10,
                "peru":          0.09, "alaska":     0.08,
            }
            _REGION_BBOX = {
                "japan":         (30.0, 45.0, 130.0, 145.0),
                "indonesia":     (-10.0, 5.0, 95.0, 140.0),
                "turkey":        (36.0, 42.0, 26.0, 44.0),
                "chile":         (-40.0, -20.0, -75.0, -65.0),
                "california":    (32.0, 42.0, -124.0, -114.0),
                "san francisco": (36.5, 38.5, -123.5, -121.0),
                "los angeles":   (33.5, 34.5, -119.0, -117.5),
                "nepal":         (26.0, 30.0, 80.0, 88.0),
            }
            region    = next((r for r in _REGION_RATES if r in q_low), None)
            base_prob = _REGION_RATES.get(region, 0.04)
            source_note = "USGS NSHM base-rate"
            bbox = _REGION_BBOX.get(region) if region else None
            if bbox:
                try:
                    from datetime import datetime as _dt, timedelta as _td
                    import httpx as _hx
                    _end   = _dt.utcnow()
                    _start = _end - _td(days=30)
                    _params = {
                        "format": "geojson", "starttime": _start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "endtime": _end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "minmagnitude": "4.0", "minlatitude": str(bbox[0]),
                        "maxlatitude": str(bbox[1]), "minlongitude": str(bbox[2]),
                        "maxlongitude": str(bbox[3]), "orderby": "magnitude", "limit": "5",
                    }
                    async with _hx.AsyncClient(timeout=8.0) as _cl:
                        _r = await _cl.get(
                            "https://earthquake.usgs.gov/fdsnws/event/1/query", params=_params
                        )
                        if _r.status_code == 200:
                            _evts = _r.json().get("features", [])
                            if _evts:
                                _mx = max(f["properties"].get("mag", 0) or 0 for f in _evts)
                                _boost = min(0.30, (_mx - 4.0) * 0.15)
                                base_prob = min(0.95, base_prob + _boost)
                                source_note = (
                                    f"USGS catalog: M{_mx:.1f} in last 30 days "
                                    f"(Omori +{_boost*100:.0f}%) + NSHM base"
                                )
                            else:
                                source_note = "USGS catalog: no M4+ in last 30 days + NSHM base"
                except Exception as _se:
                    logger.debug(f"[Seismic] USGS catalog fetch failed: {_se}")
            edge = round(base_prob - yes_price, 4)
            ext_narrative = (
                f"[Seismic] Region '{region or 'global'}' | {source_note} "
                f"{base_prob*100:.1f}% vs market {yes_price*100:.1f}%. Edge: {edge:+.3f}."
            )
            source_label = "seismic"

        elif not any(kw in q_low for kw in _temp_kw):
            # ── Weather/disaster sub-path: Open-Meteo wind/precipitation ──
            _wx = await compute_weather_alpha(market_q, "YES", yes_price)
            if isinstance(_wx, tuple):
                edge, ext_narrative = _wx
            source_label = "weather_disaster"

    if edge == 0.0:
        return None

    bias            = "YES" if edge > 0 else "NO"
    signal_strength = abs(edge)
    threshold       = MIN_STRENGTH.get(strategy, 0.3)
    if signal_strength < threshold:
        return None

    _TYPE_MAP = {
        "laddering":          "weather_laddering",
        "weather_prediction": "weather_prediction",
    }
    # disaster maps to either "seismic" or "weather_disaster" depending on source_label
    sig_type = _TYPE_MAP.get(strategy, source_label if source_label else f"{strategy}_signal")
    return SignalResult(
        market_id=market_id,
        outcome_id=outcome_id,
        signal_type=sig_type,
        directional_bias=bias,
        signal_strength=signal_strength,
        explanation=f"{ext_narrative} (Strategy: {strategy})",
        top_traders=[],
        created_at=now_utc,
    )


# ── Main signal aggregation ───────────────────────────────────────────────────

async def aggregate_market_signals(
    db: DB,
    market_id: str,
    strategy: str = "bayesian_ensemble",
    cutoff_time: Optional[datetime] = None,
    market_question: Optional[str] = None,
) -> Optional[SignalResult]:
    """
    Aggregate skilled-trader activity into a Bayesian alpha signal.
    Returns a SignalResult dataclass or None if no signal passes quality gates.
    """

    # ── 0a. no_bias — pure price-structure strategy ───────────────────────────
    if strategy == "no_bias":
        yes_price = await _get_yes_price(db, market_id)
        # Backtest-validated zone: 0.20-0.50 only. Peripheral 0.15-0.20 and
        # 0.50-0.80 zones had negative PnL in backtests and are excluded.
        if yes_price is None or not (0.20 <= yes_price <= 0.50):
            return None
        outcome_id = await _get_yes_outcome_id(db, market_id)
        if outcome_id is None:
            return None

        bias_strength = 1.5 + (0.50 - yes_price) * 2.0
        narrative = (
            f"Core retail-overbuy zone: Significant 'YES' premium detected "
            f"(Price=${yes_price:.3f})."
        )

        if bias_strength < MIN_STRENGTH.get(strategy, 0.8):
            return None

        return SignalResult(
            market_id=market_id,
            outcome_id=outcome_id,
            signal_type="no_bias",
            directional_bias="NO",
            signal_strength=bias_strength,
            explanation=(
                f"[No-Bias Discovery] {narrative} "
                f"Documented alpha edge for NO against retail YES conviction. "
                f"Strength={bias_strength:.2f}"
            ),
            top_traders=[],
            created_at=cutoff_time or datetime.utcnow(),
        )

    # ── 0b. External-data strategies — always return here, no trader fallthrough ─
    if strategy in _EXTERNAL_DATA_STRATEGIES:
        market_q = market_question
        if not market_q:
            market_q = await db.fetchval(
                "SELECT question FROM markets WHERE id=?", (market_id,)
            )
        if not market_q:
            return None
        ext_sig = await _build_external_signal(
            db, market_id, market_q, strategy, cutoff_time=cutoff_time
        )
        return ext_sig  # None if no signal — never fall through to trader data

    # ── 0c. black_swan — price-structure only, no trader data ─────────────────
    if strategy == "black_swan":
        yes_price = await _get_yes_price(db, market_id)
        if yes_price is None or not (0.005 <= yes_price <= 0.05):
            return None
        outcome_id = await _get_yes_outcome_id(db, market_id)
        if outcome_id is None:
            return None
        strength = min(2.5, round(1.0 + (0.05 - yes_price) / 0.045 * 1.5, 4))
        if strength < MIN_STRENGTH.get(strategy, 0.5):
            return None
        return SignalResult(
            market_id=market_id,
            outcome_id=outcome_id,
            signal_type="black_swan",
            directional_bias="YES",
            signal_strength=strength,
            explanation=f"[Black Swan] Deep tail YES floor: YES={yes_price:.3f}. Strength={strength:.2f}",
            top_traders=[],
            created_at=cutoff_time or datetime.utcnow(),
        )

    # ── 0d. long_range — price-structure on 30+ day markets, no trader data ───
    if strategy == "long_range":
        now_utc  = cutoff_time or datetime.utcnow()
        end_date = await db.fetchval(
            "SELECT end_date_iso FROM markets WHERE id=?", (market_id,)
        )
        resolved_end: Optional[datetime] = None
        if end_date is not None:
            resolved_end = end_date if isinstance(end_date, datetime) else (
                datetime.fromisoformat(str(end_date).split(".")[0])
            )
        if resolved_end is None and market_question:
            resolved_end = _parse_end_date_from_question(market_question)
        if resolved_end is None:
            mq = await db.fetchval("SELECT question FROM markets WHERE id=?", (market_id,))
            if mq:
                resolved_end = _parse_end_date_from_question(mq)
        if resolved_end is None or (resolved_end - now_utc).days < 30:
            return None

        yes_price = await _get_yes_price(db, market_id)
        if yes_price is None:
            return None

        if 0.15 <= yes_price <= 0.42:
            bias       = "YES"
            outcome_id = await _get_yes_outcome_id(db, market_id)
            strength   = round((0.42 - yes_price) / 0.27 * 1.2 + 0.3, 4)
            narrative  = f"Underpriced YES on 30d+ market: {yes_price:.3f}"
        elif 0.58 <= yes_price <= 0.85:
            bias       = "NO"
            outcome_id = await db.fetchval(
                "SELECT id FROM outcomes WHERE market_id=? AND LOWER(name)='no' LIMIT 1",
                (market_id,),
            )
            strength   = round((yes_price - 0.58) / 0.27 * 1.2 + 0.3, 4)
            narrative  = f"Overpriced YES on 30d+ market: {yes_price:.3f}"
        else:
            return None

        if outcome_id is None or strength < MIN_STRENGTH.get(strategy, 0.4):
            return None
        return SignalResult(
            market_id=market_id,
            outcome_id=outcome_id,
            signal_type="long_range",
            directional_bias=bias,
            signal_strength=strength,
            explanation=f"[Long-Range] {narrative}. Strength={strength:.2f}",
            top_traders=[],
            created_at=now_utc,
        )

    # ── 0e. volatility — bid-ask spread signal, no trader data ───────────────
    if strategy == "volatility":
        snap = await db.fetchone(
            """
            SELECT ps.best_bid, ps.best_ask, ps.mid_price
            FROM price_snapshots ps
            JOIN outcomes o ON ps.outcome_id = o.id
            WHERE ps.market_id=? AND LOWER(o.name)='yes'
            ORDER BY ps.rowid DESC LIMIT 1
            """,
            (market_id,),
        )
        if snap is None:
            return None
        spread    = float(snap["best_ask"] or 0) - float(snap["best_bid"] or 0)
        yes_price = float(snap["mid_price"] or 0.5)
        if spread < 0.06 or not (0.25 <= yes_price <= 0.75):
            return None
        if yes_price <= 0.50:
            bias       = "YES"
            outcome_id = await _get_yes_outcome_id(db, market_id)
        else:
            bias       = "NO"
            outcome_id = await db.fetchval(
                "SELECT id FROM outcomes WHERE market_id=? AND LOWER(name)='no' LIMIT 1",
                (market_id,),
            )
        strength = round(spread * 6.0, 4)
        if outcome_id is None or strength < MIN_STRENGTH.get(strategy, 0.3):
            return None
        return SignalResult(
            market_id=market_id,
            outcome_id=outcome_id,
            signal_type="volatility",
            directional_bias=bias,
            signal_strength=strength,
            explanation=(
                f"[Volatility] Wide bid-ask spread {spread:.3f} — "
                f"{bias} at YES={yes_price:.3f}. Strength={strength:.2f}"
            ),
            top_traders=[],
            created_at=cutoff_time or datetime.utcnow(),
        )

    # ── 1. Fetch skilled-trader trades (trader strategies only below here) ─────
    ts_filter = ""
    ts_params: tuple = ()
    if cutoff_time:
        ts_filter = "AND t.timestamp <= ?"
        ts_params = (cutoff_time.strftime("%Y-%m-%d %H:%M:%S"),)

    results = await db.fetchall(
        f"""
        SELECT t.trader_address, t.side, t.size, t.market_id, t.outcome_id,
               t.timestamp, t.price,
               tc.label, tp.gamma_score, tp.avg_clv
        FROM trades t
        JOIN trader_classifications tc ON t.trader_address = tc.address
        JOIN trader_profiles        tp ON t.trader_address = tp.address
        WHERE t.market_id=?
          AND tc.label IN ('whale','serious_non_whale','topic_specialist')
          {ts_filter}
        """,
        (market_id,) + ts_params,
    )

    if not results:
        # No skilled-trader trades for this market — return None for all strategies.
        # No-fallback policy: each strategy is independent, no cross-contamination.
        return None

    # ── 2. Strategy-specific filtering ───────────────────────────────────────
    weather_edge:    float = 0.0
    yes_market_price: Optional[float] = None

    if strategy in ("laddering", "disaster"):
        market_q = market_question or await db.fetchval(
            "SELECT question FROM markets WHERE id=?", (market_id,)
        )
        if market_q:
            live_price = await _get_yes_price(db, market_id)
            yes_market_price = live_price if live_price is not None else 0.5
            _wx = await compute_weather_alpha(market_q, "YES", yes_market_price)
            weather_edge = _wx[0] if isinstance(_wx, tuple) else 0.0

    elif strategy == "black_swan":
        yes_market_price = await _get_yes_price(db, market_id)

    if strategy == "conservative_snw":
        filtered = [r for r in results if r.label == "serious_non_whale"]
    elif strategy == "aggressive_whale":
        filtered = [r for r in results if r.label == "whale"]
    elif strategy == "specialist_precision":
        filtered = [r for r in results
                    if (r.gamma_score or 0) > 0.6 or r.label == "topic_specialist"]
    elif strategy == "long_range":
        filtered = [r for r in results
                    if r.label == "topic_specialist"
                    or (r.label == "serious_non_whale" and (r.gamma_score or 0) > 0.4)]
    elif strategy == "volatility":
        filtered = [r for r in results
                    if r.label in ("whale", "serious_non_whale", "topic_specialist")]
    elif strategy == "no_bias":
        if yes_market_price is not None and not (0.15 <= yes_market_price <= 0.80):
            return None
        filtered = list(results)
    elif strategy == "black_swan":
        # Narrowed from 0.01-0.12 to 0.005-0.05: only deepest tail events.
        # Range 0.05-0.12 produces too many false positives (markets correctly
        # priced at 5-12% that rarely appreciate in any reasonable hold window).
        if yes_market_price is not None and not (0.005 <= yes_market_price <= 0.05):
            return None
        filtered = [r for r in results
                    if r.label in ("serious_non_whale", "topic_specialist", "whale")]
    elif strategy in ("laddering", "disaster"):
        filtered = [r for r in results
                    if (r.gamma_score or 0) > 0.5
                    or r.label in ("serious_non_whale", "topic_specialist")]
    else:
        filtered = list(results)

    if not filtered:
        return None

    # ── 3. Compute five signal components ────────────────────────────────────
    yes_global = no_global = 0.0
    yes_topic  = no_topic  = 0.0
    yes_traders: set = set()
    no_traders:  set = set()
    yes_early = no_early = 0.0
    yes_conviction = no_conviction = 0.0

    timestamps = [r.timestamp for r in filtered if r.timestamp is not None]
    earliest_ts = min(timestamps) if timestamps else None
    max_size = max((r.size or 0.0) for r in filtered) or 1.0

    top_traders: List[Dict[str, Any]] = []

    for r in filtered:
        addr  = r.trader_address
        side  = r.side
        size  = r.size or 0.0
        label = r.label
        gamma = r.gamma_score or 0.0
        avg_clv = r.avg_clv or 0.0
        ts    = r.timestamp

        base_w    = BASE_WEIGHTS.get(label, 1.0)
        skill_w   = max(0.0, min(1.0, avg_clv))
        topic_w   = max(0.0, min(1.0, gamma))
        size_norm = float(size) / float(max_size)

        early_bonus = 0.0
        if earliest_ts and ts and timestamps:
            time_span  = (max(timestamps) - earliest_ts).total_seconds() + 1
            trade_age  = (ts - earliest_ts).total_seconds()
            rel_age    = trade_age / time_span
            if rel_age <= 0.20:
                early_bonus = 1.0 - rel_age

        composite_skill = base_w * (1.0 + skill_w)

        if (side or "").lower() in ("yes", "buy"):
            yes_global     += composite_skill
            yes_topic      += topic_w * base_w
            yes_traders.add(addr)
            yes_early      += early_bonus
            yes_conviction += size_norm
        else:
            no_global      += composite_skill
            no_topic       += topic_w * base_w
            no_traders.add(addr)
            no_early       += early_bonus
            no_conviction  += size_norm

        top_traders.append({
            "address": addr,
            "label":   label,
            "side":    side,
            "size":    size,
            "skill":   round(composite_skill, 4),
        })

    # ── Combine ───────────────────────────────────────────────────────────────
    yes_score = (
        COEFF_GLOBAL_SKILL  * yes_global      +
        COEFF_TOPIC_SKILL   * yes_topic        +
        COEFF_CONVERGENCE   * len(yes_traders) +
        COEFF_EARLY_ENTRY   * yes_early        +
        COEFF_CONVICTION    * yes_conviction
    )
    no_score = (
        COEFF_GLOBAL_SKILL  * no_global      +
        COEFF_TOPIC_SKILL   * no_topic        +
        COEFF_CONVERGENCE   * len(no_traders) +
        COEFF_EARLY_ENTRY   * no_early        +
        COEFF_CONVICTION    * no_conviction
    )

    if weather_edge > 0:
        yes_score += weather_edge * 3.0
    elif weather_edge < 0:
        no_score  += abs(weather_edge) * 3.0

    # Strategy overlays
    if strategy == "no_bias" and yes_market_price is not None:
        if 0.20 <= yes_market_price <= 0.50:
            no_score *= 1.5 + (0.50 - yes_market_price) * 2.0
        elif yes_market_price < 0.20 or yes_market_price > 0.50:
            no_score *= 1.2

    if strategy == "black_swan" and yes_market_price is not None:
        # Score boost for deepest tail events (already filtered to 0.005-0.05)
        if 0.005 <= yes_market_price <= 0.03:
            yes_score *= 1.0 + (0.03 - yes_market_price) / 0.025 * 1.5
        elif yes_market_price <= 0.05:
            yes_score *= 1.3

    if strategy == "volatility":
        yes_score = (COEFF_GLOBAL_SKILL * yes_global + COEFF_TOPIC_SKILL * yes_topic +
                     COEFF_CONVERGENCE * len(yes_traders) + COEFF_EARLY_ENTRY * yes_early +
                     0.30 * yes_conviction)
        no_score  = (COEFF_GLOBAL_SKILL * no_global  + COEFF_TOPIC_SKILL * no_topic +
                     COEFF_CONVERGENCE * len(no_traders) + COEFF_EARLY_ENTRY * no_early +
                     0.30 * no_conviction)

    if strategy == "long_range":
        yes_score = (COEFF_GLOBAL_SKILL * yes_global + 0.45 * yes_topic +
                     0.35 * len(yes_traders) + COEFF_EARLY_ENTRY * yes_early +
                     0.05 * yes_conviction)
        no_score  = (COEFF_GLOBAL_SKILL * no_global  + 0.45 * no_topic +
                     0.35 * len(no_traders) + COEFF_EARLY_ENTRY * no_early +
                     0.05 * no_conviction)

    bias           = "YES" if yes_score >= no_score else "NO"
    final_strength = abs(yes_score - no_score)

    # ── 4a. Quality gates (trader-based strategies only) ─────────────────────
    if strategy in _TRADER_SIGNAL_STRATEGIES:
        winning_count = len(yes_traders) if bias == "YES" else len(no_traders)
        if winning_count < 1:
            return None

        if timestamps:
            newest_ts = max(timestamps)
            lag_h = (datetime.utcnow() - newest_ts).total_seconds() / 3600.0
            # Backtest showed 48h gives more time for trades to mature without
            # losing meaningful signal quality. 24h was too aggressive.
            if lag_h > 48.0:
                final_strength *= 0.70

        total_traders = len(yes_traders) + len(no_traders)
        if total_traders < 5:
            final_strength *= 0.80

    # ── 4b. Conviction threshold ──────────────────────────────────────────────
    if final_strength < MIN_STRENGTH.get(strategy, 1.0):
        return None

    # ── 5. Sort top traders ───────────────────────────────────────────────────
    top_traders_sorted = sorted(top_traders, key=lambda t: t["skill"], reverse=True)
    explanation = generate_signal_explanation(final_strength, bias, top_traders_sorted)

    # ── 6. Determine outcome_id for bias direction ────────────────────────────
    if bias == "NO":
        final_outcome_id = await db.fetchval(
            "SELECT id FROM outcomes WHERE market_id=? AND LOWER(name)='no' LIMIT 1",
            (market_id,),
        ) or (filtered[0].outcome_id if filtered else None)
    else:
        final_outcome_id = await db.fetchval(
            "SELECT id FROM outcomes WHERE market_id=? AND LOWER(name)='yes' LIMIT 1",
            (market_id,),
        ) or (filtered[0].outcome_id if filtered else None)

    if final_outcome_id is None:
        return None

    # ── 7. Live liquidity gate ────────────────────────────────────────────────
    clob = ClobClient()
    is_liquid = True
    liquidity_label = "High"
    try:
        asset_id = await db.fetchval(
            "SELECT asset_id FROM outcomes WHERE id=?", (final_outcome_id,)
        )
        if asset_id:
            book = await clob.get_orderbook(asset_id)
            side_key     = "asks" if bias == "YES" else "bids"
            orders       = book.get(side_key, [])
            required     = float(settings.app.trade_amount) * 2.0
            depth_found  = 0.0
            best_bid     = float(book.get("bids", [[0, 0]])[0][0]) if book.get("bids") else 0.5
            best_ask     = float(book.get("asks", [[1, 0]])[0][0]) if book.get("asks") else 0.5
            mid_price    = (best_bid + best_ask) / 2.0
            price_limit  = mid_price * 1.02 if bias == "YES" else mid_price * 0.98
            for p, s in orders:
                p_val, s_val = float(p), float(s)
                if (bias == "YES" and p_val <= price_limit) or \
                   (bias == "NO"  and p_val >= price_limit):
                    depth_found += p_val * s_val
                else:
                    break
            if depth_found < required:
                is_liquid = False
                liquidity_label = "Low"
            elif depth_found < required * 2:
                liquidity_label = "Medium"
    except Exception as e:
        logger.warning(f"Liquidity check failed for {market_id}: {e}")
    finally:
        await clob.close()

    if not is_liquid:
        return None

    _SIGNAL_TYPE_MAP = {
        "no_bias":             "no_bias",
        "black_swan":          "black_swan",
        "long_range":          "long_range",
        "volatility":          "volatility",
        "conservative_snw":    "conservative_snw",
        "aggressive_whale":    "aggressive_whale",
        "specialist_precision":"specialist_precision",
        "bayesian_ensemble":   "bayesian_ensemble",
        "laddering":           "weather_laddering",
        "disaster":            "weather_disaster",
        "weather_prediction":  "weather_prediction",
    }

    return SignalResult(
        market_id=market_id,
        outcome_id=final_outcome_id,
        signal_type=_SIGNAL_TYPE_MAP.get(strategy, "bayesian_ensemble"),
        directional_bias=bias,
        signal_strength=final_strength,
        explanation=f"[{liquidity_label} Liquidity] {explanation}",
        top_traders=top_traders_sorted[:5],
        created_at=cutoff_time or datetime.utcnow(),
    )
