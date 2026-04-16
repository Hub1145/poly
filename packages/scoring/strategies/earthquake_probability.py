"""
Earthquake / Seismic Risk Signal

Strategy:
  1. Detect if a Polymarket question relates to earthquakes / seismic events.
  2. Parse the exact count constraint from the question
     ("exactly N", "more than N", "fewer than N", "N or more", "between N and M", etc.)
  3. Query USGS FDSN for historical rate in the relevant region.
  4. Compute the correct Poisson probability for that specific count constraint.
  5. Return edge = P(constraint) - market_price.
"""

import logging
import math
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------- #
# Region keyword → bounding box (minlat, maxlat, minlon, maxlon)         #
# ---------------------------------------------------------------------- #
REGION_MAP = {
    # North America
    "California":        (32.5,  42.0, -124.5, -114.0),
    "San Francisco":     (37.2,  38.2, -122.8, -121.8),
    "Los Angeles":       (33.5,  34.5, -118.9, -117.6),
    "Seattle":           (47.0,  48.5, -122.5, -121.0),
    "Pacific Northwest": (42.0,  49.0, -125.0, -116.0),
    "Alaska":            (54.0,  71.5, -168.0, -130.0),
    "Cascadia":          (40.0,  52.0, -130.0, -115.0),
    "New Madrid":        (34.0,  38.0,  -92.0,  -87.0),
    # South/Central America
    "Chile":             (-56.0, -17.0, -76.0, -65.0),
    "Mexico":            ( 14.5,  32.7,-118.0, -86.5),
    "Peru":              (-18.5,   0.0, -82.0, -68.0),
    # Europe / Mediterranean
    "Turkey":            ( 35.8,  42.3,  25.0,  44.8),
    "Italy":             ( 36.5,  47.1,   6.6,  18.5),
    "Greece":            ( 34.8,  41.8,  19.3,  29.7),
    "Romania":           ( 43.7,  48.3,  20.2,  30.0),
    # Middle East / Asia
    "Iran":              ( 25.0,  39.8,  44.0,  63.3),
    "Nepal":             ( 26.4,  30.4,  80.0,  88.2),
    "India":             (  8.0,  37.5,  68.0,  97.5),
    "Pakistan":          ( 23.5,  37.0,  60.5,  77.0),
    "Afghanistan":       ( 29.5,  38.5,  60.5,  75.0),
    "China":             ( 18.0,  53.5,  73.5, 135.0),
    # East Asia / Pacific
    "Japan":             ( 30.0,  45.5, 129.5, 146.0),
    "Taiwan":            ( 21.5,  25.5, 119.5, 122.5),
    "Philippines":       (  4.5,  20.5, 116.5, 127.0),
    "Indonesia":         (-11.0,   6.0,  95.0, 141.0),
    "New Zealand":       (-47.5, -34.0, 166.0, 178.5),
    # Generic
    "Pacific Rim":       (-60.0,  60.0, 120.0, -60.0),
    "Ring of Fire":      (-60.0,  60.0, 100.0, -60.0),
}

EARTHQUAKE_KEYWORDS = {
    "earthquake", "seismic", "quake", "tremor", "magnitude",
    "richter", "usgs", "epicenter", "aftershock", "tsunami",
    "fault", "tectonic", "seismicity", "shaking"
}

_SPORTS_CONTEXT = {
    "win", "mls", "cup", "championship", "playoff", "season",
    "league", "soccer", "football", "basketball", "baseball",
    "nfl", "nba", "mlb", "nhl", "title", "finals", "coach"
}
_STRONG_EQ_KEYWORDS = EARTHQUAKE_KEYWORDS - {"earthquake", "quake"}

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def is_earthquake_market(question: str) -> bool:
    q_lower = question.lower()
    if not any(kw in q_lower for kw in EARTHQUAKE_KEYWORDS):
        return False
    if not any(kw in q_lower for kw in _STRONG_EQ_KEYWORDS):
        if any(sw in q_lower for sw in _SPORTS_CONTEXT):
            return False
    return True


def _extract_region(question: str) -> Optional[str]:
    for region in REGION_MAP:
        if region.lower() in question.lower():
            return region
    return None


def _extract_magnitude_threshold(question: str) -> Optional[float]:
    patterns = [
        r"magnitude\s*(\d+(?:\.\d+)?)",
        r"\bm\s*(\d+(?:\.\d+)?)\b",
        r"[≥>=]\s*(\d+(?:\.\d+)?)",
        r"(\d+(?:\.\d+)?)\s*(?:or\s+above|or\s+higher|or\s+greater|and\s+above|and\s+higher)",
        r"(?:above|over|exceed(?:ing)?|at\s+least)\s+(\d+(?:\.\d+)?)",
    ]
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1))
                # Sanity: magnitudes are 2.0–10.0, counts would be 0–100+
                if 2.0 <= val <= 10.0:
                    return val
            except ValueError:
                continue
    return None


def _extract_days_window(question: str) -> int:
    """
    Extract forecast window in days remaining from now to the deadline.
    Handles: 'within N days', 'next N weeks', 'by Month Day [Year]',
             'from Month Day - Day', 'in 2026' (full year), etc.
    Falls back to 30 days.
    """
    today = datetime.utcnow()

    # "from April 6 - 12" or "from April 6 to April 12"
    m = re.search(
        r"from\s+([A-Za-z]+)\s+(\d{1,2})\s*[-–to]+\s*(?:[A-Za-z]+\s+)?(\d{1,2})",
        question, re.IGNORECASE
    )
    if m:
        month_str = m.group(1).lower()
        month = _MONTH_MAP.get(month_str)
        if month:
            day_start = int(m.group(2))
            day_end   = int(m.group(3))
            for yr in (today.year, today.year + 1):
                try:
                    start = datetime(yr, month, day_start)
                    end   = datetime(yr, month, day_end)
                    if end >= today:
                        remaining = max(1, (end - max(today, start)).days + 1)
                        return remaining
                except ValueError:
                    pass

    # "by <Month> <Day>[, <Year>]"
    m = re.search(
        r"by\s+([A-Za-z]+)\s+(\d{1,2})(?:,?\s+(\d{4}))?",
        question, re.IGNORECASE
    )
    if m:
        month_str = m.group(1).lower()
        day       = int(m.group(2))
        year_str  = m.group(3)
        month     = _MONTH_MAP.get(month_str)
        if month:
            year = int(year_str) if year_str else today.year
            for yr in (year, year + 1):
                try:
                    target = datetime(yr, month, day)
                    if target >= today:
                        return max(1, (target - today).days + 1)
                except ValueError:
                    pass

    # "before <Year>" / "in <Year>"
    m = re.search(r"\b(20\d{2})\b", question)
    if m:
        year = int(m.group(1))
        try:
            year_end = datetime(year, 12, 31)
            if year_end >= today:
                return max(1, (year_end - today).days + 1)
        except ValueError:
            pass

    # Explicit durations
    m = re.search(r"(\d+)\s*days?", question, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*weeks?", question, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 7
    m = re.search(r"(\d+)\s*months?", question, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 30

    return 30


def _extract_count_constraint(question: str) -> Tuple[str, int, int]:
    """
    Parse the count constraint from the market question.

    Returns (constraint_type, n, m):
      "exactly"    → P(X = n)
      "more_than"  → P(X > n)
      "fewer_than" → P(X < n)  = P(X <= n-1)
      "at_least"   → P(X >= n)
      "at_most"    → P(X <= n)
      "between"    → P(n <= X <= m)
      "any"        → P(X >= 1)    [generic "will there be an earthquake"]
    """
    q = question.lower()

    # "exactly N" — must appear before "earthquake" and not be a magnitude
    m = re.search(r"exactly\s+(\d+)\s+earthquake", q)
    if m:
        return ("exactly", int(m.group(1)), 0)
    # "exactly N" anywhere (looser)
    m = re.search(r"exactly\s+(\d+)", q)
    if m:
        n = int(m.group(1))
        # Skip if this looks like a magnitude (2–10)
        if not (2 <= n <= 10 and ("magnitude" in q or " m" + str(n) in q)):
            return ("exactly", n, 0)

    # "more than N earthquakes" / "greater than N"
    m = re.search(r"(?:more|greater)\s+than\s+(\d+)\s+earthquake", q)
    if m:
        return ("more_than", int(m.group(1)), 0)
    m = re.search(r"(?:more|greater)\s+than\s+(\d+)", q)
    if m:
        n = int(m.group(1))
        if n > 10:  # not a magnitude
            return ("more_than", n, 0)

    # "fewer than N earthquakes" / "less than N"
    m = re.search(r"(?:fewer|less)\s+than\s+(\d+)\s+earthquake", q)
    if m:
        return ("fewer_than", int(m.group(1)), 0)
    m = re.search(r"(?:fewer|less)\s+than\s+(\d+)", q)
    if m:
        n = int(m.group(1))
        if n > 10:
            return ("fewer_than", n, 0)

    # "between N and M earthquakes"
    m = re.search(r"between\s+(\d+)\s+and\s+(\d+)\s+earthquake", q)
    if m:
        return ("between", int(m.group(1)), int(m.group(2)))
    m = re.search(r"between\s+(\d+)\s+and\s+(\d+)", q)
    if m:
        n, k = int(m.group(1)), int(m.group(2))
        if n > 10 or k > 10:  # not magnitudes
            return ("between", n, k)

    # "N or fewer earthquakes" / "N or less"
    m = re.search(r"(\d+)\s+or\s+(?:fewer|less)\s+earthquake", q)
    if m:
        return ("at_most", int(m.group(1)), 0)
    m = re.search(r"(\d+)\s+or\s+(?:fewer|less)", q)
    if m:
        n = int(m.group(1))
        if n > 10:
            return ("at_most", n, 0)

    # "N or more earthquakes" / "20 or more earthquakes"
    m = re.search(r"(\d+)\s+or\s+more\s+earthquake", q)
    if m:
        return ("at_least", int(m.group(1)), 0)
    m = re.search(r"(\d+)\s+or\s+more", q)
    if m:
        n = int(m.group(1))
        if n > 10:
            return ("at_least", n, 0)

    # "at least N earthquakes"
    m = re.search(r"at\s+least\s+(\d+)\s+earthquake", q)
    if m:
        return ("at_least", int(m.group(1)), 0)

    # Generic: "Another earthquake", "will there be a magnitude X earthquake"
    return ("any", 1, 0)


# ---------------------------------------------------------------------- #
# Poisson math (no scipy dependency)                                     #
# ---------------------------------------------------------------------- #

def _poisson_pmf(k: int, lam: float) -> float:
    """P(X = k) for Poisson(lam)."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    if k < 0:
        return 0.0
    # Use log to avoid factorial overflow
    log_p = -lam + k * math.log(lam) - sum(math.log(i) for i in range(1, k + 1))
    return math.exp(log_p)


def _poisson_cdf(k: int, lam: float) -> float:
    """P(X <= k) for Poisson(lam)."""
    if k < 0:
        return 0.0
    return min(1.0, sum(_poisson_pmf(i, lam) for i in range(k + 1)))


def _compute_prob(constraint: str, n: int, m: int, lam: float) -> float:
    """Compute P(count satisfies constraint) under Poisson(lam)."""
    if constraint == "exactly":
        return _poisson_pmf(n, lam)
    elif constraint == "more_than":
        return 1.0 - _poisson_cdf(n, lam)
    elif constraint == "fewer_than":
        return _poisson_cdf(n - 1, lam)
    elif constraint == "at_least":
        return 1.0 - _poisson_cdf(n - 1, lam)
    elif constraint == "at_most":
        return _poisson_cdf(n, lam)
    elif constraint == "between":
        return _poisson_cdf(m, lam) - _poisson_cdf(n - 1, lam)
    else:  # "any" — at least 1
        return 1.0 - _poisson_pmf(0, lam)


async def compute_earthquake_alpha(
    question: str,
    current_price: float,
) -> Optional[Tuple[float, str]]:
    """
    Compute alpha edge for a seismic/earthquake prediction market.

    Returns (edge, explanation) or None if the market cannot be scored.
    edge > 0 → YES is underpriced.
    edge < 0 → NO is underpriced.
    """
    if not is_earthquake_market(question):
        return None

    region = _extract_region(question)
    if not region:
        region = "_global"
        REGION_MAP["_global"] = (-90.0, 90.0, -180.0, 180.0)

    min_mag = _extract_magnitude_threshold(question)
    if min_mag is None:
        min_mag = 5.0

    days_window  = _extract_days_window(question)
    constraint, n, m = _extract_count_constraint(question)

    # ------------------------------------------------------------------ #
    # Query USGS for historical annual rate                               #
    # ------------------------------------------------------------------ #
    bbox     = REGION_MAP[region]
    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=365)

    usgs_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format":       "geojson",
        "starttime":    start_dt.strftime("%Y-%m-%d"),
        "endtime":      end_dt.strftime("%Y-%m-%d"),
        "minlatitude":  bbox[0],
        "maxlatitude":  bbox[1],
        "minlongitude": bbox[2],
        "maxlongitude": bbox[3],
        "minmagnitude": min_mag,
        "orderby":      "time",
        "limit":        20000,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(usgs_url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"USGS API request failed for {region}: {e}")
        return None

    count_in_year = len(data.get("features", []))
    if count_in_year == 0:
        return None

    # λ = expected count in the question's time window
    lam_per_day = count_in_year / 365.0
    lam         = lam_per_day * days_window

    # ------------------------------------------------------------------ #
    # Aftershock boost — only for "any/at_least 1" markets               #
    # ------------------------------------------------------------------ #
    aftershock_note = ""
    if constraint in ("any", "at_least") and n <= 1:
        recent_start = end_dt - timedelta(days=7)
        recent_params = {
            "format":       "geojson",
            "starttime":    recent_start.strftime("%Y-%m-%d"),
            "endtime":      end_dt.strftime("%Y-%m-%d"),
            "minlatitude":  bbox[0],
            "maxlatitude":  bbox[1],
            "minlongitude": bbox[2],
            "maxlongitude": bbox[3],
            "minmagnitude": max(min_mag, 5.5),
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                recent_resp = await client.get(usgs_url, params=recent_params)
                recent_resp.raise_for_status()
                recent_count = len(recent_resp.json().get("features", []))
                if recent_count > 0:
                    boost = min(0.20, recent_count * 0.04)
                    lam   = lam + boost * days_window
                    aftershock_note = f" (+{boost:.2f} aftershock boost from {recent_count} recent events)"
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Compute forecast probability for the specific constraint            #
    # ------------------------------------------------------------------ #
    forecast_prob = _compute_prob(constraint, n, m, lam)
    forecast_prob = max(0.01, min(0.99, forecast_prob))

    edge = forecast_prob - current_price

    # Constraint description for explanation
    if constraint == "exactly":
        constraint_desc = f"exactly {n}"
    elif constraint == "more_than":
        constraint_desc = f"more than {n}"
    elif constraint == "fewer_than":
        constraint_desc = f"fewer than {n}"
    elif constraint == "at_least":
        constraint_desc = f"at least {n}"
    elif constraint == "at_most":
        constraint_desc = f"{n} or fewer"
    elif constraint == "between":
        constraint_desc = f"between {n} and {m}"
    else:
        constraint_desc = "at least 1"

    region_label = "Global" if region == "_global" else region
    explanation = (
        f"[Seismic Audit] {region_label} M{min_mag}+: "
        f"{count_in_year} historical events/year → λ={lam:.2f} in {days_window}d window. "
        f"P({constraint_desc}) = {forecast_prob*100:.1f}% vs {current_price*100:.1f}% market price. "
        f"Edge={edge:+.3f}.{aftershock_note}"
    )

    logger.info(f"[EQ] {region_label}: {explanation}")

    if abs(edge) < 0.03:
        return None
    return edge, explanation
