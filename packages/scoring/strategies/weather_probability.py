"""
Weather / Meteorological Alpha Signal
======================================
Implements three signal types based on documented profitable-trader strategies:

  Signal 1 — Temperature Ladder YES (primary daily signal)
    Fetch Open-Meteo probability distribution for the city.
    Lead leg: bucket has >35% model probability but Polymarket prices it
    below that by more than MIN_GAP_PP percentage points → YES.
    Also layer adjacent ±1°C/°F buckets if >15% model prob and underpriced.
    gopfan2 rule: any bucket priced below $0.10 that model gives >20% → YES.

  Signal 2 — Structural NO (75% base-rate bias)
    YES priced $0.20–$0.45 but model gives <5% probability → NO.
    gopfan2 hard rule: YES above $0.45 + model agrees it's overpriced → NO.

  Signal 3 — Disaster/alert (opportunistic)
    Uses wind/gust data from Open-Meteo for hurricane/flood threshold markets.
    Precipitation quantity markets are skipped entirely (resolution-mismatch risk).

Data source: Open-Meteo (free, global, ECMWF/GFS data, no API key needed).
"""

import logging
import re
import time as _time
from datetime import datetime as _dt, timedelta as _td, timezone as _tz, date as _date
from typing import Any, Dict, Optional, Tuple

import httpx
from scipy.stats import norm

from packages.core.config import settings, WEATHER_MIN_GAP_PP

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------- #
# City map — lat/lon for Open-Meteo queries                              #
# ---------------------------------------------------------------------- #
CITY_MAP: Dict[str, Dict[str, float]] = {
    # Phase 1 focus cities (best volume + coverage)
    "New York":       {"lat": 40.7128, "lon": -74.0060},
    "NYC":            {"lat": 40.7128, "lon": -74.0060},
    "London":         {"lat": 51.5074, "lon": -0.1278},
    "Seoul":          {"lat": 37.5665, "lon": 126.9780},
    "Buenos Aires":   {"lat": -34.6037, "lon": -58.3816},
    # North America
    "Los Angeles":    {"lat": 34.0522, "lon": -118.2437},
    "LA":             {"lat": 34.0522, "lon": -118.2437},
    "Chicago":        {"lat": 41.8781, "lon": -87.6298},
    "Houston":        {"lat": 29.7604, "lon": -95.3698},
    "Phoenix":        {"lat": 33.4484, "lon": -112.0740},
    "Philadelphia":   {"lat": 39.9526, "lon": -75.1652},
    "San Diego":      {"lat": 32.7157, "lon": -117.1611},
    "Dallas":         {"lat": 32.7767, "lon": -96.7970},
    "Miami":          {"lat": 25.7617, "lon": -80.1918},
    "Atlanta":        {"lat": 33.7490, "lon": -84.3880},
    "Seattle":        {"lat": 47.6062, "lon": -122.3321},
    "Boston":         {"lat": 42.3601, "lon": -71.0589},
    "Denver":         {"lat": 39.7392, "lon": -104.9903},
    "Las Vegas":      {"lat": 36.1699, "lon": -115.1398},
    "Washington":     {"lat": 38.9072, "lon": -77.0369},
    "San Francisco":  {"lat": 37.7749, "lon": -122.4194},
    "SF":             {"lat": 37.7749, "lon": -122.4194},
    "Toronto":        {"lat": 43.6532, "lon": -79.3832},
    "Vancouver":      {"lat": 49.2827, "lon": -123.1207},
    "Mexico City":    {"lat": 19.4326, "lon": -99.1332},
    "Ankara":         {"lat": 39.9334, "lon": 32.8597},
    # Europe
    "Paris":          {"lat": 48.8566, "lon":  2.3522},
    "Berlin":         {"lat": 52.5200, "lon": 13.4050},
    "Madrid":         {"lat": 40.4168, "lon": -3.7038},
    "Rome":           {"lat": 41.9028, "lon": 12.4964},
    "Amsterdam":      {"lat": 52.3676, "lon":  4.9041},
    "Vienna":         {"lat": 48.2082, "lon": 16.3738},
    "Zurich":         {"lat": 47.3769, "lon":  8.5417},
    "Stockholm":      {"lat": 59.3293, "lon": 18.0686},
    "Oslo":           {"lat": 59.9139, "lon": 10.7522},
    "Copenhagen":     {"lat": 55.6761, "lon": 12.5683},
    "Athens":         {"lat": 37.9838, "lon": 23.7275},
    "Warsaw":         {"lat": 52.2297, "lon": 21.0122},
    "Lisbon":         {"lat": 38.7223, "lon": -9.1393},
    "Barcelona":      {"lat": 41.3851, "lon":  2.1734},
    "Munich":         {"lat": 48.1351, "lon": 11.5820},
    "Istanbul":       {"lat": 41.0082, "lon": 28.9784},
    "Moscow":         {"lat": 55.7558, "lon": 37.6173},
    "Hamburg":        {"lat": 53.5753, "lon":  9.9952},
    "Frankfurt":      {"lat": 50.1109, "lon":  8.6821},
    "Budapest":       {"lat": 47.4979, "lon": 19.0402},
    "Helsinki":       {"lat": 60.1699, "lon": 24.9384},
    "Dublin":         {"lat": 53.3498, "lon": -6.2603},
    # Asia / Pacific
    "Tokyo":          {"lat": 35.6895, "lon": 139.6917},
    "Beijing":        {"lat": 39.9042, "lon": 116.4074},
    "Shanghai":       {"lat": 31.2304, "lon": 121.4737},
    "Hong Kong":      {"lat": 22.3193, "lon": 114.1694},
    "Singapore":      {"lat":  1.3521, "lon": 103.8198},
    "Mumbai":         {"lat": 19.0760, "lon": 72.8777},
    "Delhi":          {"lat": 28.6139, "lon": 77.2090},
    "Bangkok":        {"lat": 13.7563, "lon": 100.5018},
    "Sydney":         {"lat": -33.8688, "lon": 151.2093},
    "Melbourne":      {"lat": -37.8136, "lon": 144.9631},
    "Dubai":          {"lat": 25.2048, "lon": 55.2708},
    "Tel Aviv":       {"lat": 32.0853, "lon": 34.7818},
    "Taipei":         {"lat": 25.0330, "lon": 121.5654},
    "Osaka":          {"lat": 34.6937, "lon": 135.5023},
    "Busan":          {"lat": 35.1796, "lon": 129.0756},
    "Manila":         {"lat": 14.5995, "lon": 120.9842},
    "Kuala Lumpur":   {"lat":  3.1390, "lon": 101.6869},
    "Hanoi":          {"lat": 21.0285, "lon": 105.8542},
    # Africa / South America
    "Cairo":          {"lat": 30.0444, "lon": 31.2357},
    "Lagos":          {"lat":  6.5244, "lon":  3.3792},
    "Johannesburg":   {"lat": -26.2041, "lon": 28.0473},
    "Nairobi":        {"lat": -1.2921, "lon": 36.8219},
    "Sao Paulo":      {"lat": -23.5505, "lon": -46.6333},
    "Rio de Janeiro": {"lat": -22.9068, "lon": -43.1729},
    "Casablanca":     {"lat": 33.5731, "lon": -7.5898},
    "Accra":          {"lat":  5.6037, "lon": -0.1870},
    "Wellington":     {"lat": -41.2865, "lon": 174.7762},
    "Auckland":       {"lat": -36.8485, "lon": 174.7633},
}

# Weather condition keywords
_TEMP_KW     = {"temperature", "temp", "degrees", "heat", "cold", "hot", "warm",
                "cool", "freeze", "frozen", "record high", "record low", "high of",
                "low of", "fahrenheit", "celsius"}
_PRECIP_KW   = {"rainfall", "precipitation", "snowfall", "inches of rain",
                "inches of snow", "mm of rain"}  # narrowed — avoids flood markets
_WIND_KW     = {"wind", "hurricane", "typhoon", "cyclone", "mph", "knots", "gust"}
_STORM_KW    = {"tornado", "blizzard", "ice storm", "hail", "severe weather",
                "tropical storm", "derecho"}
_DISASTER_KW = {"warning", "watch", "advisory", "emergency", "evacuation",
                "red flag", "flood warning", "hurricane warning"}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# In-memory forecast cache — 30 min TTL
_FORECAST_CACHE: dict = {}
_CACHE_TTL = 1800


def _cache_key(lat, lon, target_date, unit, weather_type, past_days):
    return (round(lat, 4), round(lon, 4), target_date, unit, weather_type, past_days)


def _get_cached(key):
    entry = _FORECAST_CACHE.get(key)
    if entry and (_time.monotonic() - entry["ts"]) < _CACHE_TTL:
        return entry["data"]
    return None


def _set_cached(key, data):
    _FORECAST_CACHE[key] = {"data": data, "ts": _time.monotonic()}


def _find_city(question: str) -> Optional[Tuple[str, Dict[str, float]]]:
    """Return (city_name, {lat, lon}) for the first known city in the question.

    Uses word-boundary matching so abbreviations like 'LA' or 'SF' don't
    accidentally match substrings inside words (e.g. 'launch', 'suffer').
    """
    q = question.lower()
    for city in sorted(CITY_MAP.keys(), key=len, reverse=True):
        pattern = r"\b" + re.escape(city.lower()) + r"\b"
        if re.search(pattern, q):
            return city, CITY_MAP[city]
    return None


def _is_focus_city(city_name: str) -> bool:
    """True if the city is in the Phase 1 high-priority whitelist."""
    focus = [c.lower() for c in settings.weather.city_focus]
    return city_name.lower() in focus


def _find_date(question: str) -> Optional[str]:
    """Extract a YYYY-MM-DD target date from the question text."""
    m = re.search(r"\d{4}-\d{2}-\d{2}", question)
    if m:
        return m.group(0)

    m2 = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        question, re.IGNORECASE,
    )
    if m2:
        try:
            parsed = _dt.strptime(
                re.sub(r"\s+", " ", m2.group(0)).replace(",", ""), "%B %d %Y"
            )
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            pass

    m3 = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2})(?!\s*,?\s*\d{4})",
        question, re.IGNORECASE,
    )
    if m3:
        today = _dt.utcnow()
        for yr_offset in (0, 1):
            try:
                candidate = _dt.strptime(
                    f"{m3.group(1)} {m3.group(2)} {today.year + yr_offset}", "%B %d %Y"
                )
                if candidate.date() >= (today - _td(days=1)).date():
                    return candidate.strftime("%Y-%m-%d")
            except ValueError:
                pass

    m4 = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{4})",
        question, re.IGNORECASE,
    )
    if m4:
        try:
            candidate = _dt.strptime(f"{m4.group(1)} 15 {m4.group(2)}", "%B %d %Y")
            if candidate.date() >= (_dt.utcnow() - _td(days=1)).date():
                return candidate.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def _detect_weather_type(question: str) -> str:
    q = question.lower()
    if any(kw in q for kw in _DISASTER_KW):
        return "disaster"
    if any(kw in q for kw in _STORM_KW):
        return "storm"
    if any(kw in q for kw in _WIND_KW):
        return "wind"
    if any(kw in q for kw in _PRECIP_KW):
        return "precipitation"
    return "temperature"


def _is_temperature_bucket(question: str) -> bool:
    """Detect if this is a daily-high temperature bucket market.
    Bucket markets say 'between X and Y' or 'X°F / X°C' as the primary condition.
    """
    q = question.lower()
    return bool(re.search(
        r"(between\s+\d+\s*(and|-)\s*\d+|"
        r"high\s+(of|will\s+be|between|above|below|exceed)|"
        r"temperature\s+(will\s+be|between|above|below|of|exceed)|"
        r"\d+\s*°?\s*(f|c)\b)",
        q, re.IGNORECASE
    ))


async def _fetch_forecast(
    lat: float, lon: float, target_date: str, unit: str, weather_type: str,
    past_days: int = 0,
) -> Dict[str, Any]:
    """
    Fetch forecast/historical data from Open-Meteo.

    Always requests:
      - hourly temperature_2m  (for specific-hour queries)
      - daily temperature_2m_max / temperature_2m_min  (faster than computing from hourly)

    Also requests wind/precip variables for non-temperature weather types.
    Responses are cached in-memory for 30 minutes.
    """
    key = _cache_key(lat, lon, target_date, unit, weather_type, past_days)
    cached = _get_cached(key)
    if cached is not None:
        return cached

    # Always include hourly temperature and daily max/min
    hourly_vars = ["temperature_2m"]
    daily_vars  = ["temperature_2m_max", "temperature_2m_min"]

    if weather_type in ("precipitation", "storm", "disaster"):
        hourly_vars += ["precipitation", "rain", "snowfall"]
    if weather_type in ("wind", "storm", "disaster"):
        hourly_vars += ["windspeed_10m", "windgusts_10m"]

    params: Dict[str, Any] = {
        "latitude":           lat,
        "longitude":          lon,
        "hourly":             ",".join(hourly_vars),
        "daily":              ",".join(daily_vars),
        "forecast_days":      1 if past_days > 0 else 16,
        "temperature_unit":   "fahrenheit" if unit == "F" else "celsius",
        "windspeed_unit":     "mph",
        "precipitation_unit": "inch" if unit == "F" else "mm",
        "timezone":           "auto",
    }
    if past_days > 0:
        params["past_days"] = past_days

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.get(OPEN_METEO_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            _set_cached(key, data)
            return data
    except Exception as e:
        logger.error(f"Open-Meteo fetch failed ({lat},{lon}): {e}")
        return {}


def _get_reference_temp(
    forecast_data: Dict[str, Any],
    target_date: str,
    question: str,
) -> Optional[float]:
    """
    Extract the correct reference temperature from the forecast.

    Priority order:
      1. Open-Meteo daily aggregations (temperature_2m_max / temperature_2m_min)
         — more accurate than computing from hourly, already API-aggregated.
      2. Specific hour from hourly data when question references a time.
      3. Computed from hourly data as last resort.

    Rules:
      - "highest" / "high" / "max" / default → daily max
      - "lowest" / "low" / "min" / "overnight" → daily min
      - "at Xam/pm" / "at X:00" / "noon" → specific hourly reading
      - "average" / "mean" → hourly mean for the day
    """
    q = question.lower()

    # ── 1. Specific-hour reference (hourly data) ──────────────────────────────
    hour_m = re.search(r"at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", q)
    if not hour_m:
        hour_m = re.search(r"\b(noon|midnight)\b", q)

    if hour_m:
        hourly = forecast_data.get("hourly", {})
        times  = hourly.get("time", [])
        temps  = hourly.get("temperature_2m", [])
        day_pairs = [
            (ti, t) for ti, t in zip(times, temps)
            if ti and ti.startswith(target_date) and t is not None
        ]
        if day_pairs:
            grp = hour_m.group(0)
            if "noon" in grp:
                target_hour = 12
            elif "midnight" in grp:
                target_hour = 0
            else:
                h    = int(hour_m.group(1))
                ampm = (hour_m.group(3) or "").lower()
                if ampm == "pm" and h != 12:
                    h += 12
                elif ampm == "am" and h == 12:
                    h = 0
                target_hour = h
            target_str = f"{target_date}T{target_hour:02d}:"
            for ti, t in day_pairs:
                if ti.startswith(target_str):
                    return t
            return min(day_pairs, key=lambda x: abs(int(x[0][11:13]) - target_hour))[1]

    # ── 2. Daily max / min from Open-Meteo daily aggregation ─────────────────
    daily = forecast_data.get("daily", {})
    daily_times = daily.get("time", [])

    if daily_times:
        # daily_times are "YYYY-MM-DD" strings
        try:
            idx = daily_times.index(target_date)
        except ValueError:
            idx = None

        if idx is not None:
            is_low = any(kw in q for kw in (
                "lowest", "low", "minimum", "min", "overnight", "nightly", "coldest"
            ))
            is_avg = any(kw in q for kw in ("average", "mean", "avg"))

            if is_avg:
                # No daily average from Open-Meteo — fall through to hourly
                pass
            elif is_low:
                val = daily.get("temperature_2m_min", [None] * (idx + 1))[idx]
                if val is not None:
                    return float(val)
            else:
                # Default: daily high (covers "highest", "high", "max", generic buckets)
                val = daily.get("temperature_2m_max", [None] * (idx + 1))[idx]
                if val is not None:
                    return float(val)

    # ── 3. Fallback: compute from hourly data ─────────────────────────────────
    hourly = forecast_data.get("hourly", {})
    times  = hourly.get("time", [])
    temps  = hourly.get("temperature_2m", [])
    day_temps = [t for ti, t in zip(times, temps)
                 if ti and ti.startswith(target_date) and t is not None]
    if not day_temps:
        return None

    if any(kw in q for kw in ("average", "mean", "avg")):
        return sum(day_temps) / len(day_temps)
    if any(kw in q for kw in ("lowest", "low", "minimum", "min", "overnight", "nightly", "coldest")):
        return min(day_temps)
    return max(day_temps)


def _bucket_probability(
    forecast_ref: float,
    question: str,
    unit: str,
    is_low: bool = False,
) -> Optional[float]:
    """
    Compute P(temperature satisfies the market condition) using a Normal
    distribution centred on the Open-Meteo reference temperature.

    forecast_ref: the reference temperature (daily max, min, or hourly)
    std_dev: 2.7°F / 1.5°C — typical 24-hour ECMWF forecast error
    is_low: True for daily-low markets (slightly larger uncertainty)

    Handles:
      - "between X and Y" (bucket range)
      - "above / exceed / at least X" (exceedance)
      - "below / under / at most X" (lower tail)
      - bare threshold (default: exceedance for high markets, lower for low markets)
    """
    std_dev = (3.2 if is_low else 2.7) if unit == "F" else (1.8 if is_low else 1.5)
    q = question.lower()

    # "between X and Y" / "X–Y" / "X to Y" bucket
    between = re.search(
        r"between\s+(\d+(?:\.\d+)?)\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q
    )
    if not between:
        # "X-Y°F" format with no "between" keyword
        between = re.search(r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*°?\s*[fcFC]", question)
    if between:
        lo = float(between.group(1))
        hi = float(between.group(2))
        # Add 0.5-unit half-open interval to capture values right at the boundary
        prob = float(
            norm.cdf(hi + 0.5, loc=forecast_ref, scale=std_dev) -
            norm.cdf(lo - 0.5, loc=forecast_ref, scale=std_dev)
        )
        return max(0.01, min(0.99, prob))

    # Extract the numeric temperature threshold
    # Skip years (>1900) and percentages; temperature range is roughly -60 to 150°F / -50 to 60°C
    candidates = re.findall(r"-?\d+(?:\.\d+)?", question)
    threshold = None
    for c in candidates:
        val = float(c)
        if unit == "F" and -60 <= val <= 150:
            threshold = val
            break
        elif unit == "C" and -50 <= val <= 60:
            threshold = val
            break

    if threshold is None:
        return None

    has_above = any(kw in q for kw in ("exceed", "above", "over", "at least", "more than",
                                        "or higher", "or above", "or more", "high above"))
    has_below = any(kw in q for kw in ("below", "under", "at most", "less than", "or below",
                                        "or lower", "or fewer", "low below"))

    if has_above:
        # P(temp ≥ threshold)
        prob = float(1.0 - norm.cdf(threshold, loc=forecast_ref, scale=std_dev))
    elif has_below:
        # P(temp ≤ threshold)
        prob = float(norm.cdf(threshold, loc=forecast_ref, scale=std_dev))
    else:
        # Exact value: "be 16°C" / "be 72°F" — compute P(threshold±0.5)
        # This is the common Polymarket bucket format where each integer temp is its own market
        half = 0.5 if unit == "F" else 0.5
        prob = float(
            norm.cdf(threshold + half, loc=forecast_ref, scale=std_dev) -
            norm.cdf(threshold - half, loc=forecast_ref, scale=std_dev)
        )

    return max(0.01, min(0.99, prob))


def _disaster_probability(
    forecast_data: Dict[str, Any], target_date: str, question: str
) -> Optional[float]:
    """Estimate probability for categorical warning/disaster markets using Open-Meteo wind/precip."""
    hourly = forecast_data.get("hourly", {})
    times  = hourly.get("time", [])
    gusts  = hourly.get("windgusts_10m", [])
    precip = hourly.get("precipitation", [])
    q      = question.lower()

    day_gusts  = [g for ti, g in zip(times, gusts)
                  if ti and ti.startswith(target_date) and g is not None]
    day_precip = [p for ti, p in zip(times, precip)
                  if ti and ti.startswith(target_date) and p is not None]

    max_gust    = max(day_gusts)  if day_gusts  else 0.0
    total_precip = sum(day_precip) if day_precip else 0.0

    if "hurricane" in q:
        return max(0.01, min(0.99, float(1.0 - norm.cdf(74, loc=max_gust, scale=10.0))))
    if "tropical storm" in q:
        return max(0.01, min(0.99, float(1.0 - norm.cdf(39, loc=max_gust, scale=5.0))))
    if "red flag" in q or "fire" in q:
        return max(0.01, min(0.99, float(1.0 - norm.cdf(25, loc=max_gust, scale=5.0))))
    if "flood" in q:
        return max(0.01, min(0.99, float(1.0 - norm.cdf(3.0, loc=total_precip, scale=1.0))))

    wind_prob   = float(1.0 - norm.cdf(40, loc=max_gust, scale=10.0))
    precip_prob = float(1.0 - norm.cdf(2.0, loc=total_precip, scale=0.8))
    return max(0.01, min(0.99, max(wind_prob, precip_prob)))


async def compute_weather_alpha(
    question: str,
    _outcome_label: str,
    current_price: float,
) -> Optional[Tuple[float, str]]:
    """
    Compute alpha edge for a weather prediction market.

    Returns (edge, explanation):
      edge > 0 → YES underpriced (buy YES)
      edge < 0 → NO underpriced (buy NO)
    Returns None if the market cannot be scored.
    """
    # Skip precipitation quantity markets — resolution-mismatch risk too high
    weather_type = _detect_weather_type(question)
    if weather_type == "precipitation":
        return None

    city_result = _find_city(question)
    if not city_result:
        return None

    city_name, coords = city_result
    target_date = _find_date(question)
    if not target_date:
        return None

    # Detect unit: Fahrenheit vs Celsius
    unit = "F" if re.search(r"\bF\b|fahrenheit", question, re.IGNORECASE) else "C"

    # Check if target date is in the past
    utc_offset_h = round(coords["lon"] / 15.0)
    local_now    = _dt.now(_tz.utc) + _td(hours=utc_offset_h)
    local_today  = local_now.strftime("%Y-%m-%d")
    past_days    = 0

    if target_date < local_today:
        try:
            days_back = (_date.fromisoformat(local_today) - _date.fromisoformat(target_date)).days
        except ValueError:
            days_back = 99
        if days_back > 7:
            return None   # market should be resolved by now
        past_days = days_back + 1

    forecast_data = await _fetch_forecast(
        coords["lat"], coords["lon"], target_date,
        unit=unit, weather_type=weather_type, past_days=past_days,
    )
    if not forecast_data:
        return None

    # ------------------------------------------------------------------ #
    # Compute forecast probability for the market condition               #
    # ------------------------------------------------------------------ #
    forecast_prob: Optional[float] = None
    forecast_ref: Optional[float]  = None
    is_low: bool = False

    if weather_type == "disaster":
        forecast_prob = _disaster_probability(forecast_data, target_date, question)

    elif weather_type in ("wind", "storm"):
        hourly    = forecast_data.get("hourly", {})
        times     = hourly.get("time", [])
        gusts     = hourly.get("windgusts_10m", [])
        day_gusts = [g for ti, g in zip(times, gusts)
                     if ti and ti.startswith(target_date) and g is not None]
        if day_gusts:
            max_gust = max(day_gusts)
            wind_m   = re.search(r"(\d+(?:\.\d+)?)\s*(?:mph|knots|kph)", question, re.IGNORECASE)
            if wind_m:
                threshold = float(wind_m.group(1))
                std_dev   = max(2.0, max_gust * 0.15)
                forecast_prob = max(0.01, min(0.99, float(
                    1.0 - norm.cdf(threshold, loc=max_gust, scale=std_dev)
                )))

    else:
        # Temperature market: pick the right reference temp based on what the
        # market is asking (high, low, hourly, average) then compute probability
        q_lower = question.lower()
        is_low  = any(kw in q_lower for kw in ("low", "minimum", "overnight", "nightly", "coldest"))
        forecast_ref = _get_reference_temp(forecast_data, target_date, question)
        if forecast_ref is None:
            return None
        forecast_prob = _bucket_probability(forecast_ref, question, unit, is_low=is_low)

    if forecast_prob is None:
        return None

    # ------------------------------------------------------------------ #
    # Apply signal rules                                                  #
    # ------------------------------------------------------------------ #
    min_gap  = WEATHER_MIN_GAP_PP / 100.0
    gap      = forecast_prob - current_price          # positive = YES underpriced
    is_focus = _is_focus_city(city_name)
    source   = "actual" if past_days > 0 else "forecast"

    edge = 0.0
    signal_rule = ""

    # Simple rule: if model disagrees with market by more than min_gap,
    # signal YES (model > market) or NO (model < market). No special cases.
    if abs(gap) > min_gap:
        edge = gap
        direction   = "YES" if gap > 0 else "NO"
        signal_rule = f"{direction} (model={forecast_prob*100:.0f}%, market={current_price*100:.0f}%, gap={gap:+.2f})"

    if abs(edge) < 0.03:
        return None

    # Log warning when a signal fires on a non-focus city
    focus_tag = "" if is_focus else f" [non-focus city — lower confidence]"

    # Build the Open-Meteo model context line
    if weather_type == "temperature" and forecast_ref is not None:
        ref_label = "low" if is_low else "high"
        model_line = (
            f"Open-Meteo {source}: forecast {ref_label} {forecast_ref:.1f}°{unit} → "
            f"P(market condition) = {forecast_prob*100:.1f}%"
        )
    else:
        model_line = (
            f"Open-Meteo {source}: P(event) = {forecast_prob*100:.1f}%"
        )

    explanation = (
        f"[Weather {source.capitalize()}] {city_name}{focus_tag}: "
        f"{model_line} vs {current_price*100:.1f}% market price. "
        f"Rule: {signal_rule}. Edge={edge:+.3f}."
    )

    logger.info(f"[Weather] {city_name} {target_date} ({weather_type}): {explanation}")
    return edge, explanation


# ══════════════════════════════════════════════════════════════════════════════
# Weather Prediction Strategy — Open-Meteo Ensemble (51-model empirical)
# ══════════════════════════════════════════════════════════════════════════════
# Uses the Open-Meteo ensemble API which returns 40+ individual model members.
# Instead of assuming a Normal distribution, we count what fraction of models
# satisfy the market condition — giving a proper empirical probability.
#
# High-volume cities (concentrated scan):
HIGH_VOLUME_CITIES = {
    "New York", "NYC", "Shanghai", "Seoul", "London",
    "Paris", "Hong Kong", "Tokyo", "Buenos Aires",
    "Singapore", "Miami", "Los Angeles", "LA",
}

ENSEMBLE_API_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
_ENSEMBLE_CACHE: dict = {}
_ENSEMBLE_CACHE_TTL = 3600  # 60 min — refresh once per model-run cycle

# Temperature bucket mutual exclusivity:
# For each (city, date, unit) group of competing temperature buckets, only ONE
# bucket can resolve YES (mutually exclusive). The bucket whose condition the
# ensemble model favours most (highest forecast_prob) wins → YES.
# All other buckets in the group are forced → NO regardless of gap threshold.
# Key: (city_name_lower, target_date, unit) → winning question (lower-cased)
_BUCKET_WINNERS: dict = {}

# Primary: ECMWF IFS 51-member ensemble (industry standard, 6-hourly updates)
# Fallback: ICON seamless 40-member (global, free)
_ENSEMBLE_MODEL_PRIMARY  = "ecmwf_ifs025"
_ENSEMBLE_MODEL_FALLBACK = "icon_seamless"


async def _fetch_ensemble_forecast(
    lat: float,
    lon: float,
    target_date: str,
    unit: str,
    past_days: int = 0,
    model: str = _ENSEMBLE_MODEL_PRIMARY,
) -> Dict[str, Any]:
    """
    Fetch ECMWF 51-member (or ICON 40-member) ensemble from Open-Meteo.
    Cached for 60 minutes — aligned with model-run update cycle.
    Falls back to ICON seamless if ECMWF is unavailable.
    """
    key = ("ensemble", round(lat, 4), round(lon, 4), target_date, unit, past_days, model)
    entry = _ENSEMBLE_CACHE.get(key)
    if entry and (_time.monotonic() - entry["ts"]) < _ENSEMBLE_CACHE_TTL:
        return entry["data"]

    params: Dict[str, Any] = {
        "latitude":         lat,
        "longitude":        lon,
        "hourly":           "temperature_2m",
        "models":           model,   # ecmwf_ifs025 = 51 members; icon_seamless = 40 members
        "forecast_days":    1 if past_days > 0 else 16,
        "temperature_unit": "fahrenheit" if unit == "F" else "celsius",
        "timezone":         "auto",
    }
    if past_days > 0:
        params["past_days"] = past_days

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(ENSEMBLE_API_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            _ENSEMBLE_CACHE[key] = {"data": data, "ts": _time.monotonic()}
            return data
    except Exception as e:
        logger.warning(f"Open-Meteo ensemble fetch failed ({lat},{lon}) model={model}: {e}")
        # Fallback: try ICON seamless if ECMWF failed
        if model != _ENSEMBLE_MODEL_FALLBACK:
            logger.info(f"[WX-Ensemble] Falling back to {_ENSEMBLE_MODEL_FALLBACK}...")
            return await _fetch_ensemble_forecast(lat, lon, target_date, unit, past_days, _ENSEMBLE_MODEL_FALLBACK)
        return {}


def _extract_market_threshold(question: str, unit: str) -> Optional[float]:
    """
    Extract the primary temperature value the market is asking about.
    For "between X and Y" → midpoint; for bare "be X°" → X.
    Used to compute direct ensemble_mean vs market_threshold gap.
    """
    q = question.lower()
    between = re.search(
        r"between\s+(\d+(?:\.\d+)?)\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q
    )
    if not between:
        between = re.search(
            r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*°?\s*[fcFC]", question
        )
    if between:
        return (float(between.group(1)) + float(between.group(2))) / 2.0

    for c in re.findall(r"-?\d+(?:\.\d+)?", question):
        val = float(c)
        if unit == "F" and -60 <= val <= 150:
            return val
        elif unit == "C" and -50 <= val <= 60:
            return val
    return None


def _ensemble_daily_stats(
    ensemble_data: Dict[str, Any], target_date: str, use_min: bool = False
) -> list:
    """
    Extract daily max (or min for low-temp markets) for each ensemble member.
    Returns a list of floats (one per model member) for target_date.
    """
    hourly = ensemble_data.get("hourly", {})
    times  = hourly.get("time", [])
    member_keys = sorted(k for k in hourly if k.startswith("temperature_2m_member"))

    if not member_keys or not times:
        return []

    results = []
    for key in member_keys:
        vals = hourly.get(key, [])
        day_vals = [
            v for ti, v in zip(times, vals)
            if ti and ti.startswith(target_date) and v is not None
        ]
        if day_vals:
            results.append(min(day_vals) if use_min else max(day_vals))

    return results


# Keep old name as alias for backwards compatibility
def _ensemble_daily_maxes(ensemble_data: Dict[str, Any], target_date: str) -> list:
    return _ensemble_daily_stats(ensemble_data, target_date, use_min=False)


async def precompute_temperature_bucket_winners(markets: list) -> None:
    """
    Group temperature markets by (city, date, unit) and determine which bucket
    the ensemble model favours most for each group. Stores results in _BUCKET_WINNERS
    so that compute_ensemble_weather_alpha can force NO on losing buckets.

    Call this once before scoring a batch of markets.
    """
    from collections import defaultdict

    groups: dict = defaultdict(list)
    for m in markets:
        q = m.question or ""
        city_result = _find_city(q)
        if not city_result:
            continue
        target_date = _find_date(q)
        if not target_date:
            continue
        city_name, coords = city_result
        unit = "F" if re.search(r"\bF\b|fahrenheit", q, re.IGNORECASE) else "C"
        key = (city_name.lower(), target_date, unit)
        groups[key].append((m, coords, unit))

    _BUCKET_WINNERS.clear()

    for (city_lower, target_date, unit), group in groups.items():
        if len(group) < 2:
            continue  # single market — no competition, skip

        _, coords, _ = group[0]
        is_low = any(
            kw in (group[0][0].question or "").lower()
            for kw in ("low", "minimum", "min", "overnight", "nightly", "coldest")
        )

        ensemble_data = await _fetch_ensemble_forecast(
            coords["lat"], coords["lon"], target_date, unit, past_days=0
        )
        if not ensemble_data:
            continue

        member_temps = _ensemble_daily_stats(ensemble_data, target_date, use_min=is_low)
        if len(member_temps) < 5:
            continue

        best_prob = -1.0
        winner_q: Optional[str] = None
        for m, _, _ in group:
            prob = _ensemble_probability(member_temps, m.question or "", unit)
            if prob is not None and prob > best_prob:
                best_prob = prob
                winner_q = (m.question or "").lower()

        if winner_q:
            _BUCKET_WINNERS[(city_lower, target_date, unit)] = winner_q
            logger.info(
                f"[BucketWinner] {city_lower} {target_date} ({unit}): "
                f"winner='{winner_q[:60]}' (P={best_prob*100:.0f}%)"
            )


def _ensemble_probability(daily_maxes: list, question: str, unit: str) -> Optional[float]:
    """
    Compute empirical probability from ensemble member daily maxes.

    Fraction of models that satisfy the market condition — no distribution
    assumption, using actual meteorological model spread.
    """
    n = len(daily_maxes)
    if n == 0:
        return None

    q = question.lower()

    # "between X and Y" / "X-Y°F" bucket
    between = re.search(
        r"between\s+(\d+(?:\.\d+)?)\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q
    )
    if not between:
        between = re.search(
            r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*°?\s*[fcFC]", question
        )
    if between:
        lo = float(between.group(1))
        hi = float(between.group(2))
        count = sum(1 for t in daily_maxes if lo - 0.5 <= t <= hi + 0.5)
        return max(0.01, min(0.99, count / n))

    # Extract temperature threshold (skip years/unrealistic values)
    threshold = None
    for c in re.findall(r"-?\d+(?:\.\d+)?", question):
        val = float(c)
        if unit == "F" and -60 <= val <= 150:
            threshold = val
            break
        elif unit == "C" and -50 <= val <= 60:
            threshold = val
            break

    if threshold is None:
        return None

    has_above = any(kw in q for kw in (
        "exceed", "above", "over", "at least", "more than",
        "or higher", "or above", "or more", "high above"
    ))
    has_below = any(kw in q for kw in (
        "below", "under", "at most", "less than",
        "or below", "or lower", "or fewer"
    ))

    if has_above:
        count = sum(1 for t in daily_maxes if t >= threshold)
    elif has_below:
        count = sum(1 for t in daily_maxes if t <= threshold)
    else:
        # Exact bucket: "be 16°C" → fraction of models landing in ±0.5 band
        half  = 0.5
        count = sum(1 for t in daily_maxes if threshold - half <= t <= threshold + half)

    return max(0.01, min(0.99, count / n))


async def compute_ensemble_weather_alpha(
    question: str,
    current_price: float,
) -> Optional[Tuple[float, str]]:
    """
    Compute alpha edge for a temperature market using the Open-Meteo
    40-member ICON ensemble.

    Uses empirical probability (fraction of models satisfying the condition)
    rather than a Normal distribution assumption.

    Returns (edge, explanation) or None.
      edge > 0  → YES underpriced
      edge < 0  → NO underpriced
    """
    # Only temperature markets — no precipitation
    weather_type = _detect_weather_type(question)
    if weather_type not in ("temperature",):
        return None

    city_result = _find_city(question)
    if not city_result:
        return None

    city_name, coords = city_result
    target_date = _find_date(question)
    if not target_date:
        return None

    unit = "F" if re.search(r"\bF\b|fahrenheit", question, re.IGNORECASE) else "C"

    # Date check
    utc_offset_h = round(coords["lon"] / 15.0)
    local_now    = _dt.now(_tz.utc) + _td(hours=utc_offset_h)
    local_today  = local_now.strftime("%Y-%m-%d")
    past_days    = 0

    if target_date < local_today:
        try:
            days_back = (_date.fromisoformat(local_today) - _date.fromisoformat(target_date)).days
        except ValueError:
            days_back = 99
        if days_back > 7:
            return None
        past_days = days_back + 1

    # ── Fetch ensemble ────────────────────────────────────────────────────────
    is_low_market = any(kw in question.lower() for kw in (
        "low", "minimum", "min", "overnight", "nightly", "coldest"
    ))
    ensemble_data = await _fetch_ensemble_forecast(
        coords["lat"], coords["lon"], target_date, unit, past_days=past_days
    )
    if not ensemble_data:
        return None

    member_temps = _ensemble_daily_stats(ensemble_data, target_date, use_min=is_low_market)
    if len(member_temps) < 5:   # need enough members for meaningful probability
        return None

    forecast_prob = _ensemble_probability(member_temps, question, unit)
    if forecast_prob is None:
        return None

    n_members      = len(member_temps)
    ensemble_mean  = sum(member_temps) / n_members
    ensemble_spread = max(member_temps) - min(member_temps)
    source         = "historical" if past_days > 0 else "ensemble"
    is_focus       = city_name in HIGH_VOLUME_CITIES

    # ── Detect which model is being used (ECMWF = 51 members, ICON = 40) ──────
    model_label = "ECMWF-51" if n_members >= 50 else f"ICON-{n_members}"

    # ── Direct temperature gap: ensemble mean vs market's implied threshold ────
    # Core insight from strategy description: edge when forecast differs from
    # "market favorite" by 1-3°C. This catches cases where probability alone
    # is borderline but the temperature disagreement is clear.
    market_threshold = _extract_market_threshold(question, unit)
    temp_diff = 0.0
    if market_threshold is not None:
        temp_diff = ensemble_mean - market_threshold

    # ── Bucket mutual exclusivity ─────────────────────────────────────────────
    # When multiple temperature markets cover the same city+date (e.g. "70°F?",
    # "80°F?", "90°F?"), only one can resolve YES. precompute_temperature_bucket_winners()
    # determined which bucket the model favours most. Non-winners get a forced NO
    # regardless of gap size.
    focus_tag  = "" if is_focus else " [non-priority city]"
    ref_label  = "low" if is_low_market else "high"
    market_part = (
        f"Market implies {market_threshold:.1f}{unit} (diff={temp_diff:+.1f}{unit})"
        if market_threshold is not None else "Market threshold N/A"
    )

    winner_key = (city_name.lower(), target_date, unit)
    if winner_key in _BUCKET_WINNERS:
        winner_q = _BUCKET_WINNERS[winner_key]
        this_q   = question.lower()
        if this_q != winner_q:
            # This bucket is a loser — force NO if market overprices it
            forced_edge = forecast_prob - current_price  # will be negative
            if forced_edge >= -0.03:
                # Market already prices this bucket correctly (near zero) — skip
                return None
            explanation = (
                f"[WX Bucket Exclusivity{focus_tag}] {city_name} {target_date} | "
                f"{model_label}: forecast {ref_label} {ensemble_mean:.1f}{unit} "
                f"({n_members} members) | "
                f"Model favours a different bucket — this bucket loses → NO. "
                f"P={forecast_prob*100:.1f}% vs {current_price*100:.1f}% priced. "
                f"Edge={forced_edge:+.3f}."
            )
            logger.info(f"[WX-Bucket-NO] {city_name} '{question[:50]}': {explanation}")
            return forced_edge, explanation

    # ── Signal rules (winner bucket or ungrouped market) ─────────────────────
    min_gap   = WEATHER_MIN_GAP_PP / 100.0
    gap       = forecast_prob - current_price

    edge        = 0.0
    signal_rule = ""

    # Simple rule: model disagrees with market by more than min_gap →
    # YES if model > market, NO if model < market. No special cases.
    if abs(gap) > min_gap:
        edge        = gap
        direction   = "YES" if gap > 0 else "NO"
        signal_rule = f"{direction} (P={forecast_prob*100:.0f}%, market={current_price*100:.0f}%, gap={gap:+.2f})"

    if abs(edge) < 0.03:
        return None

    explanation = (
        f"[WX Prediction{focus_tag}] {city_name} {target_date} | "
        f"{model_label} {source}: forecast {ref_label} {ensemble_mean:.1f}{unit} "
        f"(+/-{ensemble_spread/2:.1f}, {n_members} members) | "
        f"{market_part} | "
        f"P(condition)={forecast_prob*100:.1f}% vs {current_price*100:.1f}% priced | "
        f"Rule: {signal_rule}. Edge={edge:+.3f}."
    )

    logger.info(f"[WX-Ensemble] {city_name}: {explanation}")
    return edge, explanation
