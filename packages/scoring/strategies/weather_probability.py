"""
Weather / Meteorological Alpha Signal
======================================
Data sources used per strategy and geography:

  Temperature markets (weather_prediction, laddering):
    PRIMARY  — Open-Meteo Ensemble API, ECMWF IFS 51-member (global, 15-day, 0.25°)
    FALLBACK1 — Open-Meteo Ensemble API, NOAA GEFS 31-member (global, 16-day, 0.25°)
    FALLBACK2 — Open-Meteo Ensemble API, DWD ICON-EPS 40-member (global, 7.5-day)
    US CROSS-CHECK — NOAA NWS api.weather.gov deterministic forecast (US cities only,
                      free/no-key, 7-day hourly). Used to validate Open-Meteo direction
                      for US markets; if NWS and ensemble disagree, signal strength is
                      reduced 20%.

  Disaster markets (wind/precipitation events):
    Open-Meteo standard forecast API (single-model, hourly wind gusts + precipitation).

  Seismic markets:
    USGS Earthquake Catalog API (live, real-time, no key) — see market_aggregation.py.

NOAA GEFS is accessed via Open-Meteo's ensemble wrapper (model=gefs025), NOT via
raw GRIB2 files. This avoids S3/GRIB2 infrastructure while still using NOAA's
31-member probabilistic output.

NOAA NWS covers US territory only (continental + AK + HI). For all non-US cities
the NWS cross-check step is skipped automatically.
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

OPEN_METEO_URL          = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# In-memory forecast cache — 30 min TTL
_FORECAST_CACHE: dict = {}
_CACHE_TTL = 1800

# Precipitation bucket mutual-exclusivity tracker (city, year, month, unit) → winning question
_PRECIP_WINNERS: dict = {}

_MONTH_NAMES: Dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


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
        r"between\s+(\d+(?:\.\d+)?)\s*°?\s*[fcFC]?\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q
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
    unit = "F" if re.search(r"fahrenheit|\bF\b|\d\s*°?\s*F\b", question, re.IGNORECASE) else "C"

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
NWS_API_URL      = "https://api.weather.gov"

_ENSEMBLE_CACHE: dict = {}
_NWS_CACHE: dict      = {}
_ENSEMBLE_CACHE_TTL   = 3600   # 60 min — aligned with model-run cycle
_NWS_CACHE_TTL        = 3600   # NWS updates every 6 h; 60 min is safe

# Temperature bucket mutual exclusivity:
# For each (city, date, unit) group of competing temperature buckets, only ONE
# bucket can resolve YES (mutually exclusive). The bucket whose condition the
# ensemble model favours most (highest forecast_prob) wins → YES.
# All other buckets in the group are forced → NO regardless of gap threshold.
# Key: (city_name_lower, target_date, unit) → winning question (lower-cased)
_BUCKET_WINNERS: dict = {}

# Ensemble model cascade (tried in order until one succeeds):
#   1. ECMWF IFS 0.25° — 51 members, global, 15-day (industry gold standard)
#   2. NOAA GEFS 0.25° — 31 members, global, 16-day (accessed via Open-Meteo)
#   3. DWD ICON-EPS    — 40 members, global, 7.5-day (European model, last resort)
_ENSEMBLE_MODEL_PRIMARY   = "ecmwf_ifs025"
_ENSEMBLE_MODEL_FALLBACK1 = "gefs025"        # NOAA GEFS 31-member via Open-Meteo
_ENSEMBLE_MODEL_FALLBACK2 = "icon_seamless"  # DWD ICON 40-member

# US cities whose lat/lon is within NOAA NWS coverage (continental US + AK + HI).
# NWS is used as a CROSS-CHECK for these cities: if NWS deterministic forecast
# disagrees with the ensemble direction, signal strength is reduced by 20%.
_NWS_CITIES: frozenset = frozenset({
    "New York", "NYC", "Los Angeles", "LA", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Diego", "Dallas", "Miami", "Atlanta", "Seattle",
    "Boston", "Denver", "Las Vegas", "Washington", "San Francisco", "SF",
})


async def _fetch_nws_hourly(lat: float, lon: float) -> Dict[str, Any]:
    """
    Fetch NOAA NWS hourly forecast for a US lat/lon.
    Two-step: /points → gridpoint URL → /forecast/hourly
    Returns {"time": [...], "temperature": [...], "unit": "F"} or {}
    NWS only covers US territory — returns {} for non-US coords.
    """
    key = ("nws", round(lat, 3), round(lon, 3))
    entry = _NWS_CACHE.get(key)
    if entry and (_time.monotonic() - entry["ts"]) < _NWS_CACHE_TTL:
        return entry["data"]

    headers = {"User-Agent": "PolymarketAlphaBot (polymarket-alpha@localhost)"}
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            # Step 1: resolve grid point
            r1 = await client.get(
                f"{NWS_API_URL}/points/{lat:.4f},{lon:.4f}",
                headers=headers,
            )
            if r1.status_code != 200:
                return {}
            props    = r1.json().get("properties", {})
            hourly_url = props.get("forecastHourly")
            if not hourly_url:
                return {}

            # Step 2: fetch hourly forecast
            r2 = await client.get(hourly_url, headers=headers)
            if r2.status_code != 200:
                return {}
            periods = r2.json().get("properties", {}).get("periods", [])

        data = {
            "time":        [p["startTime"][:10] for p in periods],  # YYYY-MM-DD
            "temperature": [p["temperature"]     for p in periods],
            "unit":        "F" if (periods[0]["temperatureUnit"] == "F" if periods else True) else "C",
            "_periods":    periods,  # keep full records for hourly lookup
        }
        _NWS_CACHE[key] = {"data": data, "ts": _time.monotonic()}
        return data
    except Exception as e:
        logger.debug(f"[NWS] Fetch failed ({lat},{lon}): {e}")
        return {}


def _nws_daily_max(nws_data: Dict[str, Any], target_date: str) -> Optional[float]:
    """Return the NWS predicted daily high for target_date, or None."""
    periods = nws_data.get("_periods", [])
    day_temps = [
        p["temperature"] for p in periods
        if p.get("startTime", "").startswith(target_date) and p.get("isDaytime", True)
    ]
    if not day_temps:
        # Fallback: any hour for that date
        day_temps = [
            p["temperature"] for p in periods
            if p.get("startTime", "").startswith(target_date)
        ]
    return max(day_temps) if day_temps else None


async def _fetch_ensemble_forecast(
    lat: float,
    lon: float,
    target_date: str,
    unit: str,
    past_days: int = 0,
    model: str = _ENSEMBLE_MODEL_PRIMARY,
) -> Dict[str, Any]:
    """
    Fetch ensemble forecast from Open-Meteo.
    Cascade order: ECMWF IFS (51) → NOAA GEFS (31) → DWD ICON (40).
    Cached 60 minutes per model/location.
    """
    key = ("ensemble", round(lat, 4), round(lon, 4), target_date, unit, past_days, model)
    entry = _ENSEMBLE_CACHE.get(key)
    if entry and (_time.monotonic() - entry["ts"]) < _ENSEMBLE_CACHE_TTL:
        return entry["data"]

    params: Dict[str, Any] = {
        "latitude":         lat,
        "longitude":        lon,
        "hourly":           "temperature_2m",
        "models":           model,
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
        logger.warning(f"[WX-Ensemble] {model} failed ({lat:.2f},{lon:.2f}): {e}")
        # Cascade to next fallback
        if model == _ENSEMBLE_MODEL_PRIMARY:
            logger.info("[WX-Ensemble] ECMWF failed — trying NOAA GEFS...")
            return await _fetch_ensemble_forecast(
                lat, lon, target_date, unit, past_days, _ENSEMBLE_MODEL_FALLBACK1
            )
        if model == _ENSEMBLE_MODEL_FALLBACK1:
            logger.info("[WX-Ensemble] GEFS failed — trying DWD ICON...")
            return await _fetch_ensemble_forecast(
                lat, lon, target_date, unit, past_days, _ENSEMBLE_MODEL_FALLBACK2
            )
        return {}


def _extract_market_threshold(question: str, unit: str) -> Optional[float]:
    """
    Extract the primary temperature value the market is asking about.
    For "between X and Y" → midpoint; for bare "be X°" → X.
    Used to compute direct ensemble_mean vs market_threshold gap.
    """
    q = question.lower()
    between = re.search(
        r"between\s+(\d+(?:\.\d+)?)\s*°?\s*[fcFC]?\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q
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
        unit = "F" if re.search(r"fahrenheit|\bF\b|\d\s*°?\s*F\b", q, re.IGNORECASE) else "C"
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
        r"between\s+(\d+(?:\.\d+)?)\s*°?\s*[fcFC]?\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q
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


async def compute_ladder_weather_alpha(
    question: str,
    current_price: float,
) -> Optional[Tuple[float, str]]:
    """
    Laddering scorer for temperature bucket markets.

    For a set of competing buckets for city X (e.g. "Will X be 70°F?", "80°F?", "90°F?"):
      - YES on the ONE bucket whose threshold is within 2°F / 1°C of the ensemble mean
      - NO on ALL other buckets (they cannot resolve YES — mutual exclusivity)

    One YES, rest NO. Never YES on adjacent buckets.
    """
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

    unit = "F" if re.search(r"fahrenheit|\bF\b|\d\s*°?\s*F\b", question, re.IGNORECASE) else "C"

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

    is_low_market = any(kw in question.lower() for kw in (
        "low", "minimum", "min", "overnight", "nightly", "coldest"
    ))
    ensemble_data = await _fetch_ensemble_forecast(
        coords["lat"], coords["lon"], target_date, unit, past_days=past_days
    )
    if not ensemble_data:
        return None

    member_temps = _ensemble_daily_stats(ensemble_data, target_date, use_min=is_low_market)
    if len(member_temps) < 5:
        return None

    n_members     = len(member_temps)
    ensemble_mean = sum(member_temps) / n_members
    ensemble_spread = max(member_temps) - min(member_temps)
    source        = "historical" if past_days > 0 else "ensemble"
    if n_members >= 50:
        model_label = "ECMWF-51"
    elif n_members >= 30:
        model_label = f"GEFS-{n_members}"
    else:
        model_label = f"ICON-{n_members}"
    is_focus  = city_name in HIGH_VOLUME_CITIES
    focus_tag = "" if is_focus else " [non-priority city]"
    ref_label = "low" if is_low_market else "high"

    # This bucket's threshold
    market_threshold = _extract_market_threshold(question, unit)
    if market_threshold is None:
        return None

    temp_diff = ensemble_mean - market_threshold  # positive = forecast above bucket
    abs_diff  = abs(temp_diff)

    # Match tolerance: 2°F or 1°C — if forecast is this close to the bucket, it's the winner
    match_tol = 2.0 if unit == "F" else 1.0

    forecast_prob = _ensemble_probability(member_temps, question, unit)
    if forecast_prob is None:
        return None

    min_gap = WEATHER_MIN_GAP_PP / 100.0
    gap     = forecast_prob - current_price

    if abs_diff <= match_tol:
        # Forecast matches this bucket — signal YES
        if gap < min_gap:
            return None  # market already correctly priced
        edge = gap
        rule = (
            f"FORECAST MATCH -> YES "
            f"(ensemble {ensemble_mean:.1f}{unit}, bucket {market_threshold:.1f}{unit}, "
            f"diff={temp_diff:+.1f}{unit}, P={forecast_prob*100:.0f}%, "
            f"market={current_price*100:.0f}%)"
        )
    else:
        # Forecast clearly misses this bucket — signal NO on the loser
        if gap >= -0.03:
            return None  # market already near-zero priced — no edge to fade
        edge = gap
        rule = (
            f"FORECAST MISMATCH -> NO "
            f"(ensemble {ensemble_mean:.1f}{unit}, bucket {market_threshold:.1f}{unit}, "
            f"diff={temp_diff:+.1f}{unit}, P={forecast_prob*100:.0f}%, "
            f"market={current_price*100:.0f}%)"
        )

    if abs(edge) < 0.03:
        return None

    explanation = (
        f"[Ladder{focus_tag}] {city_name} {target_date} | "
        f"{model_label} {source}: forecast {ref_label} {ensemble_mean:.1f}{unit} "
        f"(+/-{ensemble_spread/2:.1f}, {n_members} members) | "
        f"Bucket {market_threshold:.1f}{unit} (diff={temp_diff:+.1f}{unit}) | "
        f"P(condition)={forecast_prob*100:.1f}% vs {current_price*100:.1f}% priced | "
        f"Rule: {rule}. Edge={edge:+.3f}."
    )
    logger.info(f"[Ladder] {city_name} '{question[:50]}': {explanation}")
    return edge, explanation


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

    unit = "F" if re.search(r"fahrenheit|\bF\b|\d\s*°?\s*F\b", question, re.IGNORECASE) else "C"

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

    # ── Detect which model is being used ─────────────────────────────────────
    if n_members >= 50:
        model_label = "ECMWF-51"
    elif n_members >= 30:
        model_label = f"GEFS-{n_members}"    # NOAA GEFS via Open-Meteo
    else:
        model_label = f"ICON-{n_members}"   # DWD ICON fallback

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

    # ── NOAA NWS cross-check (US cities only) ────────────────────────────────
    # NWS provides an independent deterministic forecast from NOAA's operational
    # models. If NWS daily high disagrees with the ensemble direction (i.e. NWS
    # says the temperature is ABOVE the market threshold but ensemble says NO, or
    # vice versa) we reduce signal strength by 20% to reflect the disagreement.
    # NWS is US-only; the check is silently skipped for non-US cities.
    nws_note = ""
    if city_name in _NWS_CITIES and past_days == 0 and market_threshold is not None:
        nws_data = await _fetch_nws_hourly(coords["lat"], coords["lon"])
        if nws_data:
            nws_max = _nws_daily_max(nws_data, target_date)
            if nws_max is not None:
                # ensemble says YES → expects temp >= threshold; NWS says below → disagreement
                # ensemble says NO  → expects temp < threshold; NWS says above → disagreement
                ensemble_direction = "YES" if forecast_prob > current_price else "NO"
                nws_direction      = "YES" if nws_max >= market_threshold else "NO"
                if ensemble_direction != nws_direction:
                    nws_note = (
                        f" | NWS cross-check DISAGREES (NWS high={nws_max:.1f}{unit}, "
                        f"threshold={market_threshold:.1f}{unit}) — strength -20%"
                    )
                    logger.info(
                        f"[NWS] {city_name} {target_date}: ensemble={ensemble_direction} "
                        f"vs NWS={nws_direction} (NWS max={nws_max:.1f}) — reducing strength"
                    )
                else:
                    nws_note = f" | NWS confirms: {nws_max:.1f}{unit} vs threshold {market_threshold:.1f}{unit}"

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

    # Apply NWS disagreement penalty
    if "DISAGREES" in nws_note:
        edge = round(edge * 0.80, 4)
        if abs(edge) < 0.03:
            return None

    explanation = (
        f"[WX Prediction{focus_tag}] {city_name} {target_date} | "
        f"{model_label} {source}: forecast {ref_label} {ensemble_mean:.1f}{unit} "
        f"(+/-{ensemble_spread/2:.1f}, {n_members} members) | "
        f"{market_part}{nws_note} | "
        f"P(condition)={forecast_prob*100:.1f}% vs {current_price*100:.1f}% priced | "
        f"Rule: {signal_rule}. Edge={edge:+.3f}."
    )

    logger.info(f"[WX-Ensemble] {city_name}: {explanation}")
    return edge, explanation


# ══════════════════════════════════════════════════════════════════════════════
# Precipitation Strategy — Open-Meteo Historical Forecast API
# Monthly accumulation markets ("Will city X have Y-Z inches of precip in April?")
# ══════════════════════════════════════════════════════════════════════════════

def _find_month_year(question: str) -> Optional[Tuple[int, int]]:
    """
    Extract (year, month_int) from a monthly precipitation question.
    Handles "in April 2026", "in April", "precipitation in April".
    """
    q = question.lower()
    month_pat = (
        r"(january|february|march|april|may|june|july|"
        r"august|september|october|november|december)"
    )
    # "in April 2026" or "April 2026"
    m = re.search(month_pat + r"\s+(\d{4})", q)
    if m:
        return int(m.group(2)), _MONTH_NAMES[m.group(1)]
    # "in April" — infer year from current date
    m = re.search(r"\bin\s+" + month_pat, q)
    if m:
        month_n = _MONTH_NAMES[m.group(1)]
        today = _dt.utcnow()
        if month_n < today.month:
            return today.year + 1, month_n
        return today.year, month_n
    return None


def _parse_precip_threshold(question: str) -> Optional[Dict[str, Any]]:
    """
    Parse precipitation threshold from question text.
    Returns {"type": "between"|"above"|"below", "lo": float, "hi": float}.
    Values are in whatever unit the question uses (mm or inches).
    """
    q = question.lower()

    # "between X and Y" / "X-Y mm" / "X-Y inches"
    between = re.search(r"between\s+(\d+(?:\.\d+)?)\s*°?\s*[fcFC]?\s*(?:and|-|to)\s*(\d+(?:\.\d+)?)", q)
    if not between:
        between = re.search(
            r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*(?:mm\b|inches?\b|in\b)", q
        )
    if between:
        return {"type": "between", "lo": float(between.group(1)), "hi": float(between.group(2))}

    # "more than X" / "X or more" / "X mm or more" / "190mm or more"
    above = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:mm|inches?)?\s*(?:or more|or above|and above)", q
    )
    if not above:
        above = re.search(r"(?:more than|above|at least)\s+(\d+(?:\.\d+)?)", q)
    if above:
        return {"type": "above", "lo": float(above.group(1)), "hi": float("inf")}

    # "less than X" / "below X" / "under X"
    below = re.search(r"(?:less than|below|at most|or less|or below|under)\s+(\d+(?:\.\d+)?)", q)
    if below:
        return {"type": "below", "lo": 0.0, "hi": float(below.group(1))}

    return None


async def _fetch_monthly_precipitation(
    lat: float, lon: float, year: int, month: int, precip_unit: str
) -> Optional[float]:
    """
    Fetch total monthly precipitation from Open-Meteo.

    Splits the request so each API gets only the dates it supports:
      - historical-forecast-api: past dates (up to yesterday)
      - forecast API (api.open-meteo.com): today and future dates (up to 16 days)

    Returns the combined sum in the requested unit ("mm" or "inch").
    """
    import calendar
    _, last_day = calendar.monthrange(year, month)
    month_start = _date(year, month, 1)
    month_end   = _date(year, month, last_day)
    today       = _dt.now(_tz.utc).date()
    yesterday   = today - _td(days=1)

    key = ("precip_monthly", round(lat, 4), round(lon, 4), year, month, precip_unit)
    cached = _get_cached(key)
    if cached is not None:
        return cached

    base_params: Dict[str, Any] = {
        "latitude":           lat,
        "longitude":          lon,
        "daily":              "precipitation_sum",
        "timezone":           "auto",
        "precipitation_unit": precip_unit,
    }

    total = 0.0
    got_any = False

    async with httpx.AsyncClient(timeout=15.0) as client:
        # ── Past portion: historical-forecast-api (start of month → yesterday) ──
        hist_end = min(yesterday, month_end)
        if hist_end >= month_start:
            params = {
                **base_params,
                "start_date": str(month_start),
                "end_date":   str(hist_end),
            }
            try:
                resp = await client.get(HISTORICAL_FORECAST_URL, params=params)
                resp.raise_for_status()
                sums  = resp.json().get("daily", {}).get("precipitation_sum", [])
                total += sum(v for v in sums if v is not None)
                got_any = True
            except Exception as e:
                logger.warning(f"[Precip] Historical fetch failed ({lat:.2f},{lon:.2f}): {e}")

        # ── Future portion: forecast API (today → end of month, max 16 days) ──
        fc_start = max(today, month_start)
        fc_end   = min(month_end, today + _td(days=15))
        if fc_start <= month_end and fc_start <= fc_end:
            params = {
                **base_params,
                "start_date": str(fc_start),
                "end_date":   str(fc_end),
            }
            try:
                resp = await client.get(OPEN_METEO_URL, params=params)
                resp.raise_for_status()
                sums  = resp.json().get("daily", {}).get("precipitation_sum", [])
                total += sum(v for v in sums if v is not None)
                got_any = True
            except Exception as e:
                logger.warning(f"[Precip] Forecast fetch failed ({lat:.2f},{lon:.2f}): {e}")

    if not got_any:
        return None

    _set_cached(key, total)
    return total


async def precompute_precipitation_bucket_winners(markets: list) -> None:
    """
    Group monthly precipitation markets by (city, year, month, unit) and determine
    which bucket the expected monthly total favours most. Stores in _PRECIP_WINNERS
    so that compute_precipitation_alpha can apply mutual exclusivity.
    Call this once before the scoring loop — same pattern as
    precompute_temperature_bucket_winners.
    """
    from collections import defaultdict

    _PRECIP_WINNERS.clear()
    groups: dict = defaultdict(list)

    for m in markets:
        q = m.question or ""
        if not any(kw in q.lower() for kw in ("precipitation", "rainfall", "inches of rain", "mm of rain")):
            continue
        city_result = _find_city(q)
        if not city_result:
            continue
        month_year = _find_month_year(q)
        if not month_year:
            continue
        city_name, coords = city_result
        year, month    = month_year
        precip_unit    = "mm" if "mm" in q.lower() else "inch"
        key            = (city_name.lower(), year, month, precip_unit)
        groups[key].append((m, coords, precip_unit))

    for (city_lower, year, month, precip_unit), group in groups.items():
        if len(group) < 2:
            continue
        _, coords, _ = group[0]
        total = await _fetch_monthly_precipitation(
            coords["lat"], coords["lon"], year, month, precip_unit
        )
        if total is None:
            continue

        # Tiny sigma just to break ties at bucket boundaries
        sigma      = max(1.0 if precip_unit == "mm" else 0.05, total * 0.03)
        best_prob  = -1.0
        winner_q: Optional[str] = None

        for m, _, _ in group:
            thr = _parse_precip_threshold(m.question or "")
            if thr is None:
                continue
            if thr["type"] == "between":
                prob = float(
                    norm.cdf(thr["hi"], loc=total, scale=sigma) -
                    norm.cdf(thr["lo"], loc=total, scale=sigma)
                )
            elif thr["type"] == "above":
                prob = float(1.0 - norm.cdf(thr["lo"], loc=total, scale=sigma))
            else:
                prob = float(norm.cdf(thr["hi"], loc=total, scale=sigma))
            if prob > best_prob:
                best_prob = prob
                winner_q  = (m.question or "").lower()

        if winner_q:
            _PRECIP_WINNERS[(city_lower, year, month, precip_unit)] = winner_q
            logger.info(
                f"[PrecipWinner] {city_lower} {year}-{month:02d} ({precip_unit}): "
                f"expected={total:.1f} → winner='{winner_q[:60]}' (P={best_prob*100:.0f}%)"
            )


async def compute_precipitation_alpha(
    question: str,
    current_price: float,
) -> Optional[Tuple[float, str]]:
    """
    Compute alpha edge for monthly precipitation bucket markets.

    Uses the Open-Meteo historical-forecast API which returns both past actuals
    and remaining-month forecast days in one call, giving an up-to-date expected
    monthly total.  A normal distribution centred on that total (σ proportional to
    remaining forecast days) converts the expected total into a market probability.

    Returns (edge, explanation) or None.
      edge > 0  → YES underpriced (buy YES)
      edge < 0  → NO underpriced (buy NO)
    """
    q = question.lower()
    # Only handle precipitation markets
    if not any(kw in q for kw in ("precipitation", "rainfall", "inches of rain", "mm of rain")):
        return None

    city_result = _find_city(question)
    if not city_result:
        return None
    city_name, coords = city_result

    month_year = _find_month_year(question)
    if not month_year:
        return None
    year, month = month_year

    precip_unit = "mm" if "mm" in q else "inch"
    unit_label  = "mm" if precip_unit == "mm" else "in"

    thr = _parse_precip_threshold(question)
    if thr is None:
        return None

    total = await _fetch_monthly_precipitation(
        coords["lat"], coords["lon"], year, month, precip_unit
    )
    if total is None:
        return None

    # Days remaining in the month — drives forecast uncertainty
    import calendar
    from datetime import date as _date_cls
    _, last_day    = calendar.monthrange(year, month)
    today          = _dt.utcnow().date()
    month_end      = _date_cls(year, month, last_day)
    days_remaining = max(0, (month_end - today).days)

    # σ: std of remaining precipitation sum.  Typical daily precip std ~3 mm / 0.12 in.
    # σ_sum ≈ σ_daily × √n_remaining_days
    daily_std = 3.0 if precip_unit == "mm" else 0.12
    sigma     = max(daily_std, daily_std * (days_remaining ** 0.5))

    # P(monthly total satisfies the market condition)
    if thr["type"] == "between":
        prob = float(
            norm.cdf(thr["hi"], loc=total, scale=sigma) -
            norm.cdf(thr["lo"], loc=total, scale=sigma)
        )
    elif thr["type"] == "above":
        prob = float(1.0 - norm.cdf(thr["lo"], loc=total, scale=sigma))
    else:
        prob = float(norm.cdf(thr["hi"], loc=total, scale=sigma))
    prob = max(0.01, min(0.99, prob))

    # Mutual exclusivity — force NO on losing buckets
    winner_key = (city_name.lower(), year, month, precip_unit)
    is_loser   = (
        winner_key in _PRECIP_WINNERS
        and question.lower() != _PRECIP_WINNERS[winner_key]
    )

    min_gap = WEATHER_MIN_GAP_PP / 100.0
    gap     = prob - current_price

    if is_loser:
        if gap >= -0.03:
            return None  # market already near-zero priced — no edge
        edge = gap
        rule = f"LOSER → NO (P={prob*100:.1f}% vs market={current_price*100:.1f}%)"
    else:
        if abs(gap) <= min_gap:
            return None
        edge      = gap
        direction = "YES" if gap > 0 else "NO"
        rule      = f"{direction} (P={prob*100:.1f}%, market={current_price*100:.1f}%, gap={gap:+.2f})"

    if abs(edge) < 0.03:
        return None

    status = "complete" if days_remaining == 0 else f"{days_remaining}d remaining"
    if thr["type"] == "between":
        thr_str = f"{thr['lo']}-{thr['hi']}{unit_label}"
    elif thr["type"] == "above":
        thr_str = f"≥{thr['lo']}{unit_label}"
    else:
        thr_str = f"<{thr['hi']}{unit_label}"

    explanation = (
        f"[Precip] {city_name} {year}-{month:02d} | "
        f"Open-Meteo accumulated={total:.1f}{unit_label} ({status}, sigma={sigma:.1f}) | "
        f"Condition {thr_str} → P={prob*100:.1f}% vs {current_price*100:.1f}% market | "
        f"Rule: {rule}. Edge={edge:+.3f}."
    )
    logger.info(f"[Precip] {city_name} {year}-{month:02d}: {explanation}")
    return edge, explanation
