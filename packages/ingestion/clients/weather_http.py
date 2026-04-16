import logging
from typing import Any, Dict, Optional
from datetime import datetime

import httpx
import numpy as np
from scipy.stats import norm

logger = logging.getLogger(__name__)

class OpenMeteoClient:
    """
    Ported from legacy V1. Fetches temperature forecasts and computes 
    probability distributions for weather markets.
    """
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    @staticmethod
    async def get_temperature_distribution(lat: float, lon: float, target_date: str, unit: str = "C") -> Dict[str, float]:
        """
        Fetches temperature data and returns a probability distribution.
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "forecast_days": 14,
            "temperature_unit": "fahrenheit" if unit == "F" else "celsius",
            "timezone": "UTC"
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(OpenMeteoClient.BASE_URL, params=params)
                resp.raise_for_status()
                
                data = resp.json()
                hourly = data.get("hourly", {})
                times = hourly.get("time", [])
                temps = hourly.get("temperature_2m", [])

                # Filter temps for target date (e.g., "2024-07-20")
                day_temps = [t for time, t in zip(times, temps) if time.startswith(target_date)]
                if not day_temps:
                    logger.warning(f"No forecast data for {target_date}")
                    return {}

                daily_max = max(day_temps)
                
                # Standard deviation for probability (V1 heuristic)
                std_dev = 1.5 if unit == "C" else 2.7
                
                buckets = {}
                center = round(daily_max)
                # Generate buckets around the daily max (+/- 10 degrees)
                for b_start in range(center - 10, center + 10):
                    b_end = b_start + 1
                    prob = norm.cdf(b_end, loc=daily_max, scale=std_dev) - norm.cdf(b_start, loc=daily_max, scale=std_dev)
                    if prob > 0.001:
                        label = f"{b_start}-{b_end}{unit}"
                        buckets[label] = round(float(prob), 4)
                
                return buckets

        except Exception as e:
            logger.error(f"Failed to fetch Open-Meteo data: {e}")
            return {}
