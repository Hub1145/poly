import json
import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


def normalize_gamma_event(
    raw_event: Dict[str, Any],
) -> Tuple[SimpleNamespace, List[SimpleNamespace], List[SimpleNamespace], List[SimpleNamespace], List[Dict[str, Any]]]:
    """Normalize a Gamma event payload into plain SimpleNamespace objects.

    Returns:
        (event, markets, outcomes, tags, price_data)

        price_data is a list of dicts:
            {"market_id": ..., "outcome_name": ..., "price": float}
        These are stored as price_snapshots rows by the caller once the DB
        outcome_id is known.
    """
    event = SimpleNamespace(
        id=raw_event["id"],
        title=raw_event["title"],
        description=raw_event.get("description"),
        category=raw_event.get("category"),
        active=raw_event.get("active", True),
        closed=raw_event.get("closed", False),
    )

    markets: List[SimpleNamespace] = []
    all_outcomes: List[SimpleNamespace] = []
    all_tags: List[SimpleNamespace] = []
    all_prices: List[Dict[str, Any]] = []

    for raw_market in raw_event.get("markets", []):
        # Gamma API uses endDateIso (date-only) or endDate (datetime) —
        # normalize to a datetime so the DB column is consistently populated.
        _raw_end = (
            raw_market.get("endDateIso")
            or raw_market.get("endDate")
            or raw_market.get("end_date_iso")
        )
        if _raw_end:
            _raw_end = str(_raw_end).replace("Z", "+00:00")
            try:
                _end_dt = datetime.fromisoformat(_raw_end)
            except ValueError:
                try:
                    _end_dt = datetime.strptime(_raw_end, "%Y-%m-%d")
                except ValueError:
                    _end_dt = None
        else:
            _end_dt = None

        market = SimpleNamespace(
            id=raw_market["id"],
            event_id=event.id,
            question=raw_market["question"],
            slug=raw_market["slug"],
            active=raw_market.get("active", True),
            closed=raw_market.get("closed", False),
            resolution_source=raw_market.get("resolution_source") or raw_market.get("resolutionSource"),
            end_date_iso=_end_dt,
            market_type=(raw_market.get("market_type") or raw_market.get("marketType") or "binary").lower(),
        )
        markets.append(market)

        # ------------------------------------------------------------------ #
        # Outcomes + CLOB token IDs                                           #
        # The Gamma API encodes both fields as JSON-encoded strings:          #
        #   "outcomes":     "[\"Yes\", \"No\"]"                               #
        #   "clobTokenIds": "[\"21742...\",\"48331...\"]"                     #
        # ------------------------------------------------------------------ #
        raw_outcomes_field = raw_market.get("outcomes", [])
        if isinstance(raw_outcomes_field, str):
            try:
                raw_outcomes: List[str] = json.loads(raw_outcomes_field)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Could not parse outcomes for market {raw_market['id']}")
                raw_outcomes = []
        else:
            raw_outcomes = raw_outcomes_field if raw_outcomes_field else []

        clob_ids_raw = raw_market.get("clobTokenIds", [])
        if isinstance(clob_ids_raw, str):
            try:
                clob_ids: List[str] = json.loads(clob_ids_raw)
            except (json.JSONDecodeError, ValueError):
                logger.warning(
                    f"Could not parse clobTokenIds for market {raw_market['id']}: {clob_ids_raw[:60]}"
                )
                clob_ids = []
        else:
            clob_ids = clob_ids_raw if clob_ids_raw else []

        outcome_prices_raw = raw_market.get("outcomePrices", [])
        if isinstance(outcome_prices_raw, str):
            try:
                outcome_prices: List[str] = json.loads(outcome_prices_raw)
            except (json.JSONDecodeError, ValueError):
                outcome_prices = []
        else:
            outcome_prices = outcome_prices_raw if outcome_prices_raw else []

        # Real bid/ask spread from the Gamma API (available for Yes/primary outcome).
        # spread = bestAsk - bestBid; bestAsk is for the primary (Yes) outcome.
        _api_spread   = raw_market.get("spread")
        _api_best_ask = raw_market.get("bestAsk")
        try:
            _api_spread   = float(_api_spread)   if _api_spread   is not None else None
            _api_best_ask = float(_api_best_ask) if _api_best_ask is not None else None
        except (ValueError, TypeError):
            _api_spread = _api_best_ask = None

        for i, outcome_name in enumerate(raw_outcomes):
            asset_id = clob_ids[i] if i < len(clob_ids) else ""
            outcome = SimpleNamespace(
                market_id=market.id,
                name=outcome_name,
                asset_id=asset_id,
            )
            all_outcomes.append(outcome)

            if i < len(outcome_prices):
                try:
                    price_val = float(outcome_prices[i])
                    # Attach real spread/ask data only to the primary (i==0) outcome,
                    # which is the Yes token for binary markets.
                    price_entry: Dict[str, Any] = {
                        "market_id":    market.id,
                        "outcome_name": outcome_name,
                        "price":        price_val,
                    }
                    if i == 0 and _api_spread is not None and _api_best_ask is not None:
                        price_entry["api_spread"]   = _api_spread
                        price_entry["api_best_ask"] = _api_best_ask
                    all_prices.append(price_entry)
                except (ValueError, TypeError):
                    pass

        for tag_data in raw_event.get("tags", []):
            if isinstance(tag_data, dict):
                tag_name = tag_data.get("label", tag_data.get("slug", "unknown"))
            else:
                tag_name = str(tag_data)

            tag = SimpleNamespace(
                market_id=market.id,
                tag=tag_name,
            )
            all_tags.append(tag)

    return event, markets, all_outcomes, all_tags, all_prices
