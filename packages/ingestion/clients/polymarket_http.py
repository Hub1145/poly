import base64
import hashlib
import hmac
import logging
import asyncio
import time
from typing import Any, Dict, List

import httpx
from packages.core.config import settings


# In-memory derived CLOB credentials — populated by ClobClient.derive_api_credentials().
# Never stored to disk; always re-derived from the private key at startup.
_derived_api_key:        str = ""
_derived_api_secret:     str = ""
_derived_api_passphrase: str = ""


def clob_creds_available() -> bool:
    """Return True if CLOB L2 credentials have been successfully derived this session."""
    return bool(_derived_api_key and _derived_api_secret and _derived_api_passphrase)


def _build_l2_headers(method: str, path: str) -> Dict[str, str]:
    """Build Polymarket CLOB L2 HMAC auth headers from in-memory derived credentials.

    Required headers (per Polymarket CLOB API spec):
        POLY_ADDRESS, POLY_API_KEY, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_NONCE
    """
    wallet_address = settings.polymarket.wallet_address  # derived from private_key
    if not clob_creds_available() or not wallet_address:
        return {}
    timestamp = str(int(time.time()))
    message   = timestamp + method.upper() + path
    try:
        secret_bytes = base64.b64decode(_derived_api_secret)
        sig = base64.b64encode(
            hmac.new(secret_bytes, message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
    except Exception:
        return {}
    return {
        "POLY_ADDRESS":   wallet_address,
        "POLY_API_KEY":   _derived_api_key,
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": timestamp,
        "POLY_NONCE":     "0",
        "Content-Type":   "application/json",
    }

try:
    from py_clob_client.client import ClobClient as OfficialClobClient
    from py_clob_client.clob_types import ApiCreds
    HAS_OFFICIAL_CLIENT = True
except ImportError:
    HAS_OFFICIAL_CLIENT = False

logger = logging.getLogger(__name__)



class GammaClient:
    """Client for the Polymarket Gamma API (Market Metadata)."""
    
    def __init__(self, base_url: str = settings.polymarket.gamma_api_url):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)

    async def get_events(
        self,
        limit: int = 50,
        offset: int = 0,
        active: bool = True,
        order: str = "volume24hr",
        ascending: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch events from Gamma, sorted by 24-hour volume descending by default."""
        params = {
            "limit":     limit,
            "offset":    offset,
            "active":    str(active).lower(),
            "closed":    "false",
            "order":     order,
            "ascending": str(ascending).lower(),
        }
        response = await self.client.get("/events", params=params)
        response.raise_for_status()
        return response.json()

    async def get_events_paginated(self, max_events: int = 500) -> List[Dict[str, Any]]:
        """
        Fetch up to max_events active events across multiple pages.
        Capped at 500 — the tag-based pass in refresh_markets covers the long tail.
        """
        all_events: List[Dict[str, Any]] = []
        page_size = 50
        offset    = 0
        while len(all_events) < max_events:
            batch = await self.get_events(limit=page_size, offset=offset)
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_events[:max_events]

    async def get_events_by_tag(self, tag_id: int, max_events: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch active events filtered by Polymarket tag ID.
        Capped at 100 per tag — priority tags cover the most relevant markets.
        """
        all_events: List[Dict[str, Any]] = []
        page_size = 50
        offset    = 0
        while len(all_events) < max_events:
            params = {
                "limit":    page_size,
                "offset":   offset,
                "active":   "true",
                "closed":   "false",
                "tag_id":   tag_id,
                "order":    "volume24hr",
                "ascending": "false",
            }
            response = await self.client.get("/events", params=params)
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_events[:max_events]


    async def search_events(self, query: str, max_events: int = 100) -> List[Dict[str, Any]]:
        """
        Full-text search for events matching a keyword query via the Gamma API `?q=` param.
        Used to fetch niche markets (temperature, seismic, etc.) that may not surface in
        volume-sorted feeds or tag-based queries.
        """
        all_events: List[Dict[str, Any]] = []
        page_size = 50
        offset    = 0
        while len(all_events) < max_events:
            params = {
                "limit":     page_size,
                "offset":    offset,
                "active":    "true",
                "closed":    "false",
                "q":         query,
                "order":     "volume24hr",
                "ascending": "false",
            }
            try:
                response = await self.client.get("/events", params=params)
                response.raise_for_status()
                batch = response.json()
            except Exception:
                break
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_events[:max_events]

    async def get_event(self, event_id: str) -> Dict[str, Any]:
        """Fetch a single event by ID."""
        response = await self.client.get(f"/events/{event_id}")
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.client.aclose()

class ClobClient:
    """
    Client for the Polymarket CLOB API.
    Enhanced to use official py_clob_client if keys are provided.
    Supports automatic credential derivation from a private key.
    """

    def __init__(self, base_url: str = settings.polymarket.clob_api_url):
        self.base_url    = base_url
        self.http_client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
        self.official_client = None
        # Credentials are always derived at startup via derive_api_credentials().
        # __init__ only sets up the L1-only client; full auth happens after derivation.
        pk = settings.polymarket.private_key
        if HAS_OFFICIAL_CLIENT and pk and pk != "0x" + "0" * 64:
            try:
                self.official_client = OfficialClobClient(
                    host=self.base_url, key=pk, chain_id=137
                )
            except Exception as e:
                logger.warning(f"Failed to init L1 CLOB client: {e}")

    async def derive_api_credentials(self) -> bool:
        """Derive L2 credentials from the private key and store them in memory.

        Derivation is deterministic — same key always yields the same credentials.
        Called once at startup; credentials are never written to disk.
        Returns True on success.
        """
        global _derived_api_key, _derived_api_secret, _derived_api_passphrase

        pk = settings.polymarket.private_key
        if not HAS_OFFICIAL_CLIENT or not pk or pk == "0x" + "0" * 64:
            return False

        logger.info("Deriving CLOB API credentials from private key...")
        try:
            l1_client = OfficialClobClient(host=self.base_url, key=pk, chain_id=137)
            derived   = await asyncio.to_thread(l1_client.derive_api_key)

            _derived_api_key        = derived.api_key
            _derived_api_secret     = derived.api_secret
            _derived_api_passphrase = derived.api_passphrase

            full_creds = ApiCreds(
                api_key=derived.api_key,
                api_secret=derived.api_secret,
                api_passphrase=derived.api_passphrase,
            )
            self.official_client = OfficialClobClient(
                host=self.base_url, key=pk, chain_id=137, creds=full_creds
            )
            logger.info(f"CLOB credentials derived (key={derived.api_key[:8]}…).")
            return True

        except Exception as e:
            _derived_api_key        = ""
            _derived_api_secret     = ""
            _derived_api_passphrase = ""
            if "400" in str(e) or "Could not derive" in str(e):
                logger.warning(
                    "CLOB derivation rejected (400) — wallet must accept ToS on "
                    "polymarket.com first. Per-market sync disabled; paper trading unaffected."
                )
            else:
                logger.warning(f"Could not derive CLOB credentials: {e}")
            return False

    async def get_trades(self, asset_id: str, limit: int = 500) -> List[Dict[str, Any]]:
        """Fetch recent trades for a specific asset (CLOB token ID).

        Polymarket CLOB authenticated endpoint:
            GET /data/trades?asset_id=<token_id>&limit=N
            Requires L2 API-key auth (api_key / api_secret / api_passphrase + wallet address).

        Falls back gracefully to an empty list when:
          - The official client is unavailable (no valid private key configured)
          - The endpoint returns 401 (auth required) or 404
        """
        if not asset_id:
            return []

        if self.official_client:
            try:
                from py_clob_client.clob_types import TradeParams
                resp = await asyncio.to_thread(
                    self.official_client.get_trades,
                    params=TradeParams(asset_id=asset_id),
                )
                if isinstance(resp, list):
                    return resp
                if isinstance(resp, dict):
                    return resp.get("data", resp.get("history", []))
            except Exception as e:
                logger.debug(f"Official client get_trades failed: {e}. Falling back to HTTP.")

        # Attempt the authenticated /data/trades endpoint with L2 HMAC headers.
        # Falls back silently to empty list on 401/403 (no credentials configured).
        try:
            path = f"/data/trades?asset_id={asset_id}&limit={limit}"
            auth_headers = _build_l2_headers("GET", path)
            response = await self.http_client.get(
                "/data/trades",
                params={"asset_id": asset_id, "limit": limit},
                headers=auth_headers,
            )
            if response.status_code in (401, 403, 404):
                logger.debug(
                    f"CLOB /data/trades returned {response.status_code} for asset {asset_id[:16]}... "
                    f"— API credentials required. Returning empty trade list."
                )
                return []
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                return data.get("data", data.get("history", []))
            return data if isinstance(data, list) else []
        except httpx.HTTPStatusError:
            return []
        except Exception as e:
            logger.warning(f"CLOB get_trades request failed for {asset_id[:16]}...: {e}")
            return []

    async def get_global_trade_feed(self, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Fetch the latest trades from the PUBLIC Polymarket data-api global feed.
        No authentication required.  Returns up to `limit` recent trades across
        all markets.  Each record contains:
            proxyWallet, side, asset (token_id), conditionId, size, price,
            timestamp (unix seconds), transactionHash, outcome, name
        """
        DATA_API = "https://data-api.polymarket.com"
        try:
            async with httpx.AsyncClient(base_url=DATA_API, timeout=15.0) as c:
                r = await c.get("/trades", params={"limit": limit})
                if r.status_code != 200:
                    logger.debug(f"data-api /trades returned {r.status_code}")
                    return []
                data = r.json()
                return data if isinstance(data, list) else data.get("data", [])
        except Exception as e:
            logger.warning(f"data-api global feed failed: {e}")
            return []

    async def get_market_trade_feed(
        self, asset_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades for a specific market outcome via the public data-api.
        No authentication required.  Uses ?asset=<token_id> filter.
        Returns list of trade dicts with same schema as get_global_trade_feed.
        """
        DATA_API = "https://data-api.polymarket.com"
        try:
            async with httpx.AsyncClient(base_url=DATA_API, timeout=15.0) as c:
                r = await c.get("/trades", params={"asset": asset_id, "limit": limit})
                if r.status_code != 200:
                    logger.debug(f"data-api /trades?asset= returned {r.status_code}")
                    return []
                data = r.json()
                return data if isinstance(data, list) else data.get("data", [])
        except Exception as e:
            logger.debug(f"data-api per-market feed failed for {asset_id[:16]}...: {e}")
            return []

    async def get_orderbook(self, token_id: str) -> Dict[str, Any]:
        """Fetch current orderbook for a specific token."""
        if self.official_client:
            try:
                resp = await asyncio.to_thread(self.official_client.get_market_orderbook, token_id=token_id)
                return resp
            except Exception as e:
                logger.warning(f"Official client get_orderbook failed: {e}. Falling back to HTTP.")

        response = await self.http_client.get(f"/book", params={"token_id": token_id})
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.http_client.aclose()
