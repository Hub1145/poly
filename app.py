import asyncio
import logging
import time
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi_socketio import SocketManager

from packages.db.database import init_db
from packages.ui.state_mapper import map_db_to_bot_state
from packages.core.config import settings, WEATHER_BURST_SCAN_MINUTES
from packages.tasks.refresh_markets import refresh_markets_for_strategy
from packages.tasks.refresh_trades import refresh_trades
from packages.tasks.recompute_features import refresh_trader_profiles
from packages.tasks.compute_signals import refresh_market_signals
from packages.tasks.execute_signals import execute_signals

logger = logging.getLogger("polymarket_alpha_ui")

app = FastAPI()
sio = SocketManager(app=app, cors_allowed_origins="*", mount_location="/socket.io")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

is_trading = False
is_syncing = False
is_scanning = False       # True only while re-fetching markets for a new strategy
is_initializing = True    # True during the one-time startup fetch
db_lock = asyncio.Lock()

bot_state: Dict[str, Any] = {
    "is_trading": False,
    "is_syncing": True,
    "metrics": {"total_trades": 0, "win_rate": 0.0, "total_profit": 0.0, "balance": 0.0},
    "total_scanned": 0,
    "scanned_markets": [],
    "open_positions": [],
    "resolved_positions": [],
    "news_events": [],
    "dev_check_logs": [],
    "logs": ["Signal engine initializing — fetching markets..."],
    "config": {
        "paper_mode":    True,
        "trade_amount":  10.0,
        "min_edge":      0.20,
        "scan_interval": 30,
        "strategy":      "bayesian_ensemble",
        "paper_balance": 1000.0,
        "max_trades":    10,
    },
}

SIGNAL_CYCLE_SECONDS  = 30
TRADE_CYCLE_SECONDS   = 60
MARKET_CYCLE_MIN      = 30

# Model run burst-scan: NOAA/ECMWF/GFS update at 00Z, 06Z, 12Z, 18Z UTC.
# Within BURST_WINDOW_MIN minutes of a model run, scan every BURST_CYCLE_SECONDS.
# Outside the window, use the normal SIGNAL_CYCLE_SECONDS interval.
_MODEL_RUN_HOURS  = [0, 6, 12, 18]
_BURST_WINDOW_MIN = WEATHER_BURST_SCAN_MINUTES
_BURST_CYCLE_SECS = 5 * 60   # 5-minute scans during burst window


def _in_model_run_burst_window() -> bool:
    """Return True if we are within BURST_WINDOW_MIN minutes of a model run."""
    from datetime import datetime as _dtnow
    now = _dtnow.utcnow()
    minutes_now = now.hour * 60 + now.minute
    for run_hour in _MODEL_RUN_HOURS:
        run_min = run_hour * 60
        since   = (minutes_now - run_min) % (24 * 60)
        if since <= _BURST_WINDOW_MIN:
            return True
    return False


async def update_state_loop():
    """Periodically refresh bot state from the DB and broadcast via SocketIO."""
    while True:
        try:
            global bot_state
            bot_state = await map_db_to_bot_state(
                is_trading=is_trading,
                is_syncing=is_syncing,
                is_scanning=is_scanning,
                is_initializing=is_initializing,
            )
            await sio.emit("bot_status", bot_state)
        except Exception as e:
            logger.error(f"Error in state update loop: {e}")

        await asyncio.sleep(2)


_force_sync_event = asyncio.Event()
_last_fetched_strategy: str = ""
_last_market_fetch_time: float = 0.0
# Per-strategy fetch-time cache.
# When a strategy is fetched, we record the time. If the same strategy is
# selected again within STRATEGY_CACHE_TTL seconds we re-use the DB data
# instead of hitting the Polymarket API again.
_STRATEGY_CACHE_TTL: float = 3600.0   # 1 hour
_strategy_fetch_times: Dict[str, float] = {}


def _market_fetch_limit() -> int:
    """Initial market fetch limit — enough candidates without over-fetching.
    Scales with max_trades so small configs stay fast."""
    return max(50, settings.app.max_trades * 8)


async def background_workers_loop():
    """
    Three-layer background engine:

      Layer 1 — Signal cycle (every 30 s):
        Re-score all cached markets in the DB. No Polymarket API calls.

      Layer 2 — Trade/Position cycle (every 60 s, only when bot is running):
        Refresh trade history and re-compute trader profiles from CLOB.

      Layer 3 — Market fetch cycle (every scan_interval min, min 30):
        Pull new markets from Polymarket Gamma API for the active strategy.
        Skipped if the strategy was fetched < 1 hour ago (uses cached DB data).
    """
    global is_syncing, is_scanning, is_initializing, _last_fetched_strategy, _last_market_fetch_time
    logger.info("Starting background worker loop...")

    last_trade_refresh: float = 0.0

    # ── Unblock UI immediately ──────────────────────────────────────────────
    # The scan tab shows cached DB markets (via the fallback in state_mapper)
    # within the first 2-second update cycle — no waiting for API calls.
    is_initializing = False

    # ── CLOB credential derivation — fire-and-forget (not needed for display) ─
    async def _derive_creds():
        try:
            from packages.ingestion.clients.polymarket_http import ClobClient
            _clob = ClobClient()
            await _clob.derive_api_credentials()
            await _clob.close()
            logger.info("CLOB credentials derived successfully.")
        except Exception as exc:
            logger.warning(f"CLOB credential derivation failed (non-critical): {exc}")

    asyncio.create_task(_derive_creds())

    # ── Phase 1: market fetch + first-pass signals (runs immediately) ───────
    # No is_scanning overlay during startup — the fallback rows already show
    # in the scan table while this runs in the background.  The scan tab is
    # live from the very first update tick.
    async def _startup_phase1():
        global _last_fetched_strategy, _last_market_fetch_time
        nonlocal last_trade_refresh
        _startup_strategy = settings.strategy
        try:
            logger.info(f"Startup phase 1 — fetching markets for strategy='{_startup_strategy}'...")
            async with db_lock:
                from packages.services.market_service import MarketService as _MS
                await _MS().prune_resolved_markets(older_than_days=7)
                await refresh_markets_for_strategy(_startup_strategy, limit=_market_fetch_limit())
                # First-pass signals scored immediately (no trade data yet)
                await refresh_market_signals()
            _now = time.monotonic()
            _last_fetched_strategy           = _startup_strategy
            _last_market_fetch_time          = _now
            _strategy_fetch_times[_startup_strategy] = _now
            last_trade_refresh               = _now
            logger.info("Startup phase 1 complete — signals live. Starting trade ingestion...")

            # ── Phase 2: trade ingestion + profile rebuild (background) ────
            async def _startup_phase2():
                nonlocal last_trade_refresh
                try:
                    async with db_lock:
                        await refresh_trades()
                        await refresh_trader_profiles()
                        await refresh_market_signals()
                    last_trade_refresh = time.monotonic()
                    logger.info("Startup phase 2 complete — trader profiles and signals updated.")
                except Exception as exc:
                    logger.error(f"Startup phase 2 (trade ingestion) failed: {exc}")

            asyncio.create_task(_startup_phase2())

        except Exception as exc:
            logger.error(f"Startup phase 1 failed: {exc}")

    asyncio.create_task(_startup_phase1())

    while True:
        try:
            has_force_sync = _force_sync_event.is_set()
            _force_sync_event.clear()

            now              = time.monotonic()
            current_strategy = settings.strategy
            strategy_changed = current_strategy != _last_fetched_strategy
            market_interval  = max(MARKET_CYCLE_MIN, settings.app.scan_interval) * 60
            market_due       = (now - _last_market_fetch_time) >= market_interval
            trade_due        = (now - last_trade_refresh) >= TRADE_CYCLE_SECONDS

            is_syncing = True

            async with db_lock:
                # ── Layer 3: Market fetch ────────────────────────────────────
                if strategy_changed:
                    # Strategy switched: check per-strategy TTL before hitting the API.
                    cache_age = now - _strategy_fetch_times.get(current_strategy, 0.0)
                    if cache_age < _STRATEGY_CACHE_TTL:
                        # DB already has fresh data for this strategy — just re-score.
                        logger.info(
                            f"[Strategy] Switched to '{current_strategy}' — "
                            f"DB cache is {cache_age:.0f}s old, skipping API fetch."
                        )
                    else:
                        # Data is stale or never fetched — fetch now.
                        logger.info(
                            f"[Strategy] Switched to '{current_strategy}' — "
                            f"cache expired ({cache_age:.0f}s), fetching markets..."
                        )
                        is_scanning = True
                        await refresh_markets_for_strategy(current_strategy, limit=_market_fetch_limit())
                        is_scanning = False
                        _now_t = time.monotonic()
                        _last_market_fetch_time      = _now_t
                        _strategy_fetch_times[current_strategy] = _now_t
                    _last_fetched_strategy = current_strategy

                elif has_force_sync or market_due:
                    # Periodic scheduled refresh for the current strategy only.
                    reason = "force sync" if has_force_sync else "scheduled interval"
                    logger.info(f"[Market Update] {reason} — refreshing '{current_strategy}'...")
                    is_scanning = True
                    await refresh_markets_for_strategy(current_strategy, limit=_market_fetch_limit())
                    is_scanning = False
                    _now_t = time.monotonic()
                    _last_market_fetch_time      = _now_t
                    _strategy_fetch_times[current_strategy] = _now_t
                    logger.info("[Market Update] Done.")

                # ── Layer 2: Trade / position refresh ────────────────────────
                if trade_due or has_force_sync:
                    logger.info("[Trade Refresh] Updating positions...")
                    await refresh_trades()
                    await refresh_trader_profiles()
                    last_trade_refresh = time.monotonic()
                    logger.info("[Trade Refresh] Done.")

                # ── Layer 1: Signal recompute on cached markets ───────────────
                logger.info(f"[Signal Cycle] Scoring markets (strategy='{current_strategy}')...")
                signals_found = await refresh_market_signals()
                logger.info(f"[Signal Cycle] Done — {signals_found} signals.")

                # ── Progressive expansion: widen scan if signals are sparse ───
                # If too few signals were found, fetch more markets (up to 3×) and
                # re-score once. This avoids always over-fetching on every cycle.
                min_needed = max(2, settings.app.max_trades // 3)
                if signals_found < min_needed:
                    expanded_limit = _market_fetch_limit() * 3
                    logger.info(
                        f"[Progressive] Only {signals_found} signals (need {min_needed}) — "
                        f"expanding fetch to {expanded_limit} markets..."
                    )
                    is_scanning = True
                    await refresh_markets_for_strategy(current_strategy, limit=expanded_limit)
                    is_scanning = False
                    _strategy_fetch_times[current_strategy] = time.monotonic()
                    signals_found = await refresh_market_signals()
                    logger.info(f"[Progressive] Expanded — {signals_found} signals now available.")

                # ── Layer 0: Execute trades on top signals ────────────────────
                if is_trading:
                    logger.info("[Execute] Evaluating signals for trade execution...")
                    await execute_signals()

            is_syncing = False

        except Exception as e:
            is_syncing = False
            is_scanning = False
            logger.error(f"Background sync error: {e}")
            import traceback
            logger.error(traceback.format_exc())

        try:
            cycle_secs = _BURST_CYCLE_SECS if _in_model_run_burst_window() else SIGNAL_CYCLE_SECONDS
            await asyncio.wait_for(_force_sync_event.wait(), timeout=cycle_secs)
        except asyncio.TimeoutError:
            pass


@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(update_state_loop())
    asyncio.create_task(background_workers_loop())


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.post("/api/control")
async def control(request: Request):
    global is_trading
    data   = await request.json()
    action = data.get("action")

    if action == "start":
        is_trading = True
        _force_sync_event.set()   # wake background loop immediately to execute signals
        logger.info("Bot STARTED — triggering immediate signal execution cycle.")
    elif action == "stop":
        is_trading = False
        logger.info("Bot STOPPED.")

    return {"status": "success", "is_trading": is_trading}


@app.post("/api/config")
async def update_config(request: Request):
    """Save strategy and trading settings from the dashboard Settings tab."""
    import json
    from pathlib import Path
    from packages.core.config import SETTINGS_FILE

    data          = await request.json()
    settings_path = SETTINGS_FILE

    current = {}
    if settings_path.exists():
        try:
            with open(settings_path) as f:
                current = json.load(f)
        except Exception:
            current = {}

    allowed_keys = {
        "strategy", "paper_mode", "trade_amount",
        "min_edge", "scan_interval", "paper_balance", "max_trades",
    }
    for key in allowed_keys:
        if key in data:
            current[key] = data[key]


    pk = data.get("private_key", "").strip()
    if pk and pk != "0x" + "0" * 64:
        if "polymarket" not in current:
            current["polymarket"] = {}
        current["polymarket"]["private_key"] = pk
        settings.polymarket.private_key = pk

    with open(settings_path, "w") as f:
        json.dump(current, f, indent=2)

    if "paper_mode" in data:
        settings.app.paper_mode = str(data["paper_mode"]).lower() == "true"
    if "trade_amount" in data:
        settings.app.trade_amount = float(data["trade_amount"])
    if "min_edge" in data:
        settings.app.min_edge = float(data["min_edge"])
    if "scan_interval" in data:
        settings.app.scan_interval = int(data["scan_interval"])
    if "paper_balance" in data:
        settings.app.paper_balance = float(data["paper_balance"])
    if "max_trades" in data:
        settings.app.max_trades = int(data["max_trades"])

    if "strategy" in data:
        old_strategy = settings.strategy
        new_strategy = str(data["strategy"])
        if old_strategy != new_strategy:
            cache_age = time.monotonic() - _strategy_fetch_times.get(new_strategy, 0.0)
            action = "using cached data" if cache_age < _STRATEGY_CACHE_TTL else "re-fetching markets"
            logger.info(f"Strategy: '{old_strategy}' -> '{new_strategy}' ({action})")
            settings.strategy = new_strategy
            global _last_fetched_strategy
            _last_fetched_strategy = ""
            _force_sync_event.set()

    logger.info(f"Settings updated: {current}")
    return {"status": "saved", "settings": current}


@app.get("/api/config")
async def get_config():
    """Return current settings so the frontend can populate the form on page load."""
    return JSONResponse({
        "paper_mode":          settings.app.paper_mode,
        "trade_amount":        settings.app.trade_amount,
        "min_edge":            settings.app.min_edge,
        "scan_interval":       settings.app.scan_interval,
        "strategy":            settings.strategy,
        "paper_balance":       settings.app.paper_balance,
        "max_trades":          settings.app.max_trades,
    })


@app.sio.on("request_update")
async def handle_request_update(sid, *args, **kwargs):
    await sio.emit("bot_status", bot_state, to=sid)


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5001,
        proxy_headers=True,
        forwarded_allow_ips="*",
        log_level="info",
    )
