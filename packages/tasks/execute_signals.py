"""
Signal Execution Engine
=======================
Reads top Alpha signals and places trades:
  - Paper mode : records a PositionSnapshot, deducts from paper_balance
  - Live mode  : signs and posts a GTC limit order via py-clob-client
"""
import logging
from datetime import datetime
from typing import Optional

from packages.db.database import get_db
from packages.core.config import settings, WEATHER_LADDER_LEGS
from packages.ingestion.clients.polymarket_http import (
    _derived_api_key, _derived_api_secret, _derived_api_passphrase,
)
from packages.ui.state_mapper import _STRATEGY_SIGNAL_TYPES

logger = logging.getLogger(__name__)

BOT_PAPER_ADDRESS = "0xbot_paper_wallet"


async def _ensure_bot_wallet(db, address: str) -> None:
    exists = await db.fetchval(
        "SELECT address FROM trader_wallets WHERE address=? LIMIT 1", (address,)
    )
    if not exists:
        await db.execute(
            "INSERT INTO trader_wallets (address) VALUES (?)", (address,)
        )


async def execute_signals() -> int:
    """Evaluate current top signals and place trades where conditions are met."""
    db = get_db()

    paper_mode   = settings.app.paper_mode
    trade_amount = float(settings.app.trade_amount)
    # min_edge is a raw edge fraction (0.0–1.0). Signal strength units differ
    # by strategy: weather/price-zone signals have strength = abs(edge) (~0.1–0.99),
    # trader-based signals have composite strength (~0.5–5.0). Use min_edge directly
    # as the floor — it represents the minimum edge required regardless of scale.
    min_strength = float(settings.app.min_edge)
    max_trades   = int(settings.app.max_trades)
    strategy     = settings.strategy

    bot_address = BOT_PAPER_ADDRESS if paper_mode else settings.polymarket.wallet_address
    if not bot_address:
        logger.warning("[Execute] No wallet address configured — skipping execution.")
        return 0

    await _ensure_bot_wallet(db, bot_address)

    open_count = await db.fetchval(
        "SELECT COUNT(id) FROM position_snapshots"
        " WHERE trader_address=? AND current_size > 0",
        (bot_address,),
    ) or 0

    slots_remaining = max_trades - open_count
    if slots_remaining <= 0:
        logger.info(f"[Execute] Max trades reached ({open_count}/{max_trades}).")
        return 0

    open_rows = await db.fetchall(
        "SELECT market_id FROM position_snapshots"
        " WHERE trader_address=? AND current_size > 0",
        (bot_address,),
    )
    open_market_ids: set = {r["market_id"] for r in open_rows}

    signal_types = _STRATEGY_SIGNAL_TYPES.get(strategy, ["bayesian_ensemble"])
    placeholders = ",".join("?" * len(signal_types))

    rows = await db.fetchall(
        f"""
        SELECT s.market_id, s.outcome_id, s.directional_bias, s.signal_strength,
               o.asset_id, m.question
        FROM market_signal_snapshots s
        JOIN markets  m ON m.id = s.market_id
        JOIN outcomes o ON o.id = s.outcome_id
        WHERE s.signal_type IN ({placeholders})
          AND s.signal_strength >= ?
        ORDER BY s.signal_strength DESC
        LIMIT ?
        """,
        (*signal_types, min_strength, slots_remaining * 3),
    )

    if not rows:
        logger.info(f"[Execute] No signals above threshold (min_strength={min_strength:.2f}) for strategy='{strategy}'. "
                    f"Open positions: {open_count}/{max_trades}.")
        return 0

    # Ladder strategies: split the fixed trade_amount across legs.
    # Each leg gets trade_amount / ladder_legs (min $1).
    _LADDER_STRATEGIES = {"laddering", "weather_prediction"}
    is_ladder    = strategy in _LADDER_STRATEGIES
    ladder_legs  = WEATHER_LADDER_LEGS if is_ladder else 1
    leg_amount   = max(1.0, trade_amount / ladder_legs) if is_ladder else trade_amount

    # For ladder strategies, track city/date combos to cap at LADDER_LEGS per bucket group
    city_leg_counts: dict = {}   # city_date_key → legs placed

    trades_placed = 0
    for row in rows:
        if trades_placed >= slots_remaining:
            break
        market_id = row.market_id
        if market_id in open_market_ids:
            continue

        # Ladder cap: no more than ladder_legs YES bets per city+date
        if is_ladder and row.directional_bias == "YES":
            q = (row.question or "").lower()
            # Extract city key from question (first 30 chars is usually "Will the highest temperature in <city>")
            city_key = q[:50]
            city_leg_counts.setdefault(city_key, 0)
            if city_leg_counts[city_key] >= ladder_legs:
                continue
            city_leg_counts[city_key] += 1

        price_row = await db.fetchval(
            """
            SELECT ps.mid_price FROM price_snapshots ps
            JOIN outcomes o ON ps.outcome_id = o.id
            WHERE ps.market_id=? AND LOWER(o.name) = LOWER(?)
            ORDER BY ps.timestamp DESC LIMIT 1
            """,
            (market_id, row.directional_bias),
        )
        if price_row is None:
            logger.info(f"[Execute] No price snapshot for market {market_id[:16]}... — skip.")
            continue

        # Ladder legs use split amount; all other strategies use full trade_amount
        notional    = leg_amount if is_ladder else trade_amount
        entry_price = round(min(0.99, float(price_row) + 0.01), 4)
        contracts   = round(notional / entry_price, 4)

        if paper_mode:
            success = _paper_execute(notional)
        else:
            success = await _live_execute(row.asset_id, entry_price, contracts)

        if not success:
            continue

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        await db.execute(
            "INSERT INTO position_snapshots"
            " (trader_address, market_id, outcome_id, current_size,"
            "  avg_entry_price, unrealized_pnl, snapshot_at)"
            " VALUES (?, ?, ?, ?, ?, 0.0, ?)",
            (bot_address, market_id, row.outcome_id, contracts, entry_price, now),
        )

        open_market_ids.add(market_id)
        trades_placed += 1

        mode_tag = "PAPER" if paper_mode else "LIVE"
        logger.info(
            f"[Execute][{mode_tag}] {row.directional_bias} on '{row.question[:50]}' | "
            f"price={entry_price:.4f}  size={contracts:.4f}  notional=${notional:.2f}  "
            f"strength={row.signal_strength:.3f}"
            + (f"  [ladder leg {city_leg_counts.get(row.question[:50].lower(), '?')}/{ladder_legs}]" if is_ladder else "")
        )

    if trades_placed > 0:
        await db.commit()
        logger.info(f"[Execute] {trades_placed} trade(s) placed.")

    return trades_placed


def _paper_execute(notional: float) -> bool:
    if settings.app.paper_balance < notional:
        logger.warning(
            f"[Execute][PAPER] Insufficient paper balance "
            f"(${settings.app.paper_balance:.2f} < ${notional:.2f}). Skipping."
        )
        return False
    settings.app.paper_balance = round(settings.app.paper_balance - notional, 4)
    return True


async def _live_execute(asset_id: Optional[str], price: float, size: float) -> bool:
    try:
        from py_clob_client.client import ClobClient as OfficialClobClient
        from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, BUY
    except ImportError:
        logger.error("[Execute][LIVE] py-clob-client not installed.")
        return False

    pk = settings.polymarket.private_key
    if not pk or pk == "0x" + "0" * 64 or not pk.strip():
        logger.error("[Execute][LIVE] No private key configured.")
        return False
    if not asset_id:
        logger.error("[Execute][LIVE] No asset_id for outcome — cannot place order.")
        return False

    try:
        creds = ApiCreds(
            api_key=_derived_api_key,
            api_secret=_derived_api_secret,
            api_passphrase=_derived_api_passphrase,
        )
        client = OfficialClobClient(
            host=settings.polymarket.clob_api_url,
            key=pk, chain_id=137, creds=creds,
        )
        signed_order = client.create_order(
            OrderArgs(token_id=asset_id, price=price, size=size, side=BUY)
        )
        resp = client.post_order(signed_order, OrderType.GTC)
        logger.info(f"[Execute][LIVE] Order response: {resp}")
        return True
    except Exception as e:
        logger.error(f"[Execute][LIVE] Order failed: {e}")
        return False
