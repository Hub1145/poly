"""
Position Manager — Market Resolution + Take Profit & Stop Loss
==============================================================
Runs on every signal cycle.  For each open bot position:

  1. Resolution check (highest priority):
       Detects if the market has closed on Polymarket (closed=1, active=0 in DB).
       Fetches the final outcomePrices from the Gamma API.
       Settles at 1.0 (WIN) or 0.0 (LOSS) immediately — no waiting for TP/SL.

  2. TP/SL check (while market is still live):
       - Fetches the current YES/NO price from price_snapshots.
       - Compares against entry price using configured TP/SL percentages.
       - If TP or SL is hit:
           Paper mode : writes to closed_positions, restores paper_balance.
           Live mode  : places a market-sell order via py_clob_client, then records.

TP/SL logic
-----------
  take_profit (default 50%):  close when current_price >= entry_price * (1 + tp/100)
  stop_loss   (default 30%):  close when current_price <= entry_price * (1 - sl/100)

Prices here are Polymarket outcome-token prices (0.0–1.0).
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from packages.db.database import get_db
from packages.core.config import settings
from packages.ingestion.clients.polymarket_http import (
    GammaClient,
    _derived_api_key, _derived_api_secret, _derived_api_passphrase,
)

logger = logging.getLogger(__name__)

BOT_PAPER_ADDRESS = "0xbot_paper_wallet"


async def _get_resolved_price(db, market_id: str, outcome_id: int) -> Optional[float]:
    """
    Check whether a market has fully resolved on Polymarket.

    Returns:
      1.0  — our outcome WON (settle at full value)
      0.0  — our outcome LOST (settle at zero)
      None — market is still live; fall through to TP/SL logic
    """
    # Proceed if DB says closed OR if the market's end date has already passed.
    # Resolved markets drop out of the active feed so the closed flag may lag;
    # checking end_date_iso catches that gap without hammering the API for live markets.
    market_meta = await db.fetchone(
        "SELECT closed, active, end_date_iso FROM markets WHERE id=?",
        (market_id,),
    )
    if not market_meta:
        return None

    already_closed = market_meta["closed"] and not market_meta["active"]
    end_date_str   = market_meta["end_date_iso"]
    past_end = False
    if end_date_str:
        try:
            end_dt   = datetime.fromisoformat(str(end_date_str).replace("Z", "+00:00"))
            now_utc  = datetime.now(timezone.utc)
            past_end = now_utc > end_dt
        except ValueError:
            pass

    if not already_closed and not past_end:
        return None

    # Look up the outcome name (Yes / No) and the event_id we need for the API call.
    meta = await db.fetchone(
        """
        SELECT o.name AS outcome_name, m.event_id
        FROM outcomes o
        JOIN markets m ON m.id = o.market_id
        WHERE o.id = ? AND m.id = ?
        """,
        (outcome_id, market_id),
    )
    if not meta:
        return None

    outcome_name = (meta["outcome_name"] or "Yes").strip().lower()
    event_id     = meta["event_id"]

    # Fetch live outcomePrices from Gamma API — the single source of truth for
    # resolved prices (1.0 / 0.0).  Use a fresh client; close it right after.
    gc = GammaClient()
    try:
        raw_event = await gc.get_event(str(event_id))
    except Exception as e:
        logger.warning(f"[Resolve] Gamma API fetch failed for event {event_id}: {e}")
        return None
    finally:
        await gc.close()

    # Find the specific market inside the event.
    raw_markets = raw_event.get("markets", [])
    target = next((m for m in raw_markets if str(m.get("id", "")) == str(market_id)), None)
    if not target:
        return None

    # Parse outcomePrices — Gamma returns either a JSON string or a list.
    prices_raw = target.get("outcomePrices", [])
    if isinstance(prices_raw, str):
        try:
            prices = json.loads(prices_raw)
        except Exception:
            return None
    else:
        prices = prices_raw or []

    # Parse outcome names (same encoding quirk).
    names_raw = target.get("outcomes", '["Yes","No"]')
    if isinstance(names_raw, str):
        try:
            names = json.loads(names_raw)
        except Exception:
            names = ["Yes", "No"]
    else:
        names = names_raw or ["Yes", "No"]

    # Match our outcome by name to find its index in the prices list.
    our_idx = next(
        (i for i, n in enumerate(names) if (n or "").strip().lower() == outcome_name),
        None,
    )
    if our_idx is None or our_idx >= len(prices):
        return None

    try:
        price = float(prices[our_idx])
    except (ValueError, TypeError):
        return None

    if price >= 0.95:
        logger.info(
            f"[Resolve] Market {market_id[:16]}... RESOLVED WIN "
            f"(outcome='{outcome_name}', price={price:.2f})"
        )
        return 1.0
    if price <= 0.05:
        logger.info(
            f"[Resolve] Market {market_id[:16]}... RESOLVED LOSS "
            f"(outcome='{outcome_name}', price={price:.2f})"
        )
        return 0.0

    # Price is mid-range on a closed market — unusual, skip and let TP/SL handle it.
    return None


async def _get_current_price(db, market_id: str, outcome_id: int) -> Optional[float]:
    """Latest mid-price from price_snapshots for this outcome."""
    return await db.fetchval(
        """
        SELECT mid_price FROM price_snapshots
        WHERE market_id=? AND outcome_id=?
        ORDER BY timestamp DESC LIMIT 1
        """,
        (market_id, outcome_id),
    )


async def _close_position(
    db,
    pos_id: int,
    bot_address: str,
    market_id: str,
    outcome_id: int,
    shares: float,
    entry_price: float,
    exit_price: float,
    reason: str,
    asset_id: Optional[str],
    paper_mode: bool,
) -> bool:
    """
    Close a position:
      - Paper: deduct nothing (already paid at open), add proceeds back to balance.
      - Live:  place SELL order on CLOB.
    Records in closed_positions regardless.
    """
    realized_pnl  = round((exit_price - entry_price) * shares, 4)
    realized_edge = round(exit_price - entry_price, 4)
    now_str       = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if paper_mode:
        # Restore the proceeds (entry cost + profit) to paper balance
        proceeds = round(exit_price * shares, 4)
        settings.app.paper_balance = round(settings.app.paper_balance + proceeds, 4)
        success = True
        logger.info(
            f"[Manage][PAPER] {reason} — "
            f"market {market_id[:16]}... | "
            f"entry={entry_price:.4f} exit={exit_price:.4f} "
            f"shares={shares:.4f} pnl=${realized_pnl:+.4f}"
        )
    else:
        success = await _live_sell(asset_id, exit_price, shares)
        if not success:
            return False
        logger.info(
            f"[Manage][LIVE] {reason} — "
            f"market {market_id[:16]}... | "
            f"entry={entry_price:.4f} exit={exit_price:.4f} "
            f"shares={shares:.4f} pnl=${realized_pnl:+.4f}"
        )

    # Record in closed_positions
    await db.execute(
        "INSERT INTO closed_positions"
        " (trader_address, market_id, outcome_id,"
        "  buy_size, buy_avg_price, sell_size, sell_avg_price,"
        "  realized_pnl, realized_edge, closed_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            bot_address, market_id, outcome_id,
            shares, entry_price,
            shares, exit_price,
            realized_pnl, realized_edge,
            now_str,
        ),
    )
    # Remove from open positions
    await db.execute(
        "DELETE FROM position_snapshots WHERE id=?", (pos_id,)
    )
    return True


async def _live_sell(asset_id: Optional[str], price: float, size: float) -> bool:
    """Place a GTC SELL order on the CLOB."""
    try:
        from py_clob_client.client import ClobClient as OfficialClobClient
        from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, SELL
    except ImportError:
        logger.error("[Manage][LIVE] py-clob-client not installed.")
        return False

    pk = settings.polymarket.private_key
    if not pk or pk == "0x" + "0" * 64 or not pk.strip():
        logger.error("[Manage][LIVE] No private key — cannot place sell order.")
        return False
    if not asset_id:
        logger.error("[Manage][LIVE] No asset_id — cannot place sell order.")
        return False

    try:
        from py_clob_client.clob_types import ApiCreds
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
            OrderArgs(token_id=asset_id, price=price, size=size, side=SELL)
        )
        resp = client.post_order(signed_order, OrderType.GTC)
        logger.info(f"[Manage][LIVE] Sell order response: {resp}")
        return True
    except Exception as e:
        logger.error(f"[Manage][LIVE] Sell order failed: {e}")
        return False


async def manage_positions() -> int:
    """
    Check all open bot positions against TP/SL thresholds.
    Returns number of positions closed.
    """
    db          = get_db()
    paper_mode  = settings.app.paper_mode
    tp_pct      = float(settings.app.take_profit)   # e.g. 50.0
    sl_pct      = float(settings.app.stop_loss)     # e.g. 30.0
    bot_address = BOT_PAPER_ADDRESS if paper_mode else settings.polymarket.wallet_address

    if not bot_address:
        return 0

    open_rows = await db.fetchall(
        """
        SELECT ps.id, ps.market_id, ps.outcome_id,
               ps.current_size, ps.avg_entry_price,
               o.asset_id
        FROM position_snapshots ps
        JOIN outcomes o ON o.id = ps.outcome_id
        WHERE ps.trader_address=? AND ps.current_size > 0
        """,
        (bot_address,),
    )

    if not open_rows:
        return 0

    closed = 0
    for row in open_rows:
        market_id   = row["market_id"]
        outcome_id  = row["outcome_id"]
        shares      = float(row["current_size"])
        entry_price = float(row["avg_entry_price"])
        asset_id    = row["asset_id"]
        pos_id      = row["id"]

        # ── 1. Resolution check (market fully closed on Polymarket) ────────────
        resolved_price = await _get_resolved_price(db, market_id, outcome_id)
        if resolved_price is not None:
            reason = (
                f"MARKET RESOLVED {'WIN' if resolved_price >= 0.95 else 'LOSS'} "
                f"(settlement={resolved_price:.2f})"
            )
            ok = await _close_position(
                db, pos_id, bot_address,
                market_id, outcome_id,
                shares, entry_price, resolved_price,
                reason, asset_id, paper_mode,
            )
            if ok:
                closed += 1
            continue  # no need to check TP/SL for a resolved market

        # ── 2. TP/SL check (market still live) ─────────────────────────────
        current_price = await _get_current_price(db, market_id, outcome_id)
        if current_price is None:
            continue

        current_price = float(current_price)

        tp_price = round(entry_price * (1.0 + tp_pct / 100.0), 4)
        sl_price = round(entry_price * (1.0 - sl_pct / 100.0), 4)

        reason = None
        if current_price >= tp_price:
            reason = f"TAKE PROFIT ({tp_pct:.0f}% TP hit: {current_price:.4f} >= {tp_price:.4f})"
        elif current_price <= sl_price:
            reason = f"STOP LOSS ({sl_pct:.0f}% SL hit: {current_price:.4f} <= {sl_price:.4f})"

        if reason:
            sell_price = round(min(0.99, current_price), 4)
            ok = await _close_position(
                db, pos_id, bot_address,
                market_id, outcome_id,
                shares, entry_price, sell_price,
                reason, asset_id, paper_mode,
            )
            if ok:
                closed += 1
        else:
            # Update unrealized_pnl so the UI reflects live P&L
            unrealized = round((current_price - entry_price) * shares, 4)
            await db.execute(
                "UPDATE position_snapshots SET unrealized_pnl=? WHERE id=?",
                (unrealized, pos_id),
            )

    if closed > 0:
        await db.commit()
        logger.info(f"[Manage] {closed} position(s) closed (TP/SL).")

    return closed
