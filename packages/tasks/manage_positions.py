"""
Position Manager — Take Profit & Stop Loss
==========================================
Runs on every signal cycle.  For each open bot position:

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
import logging
from datetime import datetime
from typing import Optional

from packages.db.database import get_db
from packages.core.config import settings
from packages.ingestion.clients.polymarket_http import (
    _derived_api_key, _derived_api_secret, _derived_api_passphrase,
)

logger = logging.getLogger(__name__)

BOT_PAPER_ADDRESS = "0xbot_paper_wallet"


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
