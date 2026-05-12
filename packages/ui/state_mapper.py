import json
from typing import Any, Dict, List

from packages.core.config import settings, log_buffer
from packages.db.database import get_db

# Maps the active strategy setting → signal_type values stored in market_signal_snapshots.
# Each strategy shows ONLY its own signal type — no cross-contamination.
_STRATEGY_SIGNAL_TYPES: Dict[str, List[str]] = {
    "bayesian_ensemble":    ["bayesian_ensemble"],
    "conservative_snw":     ["conservative_snw"],
    "aggressive_whale":     ["aggressive_whale"],
    "specialist_precision": ["specialist_precision"],
    "long_range":           ["long_range"],
    "volatility":           ["volatility"],
    "black_swan":           ["black_swan"],
    "no_bias":              ["no_bias"],
    "laddering":            ["weather_laddering"],
    "disaster":             ["weather_disaster", "seismic"],
    "weather_prediction":   ["weather_prediction"],
}

_TRADER_STRATEGIES: frozenset = frozenset({
    "bayesian_ensemble", "conservative_snw", "aggressive_whale", "specialist_precision",
})

_STRATEGY_FALLBACK_TAGS: Dict[str, List[str]] = {
    "bayesian_ensemble":    ["Politics", "Crypto", "Entertainment", "Sports", "Science"],
    "conservative_snw":     ["Politics", "Crypto"],
    "aggressive_whale":     ["Politics", "Crypto", "High Volume"],
    "specialist_precision": ["Politics", "Science", "Business"],
    "no_bias":              ["Politics", "Sports", "Pop Culture"],
    "black_swan":           ["Science", "Natural Disasters", "Global Warming"],
    "long_range":           ["Politics", "Science", "Economic"],
    "volatility":           ["Crypto", "Sports", "Politics"],
    "weather_prediction":   ["Weather"],
    "laddering":            ["Weather"],
    "disaster":             ["Natural Disasters", "Earthquakes"],
}


async def map_db_to_bot_state(
    is_trading: bool = False,
    is_syncing: bool = False,
    is_scanning: bool = False,
    is_initializing: bool = False,
) -> Dict[str, Any]:
    """
    Map the SQLite database state to the bot_state dict consumed by the dashboard.
    All metrics are derived from real data — no hardcoded placeholders.
    """
    db = get_db()
    active_strategy = settings.strategy

    # ------------------------------------------------------------------ #
    # 1. Core metrics                                                       #
    # ------------------------------------------------------------------ #
    # Total bot trades = open positions + closed positions
    bot_address = (
        settings.polymarket.wallet_address
        if not settings.app.paper_mode
        else "0xbot_paper_wallet"
    )
    open_trade_count = await db.fetchval(
        "SELECT COUNT(id) FROM position_snapshots WHERE trader_address=?",
        (bot_address,),
    ) or 0

    win_row = await db.fetchone(
        """
        SELECT
            COUNT(id)                                           AS total,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(realized_pnl)                                   AS total_pnl
        FROM closed_positions
        WHERE trader_address = ?
        """,
        (bot_address,),
    )
    total_closed = int(win_row.total or 0) if win_row else 0
    total_wins   = int(win_row.wins   or 0) if win_row else 0
    total_trades = open_trade_count + total_closed
    win_rate_pct = round((total_wins / total_closed) * 100, 1) if total_closed > 0 else 0.0
    total_profit = float(win_row.total_pnl or 0.0) if win_row else 0.0

    # ------------------------------------------------------------------ #
    # 2. Market signal snapshots → scanned_markets + dev_check_logs        #
    # ------------------------------------------------------------------ #
    signal_types    = _STRATEGY_SIGNAL_TYPES.get(active_strategy, ["bayesian_ensemble"])
    placeholders    = ",".join("?" * len(signal_types))

    signal_rows = await db.fetchall(
        f"""
        SELECT s.*, m.question
        FROM market_signal_snapshots s
        JOIN markets m ON m.id = s.market_id
        WHERE s.signal_type IN ({placeholders})
        ORDER BY s.signal_strength DESC
        LIMIT 500
        """,
        tuple(signal_types),
    )

    scanned_markets = []
    dev_check_logs  = []

    for row in signal_rows:
        alpha_score = min(100.0, row.signal_strength * 20)
        liquidity   = "High"
        reasoning   = row.explanation or ""
        if isinstance(reasoning, str) and reasoning.startswith("[") and " Liquidity]" in reasoning:
            parts     = reasoning.split(" Liquidity] ", 1)
            liquidity = parts[0].replace("[", "")
            reasoning = parts[1] if len(parts) > 1 else reasoning

        scanned_markets.append({
            "question":    row.question,
            "alpha_score": alpha_score,
            "bias":        row.directional_bias,
            "liquidity":   liquidity,
            "reasoning":   reasoning,
            "warming_up":  False,
        })
        raw_traders = row.top_traders
        if isinstance(raw_traders, str):
            try:
                raw_traders = json.loads(raw_traders)
            except Exception:
                raw_traders = []
        dev_check_logs.append({
            "timestamp":        row.created_at.strftime("%H:%M:%S"),
            "question":         row.question,
            "directional_bias": row.directional_bias,
            "explanation":      row.explanation,
            "top_traders":      raw_traders or [],
            "signal_strength":  row.signal_strength,
        })

    # Real signal count before any fallback is added
    real_signal_count = len(scanned_markets)

    # ------------------------------------------------------------------ #
    # 2b. Fallback: show relevant markets when no signals yet              #
    # ------------------------------------------------------------------ #
    if not scanned_markets:
        fallback_tags = _STRATEGY_FALLBACK_TAGS.get(active_strategy)
        if fallback_tags:
            fb_placeholders = ",".join("?" * len(fallback_tags))
            fallback_rows = await db.fetchall(
                f"""
                SELECT DISTINCT m.id, m.question
                FROM markets m
                JOIN market_tags mt ON mt.market_id = m.id
                WHERE m.active=1 AND m.closed=0
                  AND (LOWER(m.market_type) IN ('binary','') OR m.market_type IS NULL)
                  AND LOWER(mt.tag) IN ({fb_placeholders})
                LIMIT 50
                """,
                tuple(t.lower() for t in fallback_tags),
            )
        else:
            fallback_rows = await db.fetchall(
                "SELECT id, question FROM markets"
                " WHERE active=1 AND closed=0"
                " AND (LOWER(market_type) IN ('binary','') OR market_type IS NULL)"
                " ORDER BY id DESC LIMIT 50"
            )

        for m in fallback_rows:
            scanned_markets.append({
                "question":    m.question or m.id,
                "alpha_score": 0.0,
                "bias":        "N/A",
                "liquidity":   "N/A",
                "reasoning":   "Signal engine scanning — scores pending...",
                "warming_up":  True,
            })

    # ------------------------------------------------------------------ #
    # 3. Recent skilled-trader activity feed (trader strategies only)     #
    # ------------------------------------------------------------------ #
    news_events = []
    if active_strategy in _TRADER_STRATEGIES:
        # One row per unique market — most recent skilled trade in each.
        # Prevents the feed from showing 10 trades all from the same market
        # when a large batch was just ingested from a single high-volume market.
        feed_rows = await db.fetchall(
            """
            SELECT t.trader_address, t.side, t.notional, t.size,
                   tc.label, m.question
            FROM trades t
            JOIN trader_classifications tc ON tc.address = t.trader_address
            JOIN markets m ON m.id = t.market_id
            WHERE tc.label IN ('whale','serious_non_whale','topic_specialist')
              AND t.id IN (
                  SELECT MAX(t2.id)
                  FROM trades t2
                  JOIN trader_classifications tc2 ON tc2.address = t2.trader_address
                  WHERE tc2.label IN ('whale','serious_non_whale','topic_specialist')
                  GROUP BY t2.market_id
              )
            ORDER BY t.timestamp DESC
            LIMIT 10
            """
        )
        for row in feed_rows:
            notional_val = float(row.notional if row.notional is not None else (row.size or 0.0))
            direction = "+" if (row.side or "").upper() in ("YES", "BUY") else "-"
            news_events.append({
                "trader":   row.trader_address[:12] + "...",
                "label":    row.label.upper(),
                "activity": f"{(row.side or '').upper()} ${notional_val:.2f}",
                "impact":   f"{direction}{notional_val / 100:.1f}%",
                "summary":  f"Entered {row.side} on '{row.question[:30]}...'",
            })

    # ------------------------------------------------------------------ #
    # 4. Open Positions (for the bot wallet)                               #
    # ------------------------------------------------------------------ #
    pos_rows = await db.fetchall(
        """
        SELECT ps.current_size, ps.avg_entry_price, ps.unrealized_pnl,
               m.question, o.name AS outcome_name
        FROM position_snapshots ps
        JOIN markets m  ON m.id  = ps.market_id
        JOIN outcomes o ON o.id  = ps.outcome_id
        WHERE ps.current_size > 0 AND ps.trader_address = ?
        ORDER BY ps.snapshot_at DESC
        """,
        (bot_address,),
    )
    open_positions = [
        {
            "market":       row.question,
            "side":         row.outcome_name.upper(),
            "size":         round(row.current_size * row.avg_entry_price, 2),  # USDC cost
            "shares":       round(row.current_size, 4),
            "price":        round(row.avg_entry_price, 3),
            "unrealized":   round(row.unrealized_pnl, 2),
            "signal_type":  "Live",
        }
        for row in pos_rows
    ]

    # ------------------------------------------------------------------ #
    # 5. Resolved Positions (Profit/Loss History)                          #
    # ------------------------------------------------------------------ #
    resolved_rows = await db.fetchall(
        """
        SELECT cp.buy_size, cp.realized_pnl, cp.closed_at,
               m.question, o.name AS outcome_name
        FROM closed_positions cp
        JOIN markets m  ON m.id  = cp.market_id
        JOIN outcomes o ON o.id  = cp.outcome_id
        WHERE cp.trader_address = ?
        ORDER BY cp.closed_at DESC
        LIMIT 20
        """,
        (bot_address,),
    )
    resolved_positions = [
        {
            "market":      row.question,
            "side":        row.outcome_name.upper(),
            "size":        round(row.buy_size, 2),
            "profit":      round(row.realized_pnl, 2),
            "resolved_at": row.closed_at.strftime("%Y-%m-%d %H:%M"),
        }
        for row in resolved_rows
    ]

    # ------------------------------------------------------------------ #
    # 6. Assemble bot_state                                                #
    # ------------------------------------------------------------------ #
    return {
        "is_trading": is_trading,
        "is_syncing": is_syncing,
        "is_scanning": is_scanning,
        "is_initializing": is_initializing,
        "metrics": {
            "total_trades": total_trades,
            "win_rate":     win_rate_pct,
            "total_profit": round(total_profit, 2),
            "balance":      round(settings.app.paper_balance, 2),
        },
        "total_scanned":      real_signal_count,
        "scanned_markets":    scanned_markets,
        "open_positions":     open_positions,
        "resolved_positions": resolved_positions,
        "news_events":        news_events,
        "dev_check_logs":     dev_check_logs,
        "logs": log_buffer.get_entries(60),
        "config": {
            "paper_mode":    settings.app.paper_mode,
            "trade_amount":  settings.app.trade_amount,
            "min_edge":      settings.app.min_edge,
            "scan_interval": settings.app.scan_interval,
            "strategy":      settings.strategy,
            "paper_balance": settings.app.paper_balance,
            "max_trades":    settings.app.max_trades,
            "take_profit":   settings.app.take_profit,
            "stop_loss":     settings.app.stop_loss,
        },
    }
