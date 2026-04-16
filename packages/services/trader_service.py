import asyncio
import logging
from datetime import datetime

from packages.db.database import get_db
from packages.ingestion.clients.polymarket_http import ClobClient
from packages.ingestion.normalize.trades import normalize_clob_trade

logger = logging.getLogger(__name__)


class TraderService:
    def __init__(self):
        self.db = get_db()
        self.clob_client = ClobClient()

    async def sync_trades_for_market(self, market_id: str):
        """Fetch latest trades for all outcomes of a given market."""
        logger.info(f"Syncing trades for market {market_id}...")

        outcomes = await self.db.fetchall(
            "SELECT id, name, asset_id FROM outcomes WHERE market_id=?", (market_id,)
        )

        for outcome in outcomes:
            if not outcome.asset_id:
                logger.warning(
                    f"Outcome {outcome.name} (id {outcome.id}) has no asset_id. Skipping."
                )
                continue

            try:
                raw_trades = await self.clob_client.get_trades(outcome.asset_id)
                if not raw_trades:
                    continue

                for raw_trade in raw_trades:
                    trader_address = (
                        raw_trade.get("maker_address")
                        or raw_trade.get("trader_address")
                        or raw_trade.get("transactor")
                    )
                    if not trader_address:
                        continue

                    # Ensure wallet exists
                    wallet_exists = await self.db.fetchval(
                        "SELECT address FROM trader_wallets WHERE address=? LIMIT 1",
                        (trader_address,),
                    )
                    if not wallet_exists:
                        await self.db.execute(
                            "INSERT INTO trader_wallets (address) VALUES (?)",
                            (trader_address,),
                        )

                    # Idempotent insert: skip if transaction_hash already stored
                    tx_hash = (
                        raw_trade.get("transaction_hash")
                        or raw_trade.get("transactionHash", "")
                    )
                    if tx_hash:
                        existing = await self.db.fetchval(
                            "SELECT id FROM trades WHERE transaction_hash=? LIMIT 1",
                            (tx_hash,),
                        )
                        if existing:
                            continue

                    trade = normalize_clob_trade(raw_trade, market_id, outcome.id)
                    await self.db.execute(
                        "INSERT INTO trades"
                        " (market_id, outcome_id, trader_address, side, price,"
                        "  size, notional, transaction_hash, timestamp)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            trade["market_id"], trade["outcome_id"],
                            trade["trader_address"], trade["side"],
                            trade["price"], trade["size"], trade["notional"],
                            trade["transaction_hash"], trade["timestamp"],
                        ),
                    )

                    await self._update_position(
                        trader_address, market_id, outcome.id, trade
                    )

                await self.db.commit()
                logger.info(f"Synced trades for outcome {outcome.name}.")

            except Exception as e:
                await self.db.rollback()
                logger.error(f"Failed to sync trades: {e}")
                continue

    async def _update_position(
        self,
        trader_address: str,
        market_id: str,
        outcome_id: int,
        trade: dict,
    ):
        """Maintain a running net-position per (trader, market, outcome)."""
        side  = (trade["side"] or "").lower()
        size  = float(trade["size"] or 0.0)
        price = float(trade["price"] or 0.0)

        pos = await self.db.fetchone(
            "SELECT * FROM position_snapshots"
            " WHERE trader_address=? AND market_id=? AND outcome_id=? LIMIT 1",
            (trader_address, market_id, outcome_id),
        )

        if side in ("buy", "yes"):
            if pos is None:
                await self.db.execute(
                    "INSERT INTO position_snapshots"
                    " (trader_address, market_id, outcome_id,"
                    "  current_size, avg_entry_price, unrealized_pnl)"
                    " VALUES (?, ?, ?, ?, ?, 0.0)",
                    (trader_address, market_id, outcome_id, size, price),
                )
            else:
                total_size = pos.current_size + size
                new_avg = (
                    (pos.avg_entry_price * pos.current_size + price * size) / total_size
                    if total_size > 0 else price
                )
                await self.db.execute(
                    "UPDATE position_snapshots"
                    " SET current_size=?, avg_entry_price=?"
                    " WHERE trader_address=? AND market_id=? AND outcome_id=?",
                    (total_size, new_avg, trader_address, market_id, outcome_id),
                )

        elif side in ("sell", "no") and pos is not None and pos.current_size > 0:
            sell_size    = min(size, pos.current_size)
            realized_pnl = sell_size * (price - pos.avg_entry_price)
            edge = (
                realized_pnl / (sell_size * pos.avg_entry_price)
                if pos.avg_entry_price > 0 else 0.0
            )
            remaining = pos.current_size - sell_size

            if remaining <= 0.0001:
                now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                await self.db.execute(
                    "INSERT INTO closed_positions"
                    " (trader_address, market_id, outcome_id,"
                    "  buy_size, buy_avg_price, sell_size, sell_avg_price,"
                    "  realized_pnl, realized_edge, closed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        trader_address, market_id, outcome_id,
                        pos.current_size, pos.avg_entry_price,
                        sell_size, price,
                        realized_pnl, edge, now,
                    ),
                )
                await self.db.execute(
                    "DELETE FROM position_snapshots"
                    " WHERE trader_address=? AND market_id=? AND outcome_id=?",
                    (trader_address, market_id, outcome_id),
                )
                logger.debug(
                    f"Closed position {trader_address[:8]}... "
                    f"market={market_id}: PnL=${realized_pnl:.2f}"
                )
            else:
                await self.db.execute(
                    "UPDATE position_snapshots SET current_size=?"
                    " WHERE trader_address=? AND market_id=? AND outcome_id=?",
                    (remaining, trader_address, market_id, outcome_id),
                )

    async def cleanup_ghost_positions(self):
        """Remove PositionSnapshot records for markets that are no longer active."""
        logger.info("Cleaning up ghost positions for resolved markets...")
        try:
            ghosts = await self.db.fetchall(
                "SELECT ps.id FROM position_snapshots ps"
                " JOIN markets m ON m.id = ps.market_id"
                " WHERE m.active = 0"
            )
            if ghosts:
                ids = [r["id"] for r in ghosts]
                placeholders = ",".join("?" * len(ids))
                await self.db.execute(
                    f"DELETE FROM position_snapshots WHERE id IN ({placeholders})",
                    ids,
                )
                await self.db.commit()
                logger.info(f"Pruned {len(ids)} ghost positions from resolved markets.")
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to cleanup ghost positions: {e}")

    async def sync_global_trade_feed(self) -> int:
        """
        Ingest the latest public trade feed from data-api.polymarket.com.

        Fetches up to 500 recent trades globally, then matches each trade to a
        known outcome in our DB via the `asset` (token_id) field.  This is the
        primary unauthenticated trade-ingestion path — no API credentials needed.

        Returns the number of new trades stored.
        """
        raw_trades = await self.clob_client.get_global_trade_feed(limit=500)
        if not raw_trades:
            logger.debug("Global trade feed returned no data.")
            return 0

        # Build asset_id → (market_id, outcome_id) lookup from DB
        outcome_rows = await self.db.fetchall(
            "SELECT o.asset_id, o.id AS outcome_id, o.market_id "
            "FROM outcomes o WHERE o.asset_id IS NOT NULL"
        )
        asset_map: dict = {
            r["asset_id"]: (r["market_id"], r["outcome_id"])
            for r in outcome_rows
        }
        if not asset_map:
            logger.warning("No outcomes with asset_id in DB — cannot match global feed.")
            return 0

        new_count = 0
        for raw in raw_trades:
            asset_id = raw.get("asset", "")
            if not asset_id or asset_id not in asset_map:
                continue

            market_id, outcome_id = asset_map[asset_id]
            trader_address = (
                raw.get("proxyWallet") or raw.get("maker_address")
                or raw.get("trader_address") or ""
            )
            if not trader_address:
                continue

            tx_hash = raw.get("transactionHash") or raw.get("transaction_hash") or ""
            # Skip duplicates
            if tx_hash:
                exists = await self.db.fetchval(
                    "SELECT id FROM trades WHERE transaction_hash=? LIMIT 1", (tx_hash,)
                )
                if exists:
                    continue

            # Ensure wallet tracked
            wallet_exists = await self.db.fetchval(
                "SELECT address FROM trader_wallets WHERE address=? LIMIT 1",
                (trader_address,),
            )
            if not wallet_exists:
                await self.db.execute(
                    "INSERT INTO trader_wallets (address) VALUES (?)", (trader_address,)
                )

            from packages.ingestion.normalize.trades import normalize_clob_trade
            trade = normalize_clob_trade(raw, market_id, outcome_id)
            await self.db.execute(
                "INSERT INTO trades"
                " (market_id, outcome_id, trader_address, side, price,"
                "  size, notional, transaction_hash, timestamp)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    trade["market_id"], trade["outcome_id"],
                    trade["trader_address"], trade["side"],
                    trade["price"], trade["size"], trade["notional"],
                    trade["transaction_hash"], trade["timestamp"],
                ),
            )
            await self._update_position(trader_address, market_id, outcome_id, trade)
            new_count += 1

        if new_count > 0:
            await self.db.commit()
            logger.info(f"[GlobalFeed] Stored {new_count} new trades from data-api feed.")
        return new_count

    async def sync_per_market_trade_feed(
        self, asset_ids: list, limit_per_market: int = 100
    ) -> int:
        """
        Fetch trades for each asset_id from the unauthenticated public data-api
        endpoint (/trades?asset=<id>) and store new records in the DB.

        Uses asyncio.as_completed so DB writes and commits happen as each
        market's HTTP response arrives — the UI sees data stream in every few
        seconds rather than waiting for all 400 fetches to complete.

        Returns total number of new trades stored.
        """
        # Build asset_id → (market_id, outcome_id) lookup once
        outcome_rows = await self.db.fetchall(
            "SELECT o.asset_id, o.id AS outcome_id, o.market_id "
            "FROM outcomes o WHERE o.asset_id IS NOT NULL"
        )
        asset_map: dict = {
            r["asset_id"]: (r["market_id"], r["outcome_id"])
            for r in outcome_rows
        }

        valid_ids = [a for a in asset_ids if a in asset_map]
        if not valid_ids:
            return 0

        sem = asyncio.Semaphore(20)

        async def _fetch(asset_id: str):
            async with sem:
                return asset_id, await self.clob_client.get_market_trade_feed(
                    asset_id, limit=limit_per_market
                )

        total_new   = 0
        batch_new   = 0
        COMMIT_EVERY = 20   # commit to DB after every N markets resolved

        tasks = [asyncio.ensure_future(_fetch(a)) for a in valid_ids]
        done_count = 0

        for coro in asyncio.as_completed(tasks):
            try:
                asset_id, raw_trades = await coro
            except Exception as exc:
                logger.debug(f"[PerMarketFeed] fetch error: {exc}")
                done_count += 1
                continue

            done_count += 1
            if not raw_trades:
                continue

            market_id, outcome_id = asset_map[asset_id]

            for raw in raw_trades:
                trader_address = (
                    raw.get("proxyWallet") or raw.get("maker_address")
                    or raw.get("trader_address") or ""
                )
                if not trader_address:
                    continue

                tx_hash = raw.get("transactionHash") or raw.get("transaction_hash") or ""
                if tx_hash:
                    exists = await self.db.fetchval(
                        "SELECT id FROM trades WHERE transaction_hash=? LIMIT 1",
                        (tx_hash,),
                    )
                    if exists:
                        continue

                wallet_exists = await self.db.fetchval(
                    "SELECT address FROM trader_wallets WHERE address=? LIMIT 1",
                    (trader_address,),
                )
                if not wallet_exists:
                    await self.db.execute(
                        "INSERT INTO trader_wallets (address) VALUES (?)",
                        (trader_address,),
                    )

                trade = normalize_clob_trade(raw, market_id, outcome_id)
                await self.db.execute(
                    "INSERT INTO trades"
                    " (market_id, outcome_id, trader_address, side, price,"
                    "  size, notional, transaction_hash, timestamp)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        trade["market_id"], trade["outcome_id"],
                        trade["trader_address"], trade["side"],
                        trade["price"], trade["size"], trade["notional"],
                        trade["transaction_hash"], trade["timestamp"],
                    ),
                )
                await self._update_position(trader_address, market_id, outcome_id, trade)
                total_new += 1
                batch_new += 1

            # Commit incrementally so the UI sees new data without waiting for all 400
            if done_count % COMMIT_EVERY == 0 and batch_new > 0:
                await self.db.commit()
                logger.info(
                    f"[PerMarketFeed] {done_count}/{len(valid_ids)} markets — "
                    f"{total_new} trades stored so far."
                )
                batch_new = 0

        # Final commit for the remainder
        if batch_new > 0:
            await self.db.commit()

        if total_new > 0:
            logger.info(
                f"[PerMarketFeed] Done — {total_new} new trades across {len(valid_ids)} assets."
            )
        return total_new

    async def close(self):
        await self.clob_client.close()

    async def reconcile_with_onchain_balances(self, trader_address: str):
        """Safety sync placeholder — requires web3 provider for live implementation."""
        logger.info(f"Reconciling on-chain balances for {trader_address[:12]}...")
        pass
