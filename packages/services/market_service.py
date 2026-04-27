import logging
import traceback
from datetime import datetime, timedelta

from packages.db.database import get_db
from packages.ingestion.clients.polymarket_http import GammaClient
from packages.ingestion.normalize.markets import normalize_gamma_event

logger = logging.getLogger(__name__)


class MarketService:
    def __init__(self):
        self.db = get_db()
        self.gamma_client = GammaClient()

    async def refresh_active_markets(self, limit: int = 200):
        """Fetch active events from Gamma via pagination and upsert into SQLite."""
        logger.info(f"Refreshing up to {limit} active events from Gamma...")
        try:
            raw_events = await self.gamma_client.get_events_paginated(max_events=limit)
            seen_tags: set = set()

            for i, raw_event in enumerate(raw_events):
                event, markets, outcomes, tags, price_data = normalize_gamma_event(raw_event)

                # ── Event ────────────────────────────────────────────────────
                exists = await self.db.fetchval(
                    "SELECT id FROM events WHERE id = ? LIMIT 1", (event.id,)
                )
                if exists is None:
                    await self.db.execute(
                        "INSERT INTO events (id, title, description, category, active, closed)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (event.id, event.title, event.description,
                         event.category, int(event.active), int(event.closed)),
                    )
                else:
                    await self.db.execute(
                        "UPDATE events SET title=?, description=?, category=?,"
                        " active=?, closed=? WHERE id=?",
                        (event.title, event.description, event.category,
                         int(event.active), int(event.closed), event.id),
                    )

                # ── Markets ──────────────────────────────────────────────────
                for market in markets:
                    mkt_exists = await self.db.fetchval(
                        "SELECT id FROM markets WHERE id = ? LIMIT 1", (market.id,)
                    )
                    end_iso = (
                        market.end_date_iso.strftime("%Y-%m-%d %H:%M:%S")
                        if market.end_date_iso else None
                    )
                    if mkt_exists is None:
                        await self.db.execute(
                            "INSERT INTO markets"
                            " (id, event_id, question, slug, active, closed,"
                            "  resolution_source, end_date_iso, market_type)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (market.id, market.event_id, market.question,
                             market.slug, int(market.active), int(market.closed),
                             market.resolution_source, end_iso, market.market_type),
                        )
                    else:
                        await self.db.execute(
                            "UPDATE markets SET question=?, active=?, closed=?,"
                            " resolution_source=?, end_date_iso=?, market_type=?"
                            " WHERE id=?",
                            (market.question, int(market.active), int(market.closed),
                             market.resolution_source, end_iso,
                             market.market_type, market.id),
                        )

                # ── Outcomes ─────────────────────────────────────────────────
                outcome_ids: dict = {}
                for outcome in outcomes:
                    existing_id = await self.db.fetchval(
                        "SELECT id FROM outcomes WHERE market_id=? AND name=? LIMIT 1",
                        (outcome.market_id, outcome.name),
                    )
                    if existing_id is not None:
                        await self.db.execute(
                            "UPDATE outcomes SET asset_id=? WHERE id=?",
                            (outcome.asset_id, existing_id),
                        )
                        outcome_ids[(outcome.market_id, outcome.name)] = existing_id
                    else:
                        cur = await self.db.execute(
                            "INSERT INTO outcomes (market_id, name, asset_id)"
                            " VALUES (?, ?, ?)",
                            (outcome.market_id, outcome.name, outcome.asset_id),
                        )
                        outcome_ids[(outcome.market_id, outcome.name)] = cur.lastrowid

                # ── Tags ─────────────────────────────────────────────────────
                for tag in tags:
                    tag_key = (tag.market_id, tag.tag)
                    if tag_key in seen_tags:
                        continue
                    tag_exists = await self.db.fetchval(
                        "SELECT id FROM market_tags"
                        " WHERE market_id=? AND tag=? LIMIT 1",
                        (tag.market_id, tag.tag),
                    )
                    if tag_exists is None:
                        await self.db.execute(
                            "INSERT INTO market_tags (market_id, tag) VALUES (?, ?)",
                            (tag.market_id, tag.tag),
                        )
                    seen_tags.add(tag_key)

                # ── Price snapshots ──────────────────────────────────────────
                now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                for pd_row in price_data:
                    oid = outcome_ids.get((pd_row["market_id"], pd_row["outcome_name"]))
                    if oid is None:
                        continue
                    pv = pd_row["price"]
                    # Use real CLOB spread/ask from the Gamma API when available.
                    # These fields are set on the primary (Yes) outcome only.
                    api_ask    = pd_row.get("api_best_ask")
                    api_spread = pd_row.get("api_spread")
                    if api_ask is not None and api_spread is not None:
                        best_ask = min(1.0, api_ask)
                        best_bid = max(0.0, api_ask - api_spread)
                    else:
                        best_ask = min(1.0, pv + 0.01)
                        best_bid = max(0.0, pv - 0.01)
                    await self.db.execute(
                        "INSERT INTO price_snapshots"
                        " (market_id, outcome_id, best_bid, best_ask, mid_price, timestamp)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (pd_row["market_id"], oid, best_bid, best_ask, pv, now),
                    )

                if (i + 1) % 50 == 0:
                    await self.db.commit()
                    logger.debug(f"Committed batch of 50 events.")

            await self.db.commit()
            logger.info(f"Refreshed {len(raw_events)} events with price snapshots.")

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to refresh markets: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            await self.gamma_client.close()

    async def upsert_event(self, raw_event: dict) -> None:
        """Normalize and upsert a single Gamma event."""
        event, markets, outcomes, tags, price_data = normalize_gamma_event(raw_event)

        exists = await self.db.fetchval(
            "SELECT id FROM events WHERE id = ? LIMIT 1", (event.id,)
        )
        if exists is None:
            await self.db.execute(
                "INSERT INTO events (id, title, description, category, active, closed)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (event.id, event.title, event.description,
                 event.category, int(event.active), int(event.closed)),
            )
        else:
            await self.db.execute(
                "UPDATE events SET title=?, description=?, category=?,"
                " active=?, closed=? WHERE id=?",
                (event.title, event.description, event.category,
                 int(event.active), int(event.closed), event.id),
            )

        for market in markets:
            mkt_exists = await self.db.fetchval(
                "SELECT id FROM markets WHERE id = ? LIMIT 1", (market.id,)
            )
            end_iso = (
                market.end_date_iso.strftime("%Y-%m-%d %H:%M:%S")
                if market.end_date_iso else None
            )
            if mkt_exists is None:
                await self.db.execute(
                    "INSERT INTO markets"
                    " (id, event_id, question, slug, active, closed,"
                    "  resolution_source, end_date_iso, market_type)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (market.id, market.event_id, market.question,
                     market.slug, int(market.active), int(market.closed),
                     market.resolution_source, end_iso, market.market_type),
                )
            else:
                await self.db.execute(
                    "UPDATE markets SET question=?, active=?, closed=?,"
                    " resolution_source=?, end_date_iso=?, market_type=?"
                    " WHERE id=?",
                    (market.question, int(market.active), int(market.closed),
                     market.resolution_source, end_iso,
                     market.market_type, market.id),
                )

        outcome_ids: dict = {}
        for outcome in outcomes:
            existing_id = await self.db.fetchval(
                "SELECT id FROM outcomes WHERE market_id=? AND name=? LIMIT 1",
                (outcome.market_id, outcome.name),
            )
            if existing_id is not None:
                await self.db.execute(
                    "UPDATE outcomes SET asset_id=? WHERE id=?",
                    (outcome.asset_id, existing_id),
                )
                outcome_ids[(outcome.market_id, outcome.name)] = existing_id
            else:
                cur = await self.db.execute(
                    "INSERT INTO outcomes (market_id, name, asset_id) VALUES (?, ?, ?)",
                    (outcome.market_id, outcome.name, outcome.asset_id),
                )
                outcome_ids[(outcome.market_id, outcome.name)] = cur.lastrowid

        seen_tags: set = set()
        for tag in tags:
            tag_key = (tag.market_id, tag.tag)
            if tag_key in seen_tags:
                continue
            tag_exists = await self.db.fetchval(
                "SELECT id FROM market_tags WHERE market_id=? AND tag=? LIMIT 1",
                (tag.market_id, tag.tag),
            )
            if tag_exists is None:
                await self.db.execute(
                    "INSERT INTO market_tags (market_id, tag) VALUES (?, ?)",
                    (tag.market_id, tag.tag),
                )
            seen_tags.add(tag_key)

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for pd_row in price_data:
            oid = outcome_ids.get((pd_row["market_id"], pd_row["outcome_name"]))
            if oid is None:
                continue
            pv = pd_row["price"]
            api_ask    = pd_row.get("api_best_ask")
            api_spread = pd_row.get("api_spread")
            if api_ask is not None and api_spread is not None:
                best_ask = min(1.0, api_ask)
                best_bid = max(0.0, api_ask - api_spread)
            else:
                best_ask = min(1.0, pv + 0.01)
                best_bid = max(0.0, pv - 0.01)
            await self.db.execute(
                "INSERT INTO price_snapshots"
                " (market_id, outcome_id, best_bid, best_ask, mid_price, timestamp)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (pd_row["market_id"], oid, best_bid, best_ask, pv, now),
            )

        await self.db.commit()

    async def prune_resolved_markets(self, older_than_days: int = 7):
        """Delete markets that have been closed for more than X days."""
        logger.info(f"Pruning markets resolved more than {older_than_days} days ago...")
        cutoff = (
            datetime.utcnow() - timedelta(days=older_than_days)
        ).strftime("%Y-%m-%d %H:%M:%S")

        market_ids = await self.db.fetchall(
            "SELECT id FROM markets WHERE closed=1 AND active=0"
        )
        if not market_ids:
            return

        count = 0
        for row in market_ids:
            m_id = row["id"]
            latest = await self.db.fetchval(
                "SELECT MAX(timestamp) FROM price_snapshots WHERE market_id=?", (m_id,)
            )
            if latest and latest > cutoff:
                continue
            await self.db.execute(
                "DELETE FROM price_snapshots WHERE market_id=?", (m_id,)
            )
            await self.db.execute(
                "DELETE FROM market_signal_snapshots WHERE market_id=?", (m_id,)
            )
            await self.db.execute(
                "DELETE FROM outcomes WHERE market_id=?", (m_id,)
            )
            await self.db.execute(
                "DELETE FROM market_tags WHERE market_id=?", (m_id,)
            )
            await self.db.execute(
                "DELETE FROM markets WHERE id=?", (m_id,)
            )
            count += 1

        await self.db.commit()
        if count > 0:
            logger.info(f"Pruned {count} resolved markets.")
