import asyncio
import logging

from packages.db.database import get_db
from packages.services.market_service import MarketService
from packages.ingestion.clients.polymarket_http import GammaClient

logger = logging.getLogger(__name__)

PRIORITY_TAG_IDS = [84, 103038, 496, 92, 74]


async def _fetch_all_tags() -> list:
    try:
        import httpx
        async with httpx.AsyncClient(base_url="https://gamma-api.polymarket.com", timeout=10.0) as c:
            r = await c.get("/tags")
            r.raise_for_status()
            return [t.get("id") for t in r.json() if t.get("id")]
    except Exception as e:
        logger.warning(f"Could not fetch tag list: {e}. Using priority tags only.")
        return PRIORITY_TAG_IDS


async def refresh_markets_for_strategy(strategy: str, limit: int = 100) -> int:
    """
    Fetch markets relevant to the given strategy and upsert into DB.

    limit  — maximum total events to fetch. Scales both the volume pass and
             the per-tag fetch so the caller controls how wide the scan is.
             Small (50-100) for quick initial loads; larger (200-400) for
             progressive expansion when signals are sparse.

    Returns the number of events upserted.
    """
    from packages.tasks.compute_signals import _STRATEGY_TAG_FILTER, _STRATEGY_KEYWORDS
    import httpx

    service = MarketService()

    tag_labels = _STRATEGY_TAG_FILTER.get(strategy, set())
    keywords   = _STRATEGY_KEYWORDS.get(strategy, [])

    logger.info(
        f"[Market Update] Fetching for strategy='{strategy}' "
        f"(limit={limit}, tags={tag_labels}, keywords={len(keywords)})..."
    )

    # Pass 1: volume-based — keep prices fresh, scaled to limit
    volume_limit = min(limit, 200)
    await service.refresh_active_markets(limit=volume_limit)

    if not tag_labels and not keywords:
        logger.info("[Market Update] No strategy-specific tags — volume pass only.")
        return volume_limit

    # Pass 2: tag-specific fetch
    gc = GammaClient()
    seen_event_ids: set = set()
    try:
        # Resolve tag labels → IDs from Polymarket /tags endpoint
        try:
            async with httpx.AsyncClient(base_url="https://gamma-api.polymarket.com", timeout=10.0) as c:
                r = await c.get("/tags")
                r.raise_for_status()
                all_tags_meta = r.json()
        except Exception:
            all_tags_meta = []

        matching_tag_ids = [
            tag_meta.get("id")
            for tag_meta in all_tags_meta
            if any(
                tl.lower() in (tag_meta.get("label") or tag_meta.get("slug") or "").lower()
                for tl in tag_labels
            )
        ]
        if not matching_tag_ids:
            matching_tag_ids = PRIORITY_TAG_IDS

        # Distribute limit across matching tags
        per_tag_limit = max(30, limit // max(1, len(matching_tag_ids)))

        for tag_id in matching_tag_ids:
            if len(seen_event_ids) >= limit:
                break
            try:
                tag_events = await gc.get_events_by_tag(tag_id, max_events=per_tag_limit)
                for raw_event in tag_events:
                    eid = raw_event.get("id")
                    if eid not in seen_event_ids:
                        seen_event_ids.add(eid)
                        try:
                            await service.upsert_event(raw_event)
                        except Exception as e:
                            logger.debug(f"[Market Update] upsert skipped: {e}")
            except Exception as e:
                logger.warning(f"[Market Update] tag {tag_id} failed: {e}")

    finally:
        await gc.close()

    total = len(seen_event_ids)
    logger.info(f"[Market Update] Done — {total} events fetched for '{strategy}'.")
    return total


async def refresh_markets():
    """
    Full market refresh across ALL strategies and tags.
    Called once at startup to populate the DB for every strategy.
    Subsequent updates use refresh_markets_for_strategy() for the active strategy only.

    Tag fetching is parallelized (up to 8 concurrent requests) to keep startup fast.
    """
    logger.info("Full market refresh starting (all strategies / all tags)...")

    service = MarketService()
    db = get_db()

    # 0. Prune resolved markets
    await service.prune_resolved_markets(older_than_days=7)

    # Pass 1: Global volume-based sync
    await service.refresh_active_markets(limit=500)

    # Pass 2: Tag-based comprehensive fetch — parallelized
    gc = GammaClient()
    try:
        all_tag_ids = await _fetch_all_tags()
        ordered_tags = PRIORITY_TAG_IDS + [t for t in all_tag_ids if t not in PRIORITY_TAG_IDS]

        # Limit to first 60 tags to avoid excessive API load; priority tags go first
        ordered_tags = ordered_tags[:60]

        semaphore = asyncio.Semaphore(8)  # max 8 concurrent tag fetches

        async def fetch_tag(tag_id):
            async with semaphore:
                try:
                    return await gc.get_events_by_tag(tag_id, max_events=300)
                except Exception as e:
                    logger.warning(f"Tag fetch failed for tag_id={tag_id}: {e}")
                    return []

        logger.info(f"Fetching {len(ordered_tags)} tags in parallel (concurrency=8)...")
        results = await asyncio.gather(*[fetch_tag(tid) for tid in ordered_tags])

        seen_event_ids: set = set()
        total_new = 0
        for tag_events in results:
            for raw_event in tag_events:
                eid = raw_event.get("id")
                if eid not in seen_event_ids:
                    seen_event_ids.add(eid)
                    try:
                        await service.upsert_event(raw_event)
                        total_new += 1
                    except Exception as e:
                        logger.debug(f"Tag upsert skipped: {e}")

        logger.info(f"Tag-based pass: {total_new} new events across {len(ordered_tags)} tags.")
    finally:
        await gc.close()

    # Pass 3: Gap-fill for orphan markets (no outcomes)
    orphan_rows = await db.fetchall(
        """
        SELECT DISTINCT m.event_id
        FROM markets m
        LEFT JOIN outcomes o ON o.market_id = m.id
        WHERE m.active=1 AND m.closed=0 AND o.id IS NULL
        """
    )
    orphan_event_ids = [r["event_id"] for r in orphan_rows]

    if orphan_event_ids:
        logger.info(f"Gap-fill: {len(orphan_event_ids)} events with missing outcomes.")
        gc3 = GammaClient()
        filled = 0
        try:
            for event_id in orphan_event_ids[:200]:
                try:
                    raw_event = await gc3.get_event(str(event_id))
                    await service.upsert_event(raw_event)
                    filled += 1
                except Exception as e:
                    logger.warning(f"Gap-fill skipped event {event_id}: {e}")
        finally:
            await gc3.close()
        logger.info(f"Gap-fill complete: recovered {filled} events.")

    logger.info("Market refresh complete.")


if __name__ == "__main__":
    async def run():
        await refresh_markets()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
