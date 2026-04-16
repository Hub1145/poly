import asyncio
import logging

from packages.db.database import get_db
from packages.classification.rules import classify_trader

logger = logging.getLogger(__name__)


async def classify_all_traders():
    """Apply heuristic classification rules to all trader profiles."""
    logger.info("Classifying all traders...")
    db = get_db()

    profiles = await db.fetchall("SELECT * FROM trader_profiles")

    for profile in profiles:
        label, confidence, reasoning = classify_trader(profile)

        exists = await db.fetchval(
            "SELECT id FROM trader_classifications WHERE address=? LIMIT 1",
            (profile.address,),
        )
        if exists:
            await db.execute(
                "UPDATE trader_classifications"
                " SET label=?, confidence=?, reasoning=?"
                " WHERE address=?",
                (label, confidence, reasoning, profile.address),
            )
        else:
            await db.execute(
                "INSERT INTO trader_classifications (address, label, confidence, reasoning)"
                " VALUES (?, ?, ?, ?)",
                (profile.address, label, confidence, reasoning),
            )

    await db.commit()
    logger.info(f"Classified {len(profiles)} traders.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(classify_all_traders())
