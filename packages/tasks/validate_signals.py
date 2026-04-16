import asyncio
import logging
from datetime import datetime, timedelta

from packages.validation.backtest import simulate_alpha

logger = logging.getLogger(__name__)


async def run_validation_task():
    """Weekly validation task: evaluate all signals generated in the last 7 days."""
    logger.info("Running weekly signal validation...")

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=7)

    results = await simulate_alpha(start_date, end_date)

    if not results:
        logger.warning("No signals found in the validation window.")
        return

    total     = len(results)
    correct   = sum(1 for r in results if r["is_correct"])
    precision = (correct / total) * 100 if total > 0 else 0
    total_pnl = sum(r["pnl"] for r in results)

    report = (
        f"\n--- Alpha Validation Report ({start_date.date()} to {end_date.date()}) ---\n"
        f"Total Signals Eval: {total}\n"
        f"Signal Precision: {precision:.2f}%\n"
        f"Theoretical PnL: ${total_pnl:.2f}\n"
        f"Avg Signal Strength: {sum(r['strength'] for r in results)/total:.4f}\n"
    )

    print(report)
    logger.info("Validation complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_validation_task())
