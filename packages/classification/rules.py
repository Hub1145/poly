import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def classify_trader(profile) -> Tuple[str, float, str]:
    """
    Classify a trader into one of the research-defined segments.
    Returns: (label, confidence, reasoning)

    Segments (in priority order):
      market_maker         – buys AND sells the same market; signal is noise
      topic_specialist     – mediocre globally but high CLV in one domain
      whale                – large notional, directional conviction
      serious_non_whale    – moderate size but consistently early + correct (best signal)
      noise                – too few trades to evaluate
      directional_discretionary – catch-all with some conviction
    """
    # ------------------------------------------------------------------ #
    # 1. Market Maker – filter out first; their trades are uninformative   #
    #    Low directional purity = consistently both sides = liquidity role #
    # ------------------------------------------------------------------ #
    # Docs: purity < 0.30, trade_count >= 30
    if profile.directional_purity < 0.30 and profile.total_trades >= 30:
        return (
            "market_maker",
            0.9,
            f"Low directional purity ({profile.directional_purity:.2f}) with high "
            f"activity ({profile.total_trades} trades) — likely liquidity provision.",
        )

    # ------------------------------------------------------------------ #
    # 2. Topic Specialist – domain-specific edge (research Section 5)      #
    #    Identified by an above-average gamma_score (topic CLV) even if    #
    #    global avg_clv is modest.  Minimum trade sample to avoid noise.   #
    # ------------------------------------------------------------------ #
    if (
        profile.gamma_score > 0.05          # meaningful topic-level CLV edge
        and profile.total_trades >= 5       # enough data
        and profile.directional_purity > 0.4  # not a MM
    ):
        return (
            "topic_specialist",
            0.75,
            f"High topic-specific gamma score ({profile.gamma_score:.3f}) indicates "
            f"domain edge (directional purity {profile.directional_purity:.2f}).",
        )

    # ------------------------------------------------------------------ #
    # 3. Whale – large notional exposure + directional conviction          #
    #    Note: profit_loss stored as total notional (proxy until we have   #
    #    realized PnL).  Threshold set conservatively.                     #
    # ------------------------------------------------------------------ #
    # Docs: trade_count >= 50, purity >= 0.70, mean_clv >= 0.05
    # avg_clv gate added to prevent high-notional market-makers from being mislabelled.
    if (
        profile.profit_loss > 10_000
        and profile.directional_purity >= 0.70
        and profile.avg_clv >= 0.05
        and profile.total_trades >= 30
    ):
        return (
            "whale",
            0.85,
            f"Large notional ({profile.profit_loss:.0f}) with strong directional "
            f"conviction ({profile.directional_purity:.2f}) and CLV ({profile.avg_clv:.4f}).",
        )

    # ------------------------------------------------------------------ #
    # 4. Serious Non-Whale (SNW) – research shows these are often the      #
    #    BEST signal: high CLV (price moved their way after entry) on a    #
    #    moderate but meaningful trade count.                              #
    # ------------------------------------------------------------------ #
    # Docs: trade_count >= 20, purity >= 0.60, mean_clv >= 0.03
    if (
        profile.avg_clv >= 0.03
        and profile.directional_purity >= 0.50
        and 10 <= profile.total_trades < 200
    ):
        return (
            "serious_non_whale",
            0.70,
            f"Positive avg CLV ({profile.avg_clv:.4f}) on {profile.total_trades} trades, "
            f"purity {profile.directional_purity:.2f} — consistent early entry advantage.",
        )

    # ------------------------------------------------------------------ #
    # 5. Noise – insufficient history                                       #
    # ------------------------------------------------------------------ #
    if profile.total_trades < 5:
        return (
            "noise",
            0.4,
            "Fewer than 5 trades — insufficient history for reliable classification.",
        )

    # ------------------------------------------------------------------ #
    # 6. Default: directional but no clear edge detected yet               #
    # ------------------------------------------------------------------ #
    return (
        "directional_discretionary",
        0.5,
        f"General directional trader ({profile.total_trades} trades, "
        f"avg CLV {profile.avg_clv:.4f}, purity {profile.directional_purity:.2f}).",
    )
