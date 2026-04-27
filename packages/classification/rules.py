import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def classify_trader(profile) -> Tuple[str, float, str]:
    """
    Classify a trader into one of the research-defined segments.
    Returns: (label, confidence, reasoning)

    Segments (in priority order):
      market_maker         – buys AND sells the same market; signal is noise
      serious_non_whale    – globally consistent early-entry edge (best signal overall)
      topic_specialist     – domain-specific CLV edge, modest global CLV
      whale                – large avg notional, directional conviction
      noise                – too few trades to evaluate
      directional_discretionary – catch-all with some conviction
    """
    # ------------------------------------------------------------------ #
    # 1. Market Maker – filter out first; their trades are uninformative   #
    #    Low directional purity = consistently both sides = liquidity role #
    # ------------------------------------------------------------------ #
    if profile.directional_purity < 0.30 and profile.total_trades >= 30:
        return (
            "market_maker",
            0.9,
            f"Low directional purity ({profile.directional_purity:.2f}) with high "
            f"activity ({profile.total_trades} trades) — likely liquidity provision.",
        )

    # ------------------------------------------------------------------ #
    # 2. Serious Non-Whale (SNW) – globally consistent early-entry edge.  #
    #    Research identifies these as the best overall signal: high CLV   #
    #    across market domains (not just one topic) on a meaningful trade #
    #    count.  Priority above topic_specialist so globally-profitable   #
    #    traders get the SNW label; topic_specialist then catches domain- #
    #    specialists whose global CLV is more modest.                     #
    #    min_trades lowered to 10 to account for limited ingestion window #
    #    (global trade feed samples each trader infrequently).            #
    # ------------------------------------------------------------------ #
    if (
        profile.avg_clv >= 0.05
        and profile.directional_purity >= 0.60
        and 10 <= profile.total_trades < 500
    ):
        return (
            "serious_non_whale",
            0.80,
            f"Globally consistent CLV ({profile.avg_clv:.4f}) on {profile.total_trades} trades, "
            f"purity {profile.directional_purity:.2f} — consistent early-entry advantage.",
        )

    # ------------------------------------------------------------------ #
    # 3. Topic Specialist – domain-specific edge (research Section 5)      #
    #    High gamma_score = above-average CLV in a specific topic cluster  #
    #    even when global avg_clv is modest.                               #
    # ------------------------------------------------------------------ #
    if (
        profile.gamma_score > 0.12
        and profile.total_trades >= 5
        and profile.directional_purity > 0.50
        and profile.avg_clv > 0.02
    ):
        return (
            "topic_specialist",
            0.75,
            f"High topic-specific gamma score ({profile.gamma_score:.3f}) with "
            f"domain edge (purity {profile.directional_purity:.2f}).",
        )

    # ------------------------------------------------------------------ #
    # 4. Whale – large average trade size + directional conviction         #
    #    Whales trade infrequently (1-5 big positions per market) so we   #
    #    gate on average trade size, not cumulative count.                 #
    #    $2 000+ avg notional per trade = whale-tier position sizing.      #
    #    CLV gate is lenient (> -0.20) — a large committed bet is the     #
    #    signal even when short-term CLV hasn't fully resolved.            #
    # ------------------------------------------------------------------ #
    _avg_notional = profile.profit_loss / max(1, profile.total_trades)
    if (
        _avg_notional > 2_000
        and profile.directional_purity >= 0.65
        and profile.avg_clv > -0.20
    ):
        return (
            "whale",
            0.85,
            f"Large avg notional ({_avg_notional:.0f}/trade) with directional "
            f"conviction ({profile.directional_purity:.2f}). CLV={profile.avg_clv:.4f}.",
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
