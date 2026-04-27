# Polymarket Alpha V2 — Strategy Reference

---

## Strategy Categories

| Category | Strategies | Data Source |
|---|---|---|
| Bayesian Alpha (Skilled Trader) | `bayesian_ensemble`, `conservative_snw`, `aggressive_whale`, `specialist_precision` | Polymarket CLOB trade history |
| Market Structure / Price Zone | `no_bias`, `black_swan`, `long_range`, `volatility` | Polymarket price snapshots only |
| External Data (Weather / Geophysical) | `weather_prediction`, `laddering`, `disaster` | Open-Meteo API + USGS base rates |

Alpha Feed (Skilled Trader tab) is shown **only** for Bayesian Alpha strategies. All others suppress the feed entirely — no trader data is fetched or displayed.

---

## Risk Management (All Strategies)

The same TP/SL engine applies to every strategy. Settings are configurable per-session from the Settings tab.

| Parameter | Default | Notes |
|---|---|---|
| Take Profit | 50% | Close when `current_price >= entry_price × 1.50` |
| Stop Loss | 30% | Close when `current_price <= entry_price × 0.70` |
| Position Size | $10.00 USDC | Total budget per trade (ladder strategies split this across legs) |
| Max Open Trades | 8 | No new entries opened once this limit is reached |
| Min Edge | 0.12 (12%) | Signal must exceed this threshold to be tradeable |
| Paper Mode | On | Simulates trades against a virtual balance; no real funds move |

Positions are checked on every 2-minute cycle. In paper mode, proceeds (exit price × shares) are restored to the paper balance on close. In live mode, a GTC SELL order is placed via the Polymarket CLOB before recording the close.

---

## Bayesian Alpha Strategies

These four strategies use skilled-trader trade history from Polymarket. Traders are classified into three tiers:

| Label | Base Weight | Classification Rules |
|---|---|---|
| `serious_non_whale` (SNW) | 2.5 | `avg_clv ≥ 0.05`, directional purity ≥ 0.60, 10–499 trades. Checked **first** to prevent high-CLV traders from being absorbed by lower-priority tiers. |
| `topic_specialist` | 3.0 | `gamma_score > 0.12`, purity > 0.50, `avg_clv > 0.02`, ≥ 5 trades. Domain-specific edge. |
| `whale` | 1.0 | Avg notional per trade > $2,000, directional purity ≥ 0.65, `avg_clv > -0.20`. Real whales often appear in limited trades so classification gates on **average size**, not trade count. |

**Classification priority (descending):** market_maker → SNW → topic_specialist → whale → noise → directional_discretionary

The **signal strength formula** combines five components:

```
yes_score = 0.30 × global_skill
          + 0.25 × topic_skill
          + 0.25 × convergence (unique trader count)
          + 0.10 × early_entry bonus
          + 0.10 × conviction (size-normalized)
```

A signal is emitted for whichever side (YES/NO) scores higher. Strength = `|yes_score - no_score|`.

**Staleness gate:** If the newest qualifying trade is older than 48 hours, strength is reduced by 30%. If fewer than 5 total skilled traders have touched the market, strength is reduced by 20%.

**Liquidity gate:** Before a signal is recorded, the CLOB orderbook is checked for sufficient depth (2× position size within 2% of mid). Low-liquidity markets are dropped.

---

### `bayesian_ensemble` — Bayesian Ensemble (Default)

**Market type:** Any active binary market (politics, crypto, entertainment, sports, science).

**Signal logic:**
- Aggregates all three trader tiers (whale, SNW, topic_specialist) using the full five-component formula.
- No additional filtering beyond the base quality gates.
- Fires YES or NO based on whichever side has stronger convergence of skilled traders.

**Minimum strength:** 1.0  
**Minimum qualifying traders:** 2  
**Use case:** Broadest coverage; best starting point for general Polymarket alpha.

---

### `conservative_snw` — Conservative Alpha (SNW Only)

**Market type:** Politics, Crypto (tag-filtered).

**Signal logic:**
- Only considers `serious_non_whale` traders (whales and topic specialists excluded).
- SNW traders are classified first in the priority order, ensuring high-CLV multi-trade accounts land here before being captured by topic_specialist.
- SNW traders have consistently positive CLV across diverse market types without large-capital noise.

**Minimum strength:** 1.5 (highest threshold — filters for only the cleanest convergence)  
**Minimum qualifying traders:** 1  
**Use case:** Lower trade frequency, higher precision. Best for conservative capital deployment.

---

### `aggressive_whale` — Aggressive Whale Momentum

**Market type:** Politics, Crypto, High Volume (tag-filtered).

**Signal logic:**
- Only considers `whale` traders (avg notional per trade > $2,000).
- Whale base weight is 1.0 but position size normalization amplifies conviction.
- Trades in the direction whales are positioned — momentum-following rather than skill-weighted.
- Real whales typically hold 1–3 large positions; the classifier gates on **average notional** not cumulative trade count to avoid excluding genuine whales with small trade history.

**Minimum strength:** 0.8 (lowest trader threshold — fires more aggressively)  
**Minimum qualifying traders:** 1  
**Use case:** Higher frequency, faster-moving signals. Suited for markets with heavy whale activity where size itself is the signal.

---

### `specialist_precision` — Topic Specialist Precision

**Market type:** Politics, Science, Business (tag-filtered).

**Signal logic:**
- Considers only traders with either `gamma_score > 0.6` (top-tier domain expertise rating) or the `topic_specialist` label.
- Gamma score measures how consistently a trader outperforms in a specific topic domain (science, geopolitics, economics).
- Best for niche markets where generalist traders are noisy.

**Minimum strength:** 1.0  
**Minimum qualifying traders:** 1  
**Use case:** High-conviction specialist plays. Fewer signals, but the traders behind them have demonstrated topic-specific edge.

---

## Price-Structure Strategies

These four strategies use **no skilled-trader data**. They operate entirely on Polymarket price snapshots stored in the local database. No CLOB trade history is fetched or used. The Alpha Feed tab is hidden when any of these strategies is active.

---

### `no_bias` — NO Bias Exploitation

**Market type:** Politics, Sports, Pop Culture (tag-filtered). Binary markets priced between 0.20–0.50 YES.

**Signal logic:**
- Exploits a documented retail-behaviour bias: casual Polymarket participants systematically over-buy YES, inflating YES prices above their fair value.
- Looks for YES prices in the 0.20–0.50 zone — the "retail overbuy zone" validated in backtests. Zones outside this range (0.15–0.20 and 0.50–0.80) had negative PnL and are excluded.
- Always signals **NO**.
- Signal strength = `1.5 + (0.50 − yes_price) × 2.0` — stronger the further YES is below 0.50 (i.e. deeper into the zone).

**Minimum strength:** 0.8  
**Example:** YES priced at 0.30 → strength = 1.5 + (0.20 × 2.0) = 1.9. Edge: retail is paying too much for a 30% chance event.  
**Use case:** Passive edge harvesting across high-volume retail-driven markets. No external data dependency.

---

### `black_swan` — Black Swan Tail Bets

**Market type:** Science, Natural Disasters, Global Warming (tag-filtered). Markets with YES priced between $0.005–$0.05.

**Signal logic:**
- Targets the deepest tail events — extremely low-probability outcomes priced at 0.5¢ to 5¢.
- Rationale: Polymarket markets at this price range are often structurally underpriced because most participants dismiss near-zero events without accounting for fat-tail risk or potential market manipulation. A small position captures asymmetric upside.
- Range was tightened from 0.01–0.12 to 0.005–0.05: the 5%–12% range was found to produce too many false positives (markets correctly priced, rarely appreciating in any hold window).
- Always signals **YES**.
- Signal strength = `min(2.5, 1.0 + (0.05 − yes_price) / 0.045 × 1.5)` — stronger for deeper tail.

**Minimum strength:** 0.5  
**Example:** YES = $0.01 → strength ≈ 2.33. Position costs $0.10 for 10 shares; resolves at $1.00/share on YES.  
**Use case:** Lottery-style tail bets. Low cost per position, large asymmetric payoff. Keep position size small.

---

### `long_range` — Long-Range Forecast

**Market type:** Any binary market resolving 30+ days in the future. Price zone: 0.15–0.42 (YES) or 0.58–0.85 (YES).

**Signal logic:**
- Hypothesis: Long-dated markets are priced with excessive uncertainty in both directions. Events the market considers 58–85% likely are structurally overpriced because resolution uncertainty over a long horizon has not been discounted. Events priced at 15–42% are underpriced for the same reason.
- Horizon filter: The market's `end_date` must be ≥ 30 days from today. If no end date is stored, the question text is parsed for date references (ISO, "Month Day Year", "by Month Day", "end of Year").
- **YES zone (0.15–0.42):** Signals YES. Strength = `(0.42 − price) / 0.27 × 1.2 + 0.3`
- **NO zone (0.58–0.85):** Signals NO. Strength = `(price − 0.58) / 0.27 × 1.2 + 0.3`
- Mid-range (0.43–0.57) is neutral — no signal.

**Minimum strength:** 0.4  
**Example:** YES = 0.22 on a market resolving in 3 months → YES signal, strength ≈ 1.09.  
**Use case:** Systematic mean-reversion on long-dated uncertainty premium. Works across any domain.

---

### `volatility` — Volatility Arbitrage

**Market type:** Crypto, Sports, Politics (tag-filtered). Markets with bid-ask spread ≥ 0.06 and YES price 0.25–0.75.

**Signal logic:**
- Wide bid-ask spread indicates high market disagreement (or thin liquidity). The strategy trades toward the cheaper side of that disagreement.
- Entry condition: `spread = best_ask − best_bid ≥ 0.06` AND `0.25 ≤ YES_price ≤ 0.75`
- Real spread values are sourced from the Gamma API (`spread` and `bestAsk` fields) and stored in price snapshots — not inferred from mid-price.
- Direction: if YES_price ≤ 0.50 → signal **YES** (YES is the cheaper side); if YES_price > 0.50 → signal **NO**.
- Signal strength = `spread × 6.0` (a 0.06 spread → 0.36, a 0.20 spread → 1.20).

**Minimum strength:** 0.3  
**Example:** YES = 0.40, best_bid = 0.32, best_ask = 0.48. Spread = 0.16. YES_price < 0.50 → YES. Strength = 0.96.  
**Use case:** Captures spread compression in contested markets. Works best in high-volume liquid markets where the spread is transient.

---

## External Data Strategies

These strategies use data from third-party APIs (NOAA, Open-Meteo, USGS). No skilled-trader data is used. The Alpha Feed tab is hidden. Market fetch skips the volume-based pass (which would pull unrelated high-volume markets) and uses keyword and tag filters instead.

**Signal cycle:** Re-scored every 1 minute. Markets re-fetched from Polymarket every 5 minutes.

### Data source overview

| Source | What it provides | Coverage | Auth |
|---|---|---|---|
| Open-Meteo Ensemble API | ECMWF 51-member, NOAA GEFS 31-member, DWD ICON 40-member probabilistic forecasts | Global | Free, no key |
| NOAA NWS `api.weather.gov` | Deterministic hourly forecast (7 days) | **US only** | Free, no key |
| Open-Meteo standard API | Single-model hourly wind, precipitation + historical `precipitation_sum` | Global | Free, no key |
| USGS Earthquake Catalog API | Real-time M4+ events (last 30 days) per region | Global | Free, no key |
| USGS NSHM base rates | Annual seismic probability by region (hardcoded from published maps) | Global | N/A — static |

> **Why NOAA GEFS is accessed via Open-Meteo:** NOAA's GEFS ensemble data has no REST API — it is distributed as GRIB2 files on AWS S3, requiring specialized parsing libraries. Open-Meteo wraps GEFS (model `gefs025`, 31 members) in its ensemble API, making it accessible as standard JSON without infrastructure overhead.

> **Why NOAA NWS is US-only:** The National Weather Service API (`api.weather.gov`) covers only continental US, Alaska, and Hawaii territories. For non-US cities the NWS cross-check step is automatically skipped.

---

### `weather_prediction` — Weather Prediction (Multi-Ensemble)

**Market type:** Temperature markets only. Keywords: "temperature", "degrees fahrenheit", "degrees celsius", "highest temperature", "daily high", "°F", "°C".

**Data source cascade:**
1. **ECMWF IFS 0.25°** — 51 members, 15-day global forecast (primary, industry gold standard)
2. **NOAA GEFS 0.25°** — 31 members, 16-day global forecast (first fallback — NOAA's probabilistic model via Open-Meteo)
3. **DWD ICON-EPS** — 40 members, 7.5-day global (second fallback)

**NOAA NWS cross-check (US cities only):** For US markets (NYC, LA, Chicago, etc.), the NOAA NWS deterministic hourly forecast is fetched independently. If NWS and the ensemble model disagree on direction (e.g. ensemble says temperature will be above the threshold, NWS says below), signal strength is reduced by 20% to reflect the cross-model uncertainty. NWS agreement is noted in the signal explanation.

Forecast cache: 60 minutes per model (aligned with 00Z/06Z/12Z/18Z model-run cycle). Historical data up to 7 days back for markets that have not yet resolved.

**Signal logic:**
- Extracts city (from a 70+ city lookup table with lat/lon), date, and temperature unit (F/C) from the market question.
- Fetches the ensemble forecast and counts how many of the 51 members satisfy the market condition (empirical probability — no distribution assumption).
- **YES:** model probability exceeds market price by more than `min_gap` (default 12 pp).
- **NO:** market price exceeds model probability by more than `min_gap`.

**Mutual exclusivity:** For a group of temperature bucket markets sharing the same city, date, and unit (e.g. "NYC high 70°F?", "NYC high 80°F?", "NYC high 90°F?"), only one can resolve YES. The bucket the model favours most is identified as the winner. All other buckets in the group are forced **NO** if they are overpriced relative to the model probability.

**Minimum signal strength:** 0.10 (edge in probability points, not the same scale as trader strategies)  
**Use case:** Pure meteorological alpha from professional-grade ensemble models against unsophisticated market pricing.

**Note on precipitation markets:** Open-Meteo provides `daily=precipitation_sum` via its historical forecast API (confirmed delivering full monthly totals globally). Approximately 30 monthly precipitation bucket markets (Hong Kong, Seattle, NYC, Seoul) exist in the database. These are currently unscored — precipitation type detection returns no signal. Future work: add precipitation scorer using `historical-forecast-api.open-meteo.com/v1/forecast` monthly accumulation vs market threshold.

---

### `laddering` — Temperature Ladder

**Market type:** Same temperature keywords as `weather_prediction`.

**Data source:** Same ECMWF/ICON ensemble as `weather_prediction`.

**Signal logic:**
This strategy produces multiple YES signals across adjacent temperature buckets for the same city+date — creating a "ladder" of positions that hedges model uncertainty.

- **Winner bucket:** The ensemble-favoured bucket. Signals YES if `gap > min_gap`.
- **Adjacent buckets:** Buckets within `ensemble_sigma` of the forecast mean, where `ensemble_sigma = max(1.5, ensemble_spread / 4.0)`. Signal YES at **50% strength** (half-edge) if the model gives them positive probability.
- **Non-adjacent buckets:** Forced **NO** if `gap < -min_gap`. Otherwise skipped (neutral).

Adjacency is based on temperature, not index. A bucket is adjacent if `|market_threshold − ensemble_mean| ≤ ensemble_sigma`, computed from the actual ensemble spread rather than a fixed rule.

**Minimum signal strength:** 0.35  
**Example:** Ensemble mean = 83°F, sigma = 4°F. Winner = "82–84°F". "78–82°F" is adjacent (diff = 4.0°F ≤ sigma). "70–75°F" is non-adjacent → NO if overpriced.  
**Use case:** Distributes position budget across 2–3 adjacent temperature buckets per city per day, reducing the impact of a 1–2°F model error. Total exposure per city = configured position size split across legs.

---

### `disaster` — Disaster & Seismic (Weather + USGS)

**Market type:** Hurricane, cyclone, typhoon, tropical storm, tornado, flood, wildfire, blizzard, landslide, tsunami, drought, heat wave, volcanic eruption, earthquake, seismic, magnitude events. Keywords filter the market question directly.

**Signal routing:** The strategy uses **content-based routing** — the same strategy handles both weather-disaster and seismic markets by inspecting the question text:

- **Seismic path** (if question contains: earthquake, seismic, magnitude, richter, aftershock, volcanic eruption, eruption):
  → USGS NSHM base rate + live USGS catalog Omori boost. Signal type stored as `seismic`.

- **Weather path** (all other disaster keywords — hurricane, flood, wildfire, etc.):
  → Open-Meteo wind/precipitation hourly forecast. Signal type stored as `weather_disaster`.

Both paths are shown together in the Alpha Scan tab when `disaster` is active.

#### Weather path — Open-Meteo wind/precipitation

**Data source:** Open-Meteo standard forecast API (hourly `windgusts_10m` and `precipitation`).

- Hurricane: `P(max_gust ≥ 74 mph)` using normal CDF with σ=10 mph
- Tropical storm: `P(max_gust ≥ 39 mph)` with σ=5 mph
- Red flag / fire: `P(max_gust ≥ 25 mph)` with σ=5 mph
- Flood: `P(total_precip ≥ 3.0 inches)` with σ=1.0 inch
- Generic: `max(wind_prob, precip_prob)` across standard thresholds

#### Seismic path — USGS two-layer model

**Layer 1 — USGS NSHM static base rates** (annual probability from National Seismic Hazard Maps):
> USGS does not expose a REST API for forward-looking earthquake probabilities. The Unified Hazard Tool is web-only and non-automatable. These base rates are hardcoded directly from published NSHM data.

| Region | NSHM annual base probability |
|---|---|
| Japan | 30% |
| Indonesia | 25% |
| Turkey | 22% |
| Chile | 20% |
| California | 18% |
| New Zealand | 17% |
| Greece | 15% |
| San Francisco | 14% |
| Los Angeles | 12% |
| Nepal | 12% |
| Iran | 10% |
| Philippines | 10% |
| Global (unmatched) | 4% |

**Layer 2 — USGS Earthquake Catalog API (live, real-time):**
- Queries `earthquake.usgs.gov/fdsnws/event/1/query` for M4+ events in the past 30 days within the region's bounding box.
- If recent activity is found, applies **Omori's law boost**: each full magnitude unit above M4.0 adds 15% to the base probability (capped at +30%).
- This makes the signal data-driven: a region that just had a M6.5 event has elevated aftershock probability that the static base rate alone cannot capture.

**Signal logic (seismic):**
- `adjusted_prob = base_prob + omori_boost` (if recent catalog event found)
- `edge = adjusted_prob − market_yes_price`
- Positive edge → YES. Negative edge → NO.

**Minimum signal strength:** 0.25 (seismic) / 0.35 (weather)  
**Example (seismic):** California YES priced at 0.08. NSHM base = 0.18. USGS catalog finds M5.8 last week → Omori boost = +15% → adjusted_prob = 0.33. Edge = +0.25 → strong YES signal.  
**Example (weather):** Hurricane market, Open-Meteo shows P(gust ≥ 74 mph) = 0.72, market priced at 0.40 → YES signal, strength = 0.32.  
**Use case:** Covers the full spectrum of natural disaster and seismic markets with appropriate data sources for each type. Sports teams with "hurricane" or "earthquakes" in their name are excluded via pattern filters.

---

## Signal Strength Reference

Signal strength is not on the same scale across categories:

| Category | Strength range | Interpretation |
|---|---|---|
| Trader strategies | 0.8 – 5.0+ | Composite score of weighted skilled-trader convergence |
| Price-zone strategies | 0.3 – 2.5 | Scaled from price zone position or spread width |
| External (weather) | 0.03 – 0.99 | Raw probability gap (model vs market price, in pp) |
| External (seismic) | 0.04 – 0.30 | USGS base-rate + Omori boost minus market price |

Alpha Score shown in the UI = `min(100, signal_strength × 20)`.

---

## Scan & Refresh Timing

| Event | Interval |
|---|---|
| Signal recompute (re-score cached markets) | Every 60 seconds |
| Trade / position management (TP/SL check) | Every 120 seconds (only when bot is running) |
| Market fetch from Polymarket API | Every 5 minutes (configurable, minimum 5) |
| Trader profile refresh | Every 120 seconds (trader strategies only) |
| Weather forecast cache TTL | 30 minutes (single model), 60 minutes (ensemble) |
| Weather burst-scan window | 90 minutes after each 00Z/06Z/12Z/18Z model run |
| Signal retention in DB | 6 hours (older signals pruned) |
| Price snapshot retention | 3 days |
| Strategy DB cache TTL | 1 hour (switching back to a recently-run strategy reuses cached markets) |
