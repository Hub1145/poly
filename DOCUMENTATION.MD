# Polymarket Alpha V2 — Full Technical Documentation

> **A high-conviction Bayesian signal engine for Polymarket prediction markets.**
> Identifies, scores, and follows skilled traders using multi-layer CLV (Closing Line Value) analysis, Bayesian posterior skill estimation, and 11 configurable strategy modes — including two external-data strategies powered by live weather and seismic APIs.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Data Pipeline](#3-data-pipeline)
4. [Trader Classification System](#4-trader-classification-system)
5. [Skill Scoring Engine](#5-skill-scoring-engine)
6. [Signal Aggregation — The Section-6 Formula](#6-signal-aggregation--the-section-6-formula)
7. [Strategy Modes](#7-strategy-modes)
8. [Database Models](#8-database-models)
9. [API Endpoints](#9-api-endpoints)
10. [Configuration Reference](#10-configuration-reference)
11. [Frontend UI](#11-frontend-ui)
12. [Running the Bot](#12-running-the-bot)
13. [Project File Structure](#13-project-file-structure)
14. [Research Basis](#14-research-basis)

---

## 1. Project Overview

Polymarket Alpha V2 is an automated, research-backed trading system for [Polymarket](https://polymarket.com) — a decentralised prediction market platform where users trade binary YES/NO outcomes for real-world events.

The core insight driving the project: **a small subset of Polymarket traders are genuinely skilled**, consistently entering positions early and closing them at favourable prices. The bot:

- Ingests markets across all active categories via the Polymarket Gamma and CLOB APIs using a tag-based three-pass approach.
- Classifies every trader wallet into skill tiers using Bayesian statistics.
- Aggregates skill signals across multiple dimensions into a single directional alpha score per market.
- For weather and seismic markets, overlays live external data (Open-Meteo and USGS APIs) directly onto the signal — no trader history required.
- Executes (or simulates in paper mode) a trade when the score exceeds a minimum edge threshold.
- Broadcasts real-time state to a live web dashboard over WebSocket.

**Paper mode is enabled by default.** No real capital is at risk until you explicitly enable live trading and provide a funded private key.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI + SocketIO                        │
│                         app.py  (port 5000)                      │
└────────────────┬────────────────────────────────────────────────┘
                 │ HTTP REST + WebSocket (state broadcast)
                 ▼
┌────────────────────────────────────────────────────────────────┐
│                        Background Workers                        │
│              (launched as asyncio tasks inside app.py)           │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ refresh_         │  │ refresh_     │  │ recompute_       │  │
│  │ markets.py       │  │ trades.py    │  │ features.py      │  │
│  └────────┬─────────┘  └──────┬───────┘  └────────┬─────────┘  │
│           │                   │                    │             │
│           └───────────────────┼────────────────────┘            │
│                               ▼                                  │
│                  ┌─────────────────────────┐                    │
│                  │   compute_signals.py     │                    │
│                  │   market_aggregation.py  │                    │
│                  │   + external data APIs   │                    │
│                  └─────────────────────────┘                    │
└────────────────────────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────────┐
│                   SQLite (WAL mode)                             │
│                   polymarket_alpha.db                           │
│                                                                 │
│  markets · outcomes · market_tags · trades · price_snapshots   │
│  trader_profiles · trader_classifications · topic_features      │
│  market_signal_snapshots · open_positions · closed_positions    │
└────────────────────────────────────────────────────────────────┘
```

### Key Technology Choices

| Layer | Technology | Reason |
|---|---|---|
| Web server | FastAPI + uvicorn | Async, minimal boilerplate |
| Real-time push | python-socketio | Server-push state without polling |
| ORM | SQLAlchemy sync wrapped in DummyAsyncSession | Windows aiosqlite compatibility |
| DB | SQLite + WAL mode + StaticPool | Single-file, no external infra |
| Config | pydantic-settings BaseSettings | Env-var injection with nesting |
| HTTP client | httpx AsyncClient | Async, keeps connection pool alive |
| Weather data | Open-Meteo API (free, no key) | 16-day forecast, timezone-aware |
| Seismic data | USGS FDSN API (free, no key) | Real-time earthquake feed + Poisson model |
| Frontend | Vanilla JS + Lucide icons | Zero build-step |

---

## 3. Data Pipeline

### 3.1 Market Ingestion — `packages/tasks/refresh_markets.py`

Market ingestion runs in **three passes** every cycle to ensure comprehensive coverage across all categories:

**Pass 1 — Volume baseline**
- `GET /events?order=volume24hr&limit=500` — top 500 events by 24-hour volume.
- Covers all high-liquidity markets reliably.

**Pass 2 — Tag-based comprehensive fetch**
- Fetches all available tag IDs from `GET /tags` (~100 categories).
- Priority tags are always processed first: Weather (84), Earthquakes (103038), Natural Disasters (496), Global Warming (92), Science (74).
- For each tag: `GET /events?tag_id={id}&active=true&closed=false&limit=100`.
- This ensures low-volume but strategically important markets (weather, seismic) are always ingested — they rarely appear in the top-500 by volume.

**Pass 3 — Gap-fill**
- Queries the DB for any `Market` rows that still have zero `Outcome` rows after passes 1 and 2.
- Fetches each by event ID: `GET /events/{event_id}`.
- Recovers outcomes for any markets missed by the first two passes.

Results are stored in `Market`, `Outcome`, `MarketTag`, and `PriceSnapshot` tables. The `outcomes` and `clobTokenIds` fields from the Gamma API are JSON-encoded strings and are parsed with `json.loads()` before storage. New markets are inserted; existing markets have `active` / `close_time` updated.

### 3.2 Trade Refresh — `packages/tasks/refresh_trades.py`

Runs every `scan_interval` minutes. Processes up to 200 active markets per cycle. For each market:
1. Fetches outcome token IDs from the `Outcome` table.
2. Calls CLOB trade endpoint: `GET /data/trades?asset_id={asset_id}&limit=500`.
3. Upserts trades into DB. Duplicate prevention via `(market_id, tx_hash)` unique constraint.
4. Records latest YES price into `PriceSnapshot` table.
5. **Position Lifecycle Tracking**: Passes new trades to `TraderService` to update `PositionSnapshot` records. Handles BUY/SELL logic to maintain a net-position model and calculates realized PnL/edge upon exit.
6. **Ghost Position Cleanup**: Periodically runs `cleanup_ghost_positions` to prune snapshots for markets that have resolved (active=False) but still carry open positions due to redemptions not generating CLOB trades.

> **Note:** The CLOB API requires L2 auth (API key + secret + wallet private key). Without a valid private key, trade fetches return an empty list gracefully — the bot continues operating in signal-monitoring mode.

### 3.3 Feature Recomputation — `packages/tasks/recompute_features.py`

After every trade refresh cycle:
1. Groups trades by `maker_address`.
2. For each trader, computes **CLV (Closing Line Value)** — the probability change from entry to 1-hour-later price.
3. Writes `trade.clv_score` back to the DB row.
4. Aggregates per-topic CLV into `trader_profile.gamma_score` and `trader_profile.median_clv`.

**CLV Formula:**
```
CLV_1h = later_price - entry_price   (signed, absolute probability points)
```
A positive CLV means the trader entered before the market moved in their favour — a direct proxy for edge.

### 3.4 Trader Classification — `packages/tasks/classify_traders.py`

Uses an **upsert** pattern (SELECT then UPDATE or INSERT) to prevent duplicate rows. For each trader address with sufficient trade history:

Computes:
- `directional_purity = |Σ(size × side)| / Σ(|size|)` — 1.0 = always same direction, 0.0 = perfectly balanced
- `mean_clv` — average CLV across all trades
- `trade_count` — lifetime trade count
- `gamma_score` — max topic-specific CLV across all market tags

Assigns one of seven labels (see Section 4).

### 3.5 Signal Computation — `packages/tasks/compute_signals.py`

Runs after feature recomputation. Strategy-aware market selection:

- **External-data strategies** (`laddering`, `disaster`, `seismic`): only scans markets that carry relevant tag labels in the `MarketTag` table (e.g. seismic scans only "Earthquakes" and "Natural Disasters" tagged markets — not all 10K+ markets).
- **Bayesian strategies**: scans all open, active markets.

Markets are filtered to only those still open (close time > now, with a 12-hour timezone grace window). Signals older than 24 hours are purged before each cycle. Results are stored as `MarketSignalSnapshot` rows.

---

## 4. Trader Classification System

Every wallet is assigned exactly one label per update cycle. Labels are **not permanent** — a trader can be re-classified as their behaviour evolves.

| Label | Criteria | Meaning |
|---|---|---|
| `whale` | trade_count >= 50, purity >= 0.7, mean_clv >= 0.05 | Large, directionally consistent, high-conviction skilled |
| `serious_non_whale` (SNW) | trade_count >= 20, purity >= 0.6, mean_clv >= 0.03 | Skilled retail, consistent direction, high purity |
| `topic_specialist` | gamma_score >= 0.08, trade_count >= 10 | Expert in a narrow domain (e.g. sports, politics) |
| `market_maker` | purity < 0.3, trade_count >= 30 | Balanced buy/sell — liquidity provider, excluded from signals |
| `directional_discretionary` | purity >= 0.5, mean_clv < 0.01 | Directional but no CLV edge — noise threshold |
| `noise` | mean_clv < 0.0 or too few trades | Uninformed, excluded from pipeline |
| `unknown` | Default before enough data | Insufficient history |

**Key principle:** To ensure signal purity, `whale` and `serious_non_whale` labels are gated by both a cumulative CLV threshold and a directional purity minimum. This prevents market-makers who happen to catch a single price move from polluting the skilled-trader pool.

---

## 5. Skill Scoring Engine

### 5.1 Bayesian Posterior Update — `packages/scoring/trader_bayesian.py`

Each trader maintains a Beta distribution over their true skill. After each trade resolves:

```
alpha_new = alpha_prior + positive_outcomes
beta_new  = beta_prior  + negative_outcomes
posterior_mean = alpha_new / (alpha_new + beta_new)
```

### 5.2 Shrinkage (Regularisation)

Raw posterior means are pulled toward the global mean to prevent small-sample over-confidence:

```
shrunk_skill = (n / (n + k)) * posterior_mean + (k / (n + k)) * global_mean
```
where `k` is the shrinkage coefficient (default 10) and `n` is the trade count.

### 5.3 Composite Skill Score

```
composite_skill = 0.5 * global_skill + 0.3 * topic_skill + 0.2 * gamma_score
```

This is the per-trader final score used in signal aggregation.

---

## 6. Signal Aggregation — The Section-6 Formula

`packages/scoring/market_aggregation.py` is the core signal engine. For each market, it computes:

### The Signal Formula

```
Signal(m) = a1 * weighted_global_skill
           + a2 * weighted_topic_skill
           + a3 * trader_convergence
           + a4 * early_entry
           + a5 * conviction_proxy
```

**Default coefficient weights (bayesian_ensemble mode):**

| Component | Coefficient | Description |
|---|---|---|
| `weighted_global_skill` | 0.30 | Avg global Bayesian skill of traders in this market, weighted by position size |
| `weighted_topic_skill` | 0.25 | Avg topic-specific skill for the market's category tags |
| `trader_convergence` | 0.25 | Fraction of skilled traders on the same side |
| `early_entry` | 0.10 | Fraction of trades placed > 48h before close |
| `conviction_proxy` | 0.10 | Normalized total size from high-conviction traders |

### External Data Overlay

For `laddering`, `disaster`, and `seismic` strategies, the Bayesian formula above is bypassed if a direct external-data signal can be computed:

- **Weather** (`packages/scoring/strategies/weather_probability.py`): queries Open-Meteo 16-day forecast for the city named in the market question. Computes edge as `forecast_probability − market_price`. Uses timezone-aware date comparison (city longitude → UTC offset) to skip markets where the target date has already passed locally.

- **Seismic** (`packages/scoring/strategies/earthquake_probability.py`): queries USGS FDSN API for recent earthquakes in the region named in the market question. Applies a Poisson probability model: `P(at_least_one) = 1 − exp(−λ × t)` where λ = historical annual rate / 365 and t = days remaining. Applies an aftershock boost for recent significant events. Sports teams containing "earthquake" in their name are excluded via keyword context filtering.

### Signal Quality Gates (Research-Derived)

Trader-based strategies (Bayesian, Whale, SNW, Specialist) are subject to three mandatory quality gates to minimize false signals:

1. **Gate A: Minimum Convergence Filter**
   - Requires at least **2 distinct skilled traders** on the same side.
   - Prevents "lucky" single-trader signals from driving bot execution.

2. **Gate B: Visibility-Lag Penalty (−30%)**
   - If the most recent qualifying trade is **>24 hours old**, the signal is stale.
   - Strength is reduced by 30% to account for market information decay.

3. **Gate C: Liquidity Proxy Penalty (−20%)**
   - If **<5 distinct skilled traders** total have participated in the market, it is too thin.
   - Reduced liquidity increases the risk of market exploitation or manipulation.

---

## 7. Strategy Modes

Eleven strategy modes are available. Each adjusts which traders are included, what coefficient weights are used, and how the minimum strength threshold is set.
Recent updates enable **granular strategy tracking**, where signal snapshots are explicitly labeled by the active strategy (`signal_type`), ensuring the UI only displays signals relevant to the current mission profile.

---

### 7.1 `bayesian_ensemble` (Default)

**Description:** Balanced multi-signal approach using all skilled trader labels. Applies the default Section-6 coefficient weights equally across all four components.

**Best for:** General-purpose discovery across all market categories.

- Traders included: whale, SNW, topic_specialist, directional_discretionary
- Min strength: 1.2
- Coefficients: a1=0.30, a2=0.25, a3=0.25, a4=0.10, a5=0.10

---

### 7.2 `conservative_snw`

**Description:** Signals only from Serious Non-Whale (SNW) traders — skilled retail traders who are too small to be market makers but show consistent directional CLV. Lower conviction threshold reduces false positives.

**Best for:** Lower-risk environments, smaller position sizes.

- Traders included: serious_non_whale only
- Min strength: 1.0
- Coefficients: a1=0.35, a2=0.30, a3=0.20, a4=0.10, a5=0.05

---

### 7.3 `aggressive_whale`

**Description:** Follows whales exclusively. Whales are the highest-capital, most directionally consistent traders. Their trades move markets — following them early is high-alpha but also higher variance.

**Best for:** High-conviction, large-edge markets. Higher risk/reward.

- Traders included: whale only
- Min strength: 1.5
- Coefficients: a1=0.40, a2=0.20, a3=0.25, a4=0.05, a5=0.10

---

### 7.4 `specialist_precision`

**Description:** Restricts signals to topic specialists — wallets with high domain-specific CLV scores. Maximises topic skill coefficient.

**Best for:** Markets with deep domain expertise (science, sports, geopolitics).

- Traders included: topic_specialist only
- Min strength: 1.3
- Coefficients: a1=0.20, a2=0.45, a3=0.20, a4=0.05, a5=0.10

---

### 7.5 `no_bias`

**Description:** Exploits a documented Polymarket behavioural edge — retail traders systematically overbuy YES outcomes in the $0.20–$0.50 probability range. This creates persistent mispricing on the NO side.

The strategy:
1. Gates on markets where current YES price is between $0.15 and $0.80.
2. Uses all skilled trader classes.
3. Applies a NO-score multiplier of 1.2x to 2.5x that scales with depth inside the $0.20–$0.50 edge zone. The closer the price is to $0.35 (zone centre), the stronger the multiplier.

**Best for:** Any binary market where YES is between 20c and 50c — elections, event occurrence markets.

- Traders included: whale, SNW, topic_specialist, directional_discretionary
- Min strength: 0.8
- Price gate: YES in [0.15, 0.80]
- NO multiplier: 1.2 to 2.5x based on zone depth

---

### 7.6 `black_swan`

**Description:** Hunts for underpriced tail events. Research shows markets systematically underweight low-probability outcomes in the $0.02–$0.08 range (the favourite-longshot bias in reverse). Skilled traders accumulating YES in this range are detecting genuine tail risk.

The strategy:
1. Gates on markets where current YES price is between $0.01 and $0.12.
2. Filters to SNW, specialists, and whales only (noise traders are excluded more aggressively here).
3. Applies a YES-score multiplier of up to 2.5x that peaks at $0.02 (maximum underpricing).

**Best for:** Low-probability markets. Potential for large returns but most trades will resolve as losses — manage position size accordingly.

- Traders included: serious_non_whale, topic_specialist, whale
- Min strength: 0.5
- Price gate: YES in [0.01, 0.12]
- YES multiplier: up to 2.5x at $0.02

---

### 7.7 `long_range`

**Description:** Focuses on domain experts entering early positions in markets with distant close dates. Topic specialists and high-gamma SNWs only. Raises the topic skill coefficient heavily (0.45) and drops conviction proxy (0.05) — because early positioning in long-range markets has lower convergence by definition.

**Best for:** Political, macroeconomic, and scientific prediction markets with close dates > 30 days out.

- Traders included: topic_specialist + SNW with gamma_score >= 0.05
- Min strength: 1.0
- Coefficients: a1=0.20, a2=0.45, a3=0.25, a4=0.05, a5=0.05

---

### 7.8 `volatility`

**Description:** Seeks markets with high whale conviction scores as a proxy for incoming volatility. Raises the conviction proxy coefficient to 0.30. Used when news is expected to resolve markets rapidly — whale positioning just before a binary resolution event.

**Best for:** Breaking-news markets, earnings-style events, sports finals.

- Traders included: whale, SNW, topic_specialist, directional_discretionary
- Min strength: 0.8
- Coefficients: a1=0.25, a2=0.20, a3=0.25, a4=0.00, a5=0.30

---

### 7.9 `laddering` — Weather / Temperature (Open-Meteo)

**Description:** Designed for temperature and weather prediction markets. Uses the Open-Meteo free weather API to fetch a 16-day forecast for the city named in the market question. Computes edge as the difference between the model's precipitation/temperature probability and the current market price.

**External data source:** Open-Meteo API — free, no API key required.

**Market targeting:** Only scans markets tagged as Weather, Natural Disasters, or Science (via `MarketTag`). Uses timezone-aware date comparison based on city longitude to correctly determine whether the market's target date has already passed in local time.

**Best for:** Temperature over/under markets, "will it snow/rain in [city] on [date]" markets.

- Traders included: topic_specialist, serious_non_whale
- External data: Open-Meteo 16-day forecast (`timezone=auto`, `forecast_days=16`)
- Min strength: 0.3 (external signal) / 1.0 (Bayesian fallback)

---

### 7.10 `disaster` — Natural Disaster / Weather Extremes

**Description:** Early-warning detector for catastrophe-category markets (hurricanes, extreme weather, infrastructure failures). Combines the external weather data path with heavy early-entry weighting to catch smart money entering disaster markets before the event.

**Best for:** Hurricane path markets, extreme weather event markets, infrastructure failure markets.

- Traders included: topic_specialist, whale
- External data: Open-Meteo (same as laddering)
- Min strength: 0.3 (external signal) / 0.8 (Bayesian fallback)

---

### 7.11 `seismic` — Earthquake Probability (USGS)

**Description:** Computes earthquake probability for markets asking about seismic events using the USGS FDSN earthquake feed and a Poisson probability model.

**Model:**
```
P(at_least_one) = 1 − exp(−λ × t)
```
where `λ` = historical annual event rate for the region / 365, and `t` = days remaining in the market window. An aftershock boost is applied when significant recent events are detected in the region.

**External data source:** USGS FDSN API (`earthquake.usgs.gov/fdsnws/event/1`) — free, no API key required.

**Market targeting:** Only scans markets tagged as Earthquakes or Natural Disasters. Sports teams with "earthquake" in their name (e.g. San Jose Earthquakes) are excluded via keyword context filtering.

**Supported market types:**
- "Will there be a M7.0+ earthquake by [date]?" — binary occurrence
- "Will there be N or more M6.5+ earthquakes in [window]?" — count markets
- "Megaquake by [date]?" — high-magnitude threshold markets

**Best for:** Earthquake count markets, megaquake markets, regional seismic risk markets.

- Traders included: topic_specialist, whale
- External data: USGS FDSN real-time feed + 30-year historical rates
- Min strength: 0.3

---

## 8. Database Models

All models are in `packages/db/models/`. SQLite file: `polymarket_alpha.db` (WAL mode).

### `Market`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| condition_id | TEXT UNIQUE | Polymarket condition ID |
| event_id | TEXT | Parent event ID (used for gap-fill) |
| question | TEXT | Market question string |
| category | TEXT | Topic category (politics, sports, etc.) |
| close_time | DATETIME | Market resolution deadline |
| active | BOOLEAN | True = still tradeable |
| closed | BOOLEAN | True = resolved |
| volume | REAL | Total traded volume |

### `Outcome`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| market_id | FK -> Market | Parent market |
| name | TEXT | "Yes" or "No" |
| asset_id | TEXT | CLOB outcome token ID (76–78 char hex string) |
| price | REAL | Current probability (0.0–1.0) |

### `MarketTag`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| market_id | FK -> Market | Parent market |
| tag | TEXT | Tag label (e.g. "Weather", "Earthquakes") |
| tag_id | INTEGER | Polymarket tag ID (84=Weather, 103038=Earthquakes, etc.) |

### `Trade`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| market_id | FK -> Market | |
| trader_address | TEXT | Wallet address |
| side | TEXT | "BUY" or "SELL" |
| size | REAL | USDC size |
| price | REAL | Entry price |
| timestamp | DATETIME | On-chain timestamp |
| tx_hash | TEXT UNIQUE | Deduplication key |
| clv_score | REAL | CLV_1h computed by recompute_features |

### `PriceSnapshot`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| outcome_id | FK -> Outcome | |
| price | REAL | Snapshot price |
| captured_at | DATETIME | When recorded |

### `TraderProfile`
| Column | Type | Description |
|---|---|---|
| address | TEXT PK | Wallet |
| trade_count | INTEGER | Lifetime trades |
| mean_clv | REAL | Average CLV across all trades |
| median_clv | REAL | Median CLV |
| directional_purity | REAL | 0 to 1 |
| gamma_score | REAL | Max topic CLV across tags |
| last_seen | DATETIME | Most recent trade timestamp |

### `TraderClassification`
| Column | Type | Description |
|---|---|---|
| address | TEXT PK | Wallet |
| label | TEXT | One of the 7 labels |
| confidence | REAL | Classification confidence 0–1 |
| reasoning | TEXT | Human-readable explanation |
| updated_at | DATETIME | Last classification time |

### `TopicFeatures`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | |
| topic_tag | TEXT | Topic tag string |
| trader_address | TEXT | Wallet |
| topic_skill | REAL | Bayesian CLV skill for this tag |
| trade_count | INTEGER | Trades in this topic |

### `MarketSignalSnapshot`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PK | |
| market_id | FK -> Market | |
| signal_type | TEXT | Strategy that generated it (e.g. "seismic", "bayesian") |
| signal_strength | REAL | Aggregated score (0.0–1.0+) |
| directional_bias | TEXT | "YES" or "NO" |
| reasoning | TEXT | Human-readable explanation |
| created_at | DATETIME | Timestamp — purged after 24 hours |

### `OpenPosition` / `ClosedPosition`
Track paper and live trades with entry price, side, size, exit price, profit/loss.

---

## 9. API Endpoints

All served by FastAPI at `http://localhost:5000`.

| Method | Path | Description |
|---|---|---|
| GET | `/` | Serves the dashboard HTML |
| POST | `/api/control` | `{"action": "start"}` or `{"action": "stop"}` — controls the background pipeline |
| POST | `/api/config` | Update strategy and trading settings (hot-applied, no restart required) |

**WebSocket** — `ws://localhost:5000/socket.io/` emits `bot_status` every 2 seconds with the full state payload. The frontend subscribes and refreshes all panels without polling.

### `/api/control` Payload

```json
{ "action": "start" }
```
or
```json
{ "action": "stop" }
```

### `/api/config` Payload

```json
{
  "strategy":      "bayesian_ensemble",
  "paper_mode":    true,
  "trade_amount":  10.0,
  "min_edge":      0.20,
  "scan_interval": 1,
  "paper_balance": 1000.0,
  "max_trades":    10,
  "private_key":   "0x..."
}
```

All fields are optional. Only provided fields are updated. Settings are hot-applied to the running pipeline without restart. The private key, if provided, is written to `.env` as `POLYMARKET__PRIVATE_KEY`.

---

## 10. Configuration Reference

Configuration is managed via `packages/core/config.py` using pydantic-settings.

Settings can be set three ways (highest priority first):
1. Environment variables
2. `.env` file in the project root
3. `settings.json` (written by the UI on save)

### Environment Variable Convention

Nested settings use double-underscore separator:

```
POLYMARKET__PRIVATE_KEY=0x...
POLYMARKET__API_KEY=...
APP__PAPER_MODE=true
APP__TRADE_AMOUNT=25.0
APP__MIN_EDGE=0.15
APP__SCAN_INTERVAL=5
APP__PAPER_BALANCE=1000
APP__MAX_TRADES=10
STRATEGY=bayesian_ensemble
LOG_LEVEL=INFO
```

### `settings.json` (UI-written format)

```json
{
  "strategy":      "bayesian_ensemble",
  "paper_mode":    true,
  "trade_amount":  10.0,
  "min_edge":      0.20,
  "scan_interval": 1,
  "paper_balance": 1000.0,
  "max_trades":    10
}
```

### Full Config Schema

| Field | Default | Description |
|---|---|---|
| `strategy` | `bayesian_ensemble` | Active strategy mode |
| `app.paper_mode` | `true` | If true, no real trades placed |
| `app.trade_amount` | `10.0` | USDC per trade |
| `app.min_edge` | `0.20` | Minimum signal score to trade |
| `app.scan_interval` | `1` | Minutes between pipeline cycles |
| `app.paper_balance` | `1000.0` | Starting paper balance |
| `app.max_trades` | `10` | Maximum concurrent open positions |
| `app.execution_enabled` | `false` | Master live execution switch |
| `polymarket.private_key` | `""` | Wallet private key for live trading |
| `polymarket.api_key` | `""` | Polymarket API key |

---

## 11. Frontend UI

The dashboard is a single-page app served at `http://localhost:5000`. It connects to the WebSocket for live updates and uses REST endpoints for actions.

### Tabs

| Tab | Description |
|---|---|
| **Dashboard** | Real-time metrics: trades, win rate (from `ClosedPosition`), PnL, balance, signal stream |
| **Open Positions** | Live positions tracked via `PositionSnapshot` with entry price and signal type |
| **Alpha Scan** | Current market signals ranked by alpha score. Filtered by active strategy. |
| **Alpha Feed** | Live news feed of skilled trader entries/exits with notional impact tracking |
| **Signal Analysis**| Full signal attribution and attribution audit tool |
| **Settings** | Configuration management; hot-reloads data pipeline on save |

### Audit & Traceability
The "Download Audit" tool in the Signal Analysis tab allows for the extraction of a human-readable signal log. This log contains the high-side bias, signal strength, and a full list of all qualifying traders (with labels) who contributed to the signal, ensuring 100% transparency for bot execution.

### Technology

- Vanilla JavaScript (no framework)
- `socket.io` client for live WebSocket state
- `Lucide` icons (SVG icon set)
- Google Fonts: `Inter` (UI) + `JetBrains Mono` (numbers/code)
- CSS custom properties — dark Polymarket-style terminal theme

---

## 12. Running the Bot

### Prerequisites

- Python 3.11+
- Windows or Linux (Windows uses StaticPool SQLite workaround)
- No external database, Redis, or message broker required
- No external API keys required for weather or seismic data (Open-Meteo and USGS are free)

### Install

```bash
pip install -r requirements.txt
```

Or with pyproject.toml:

```bash
pip install -e .
```

### Start

```bash
python app.py
```

Open `http://localhost:5000` in your browser.

Click **Start Bot** on the dashboard. The pipeline will begin ingesting markets (3-pass: volume + tags + gap-fill), classifying traders, and emitting signals. All data is stored in `polymarket_alpha.db`.

### Docker

```bash
docker-compose up --build
```

Exposes port 5000. The compose file mounts a volume for the SQLite DB to persist across restarts.

### Fresh Start (Reset Database)

Stop the bot, then delete:

```
polymarket_alpha.db
polymarket_alpha.db-shm
polymarket_alpha.db-wal
```

The database will be recreated automatically with all tables on next start.

---

## 13. Project File Structure

```
polymarket-bot-v2/
│
├── app.py                          # FastAPI server, SocketIO, background workers, REST endpoints
├── pyproject.toml                  # Package metadata and dependencies
├── requirements.txt                # Pinned Python dependencies
├── settings.json                   # Runtime config (written by UI)
├── docker-compose.yml              # Docker configuration
├── Dockerfile                      # Container build
│
├── alembic/                        # DB migration scripts
│   └── versions/
│
├── packages/
│   ├── core/
│   │   └── config.py               # pydantic-settings Settings model
│   │
│   ├── db/
│   │   ├── base.py                 # SQLAlchemy declarative base
│   │   ├── session.py              # Engine, DummyAsyncSession, init_db()
│   │   └── models/
│   │       ├── market.py           # Market, Outcome, MarketTag
│   │       ├── trade.py            # Trade
│   │       ├── price.py            # PriceSnapshot
│   │       ├── trader.py           # TraderProfile, TraderClassification, TopicFeatures
│   │       ├── scoring.py          # MarketSignalSnapshot
│   │       └── position.py         # OpenPosition, ClosedPosition
│   │
│   ├── ingestion/
│   │   ├── clients/
│   │   │   └── polymarket_http.py  # Gamma API (events, tags, get_events_by_tag) + CLOB API (/data/trades)
│   │   └── normalize/
│   │       ├── markets.py          # normalize_gamma_event() — parses JSON-string fields, returns 5-tuple
│   │       └── trades.py           # normalize_clob_trades() — robust timestamp + address parsing
│   │
│   ├── scoring/
│   │   ├── trader_bayesian.py      # Beta posterior, shrinkage, composite skill
│   │   ├── market_aggregation.py   # Section-6 signal formula, all 11 strategy modes, external data overlay
│   │   └── strategies/
│   │       ├── weather_probability.py     # Open-Meteo 16-day forecast, 80+ city map, timezone-aware dates
│   │       └── earthquake_probability.py  # USGS FDSN feed, Poisson model, 30+ region bbox map
│   │
│   ├── tasks/
│   │   ├── refresh_markets.py      # 3-pass market ingestion: volume top-500, all tags, gap-fill
│   │   ├── refresh_trades.py       # Fetch new trades via /data/trades?asset_id=, update price snapshots
│   │   ├── classify_traders.py     # Label all trader wallets
│   │   ├── recompute_features.py   # Compute CLV, gamma_score, median_clv
│   │   ├── compute_signals.py      # Strategy-aware signal scan, tag-filtered market selection
│   │   └── validate_signals.py     # Signal quality validation
│   │
│   ├── classification/
│   │   └── rules.py                # Trader label rule definitions
│   │
│   ├── features/
│   │   ├── behavior.py             # Behavioural feature extraction
│   │   ├── price_relative.py       # Price-relative feature computation
│   │   └── topic_features.py       # Topic-specific CLV aggregation
│   │
│   ├── services/
│   │   ├── market_service.py       # Market + outcome upsert, PriceSnapshot storage, upsert_event()
│   │   └── trader_service.py       # Trade ingestion, trader address extraction
│   │
│   ├── explanation/
│   │   └── engine.py               # Human-readable signal reasoning generator
│   │
│   ├── validation/
│   │   └── backtest.py             # Historical signal validation
│   │
│   └── ui/
│       └── state_mapper.py         # Converts DB state to JSON for SocketIO broadcast
│
├── static/
│   ├── css/
│   │   └── style.css               # Polymarket dark terminal theme
│   └── js/
│       └── app.js                  # Dashboard JS (WebSocket, tab rendering)
│
├── templates/
│   └── index.html                  # Single-page dashboard template
│
└── Research/                       # Academic basis papers and strategy notes
```

---

## 14. Research Basis

The signal engine is grounded in prediction market research.

### CLV as Skill Proxy

Closing Line Value (CLV) is the standard metric for measuring trader skill in prediction markets. A trader who consistently enters at prices that later move in their favour demonstrates informational edge, not luck. The CLV formula used is:

```
CLV_1h = P(outcome | t + 1h) - P(outcome | entry)
```

Positive CLV over many trades, adjusted for shrinkage, is a statistically robust signal of genuine skill.

### Favourite-Longshot Bias (Black Swan Mode)

Well-documented in betting markets: low-probability outcomes are systematically underpriced by the market. Bettors overcorrect on psychological risk aversion, suppressing YES prices on rare events below their true probability. The `black_swan` mode is designed to capture this mispricing when skilled traders — who have already corrected for this bias — accumulate YES at prices below $0.08.

### YES Overpricing Bias (NO Bias Mode)

Research in prediction markets documents that retail participants systematically overbuy YES outcomes at moderate probabilities (20–50%). This is driven by psychological optimism bias and media coverage of "will it happen?" framing. Skilled NO buyers in this range consistently outperform. The `no_bias` mode quantifies and exploits this with a calibrated multiplier.

### Bayesian Skill Estimation

Beta-binomial conjugate updating is the theoretically correct method for estimating binary outcome skill. The shrinkage step is analogous to James-Stein estimation — pulling noisy small-sample estimates toward the global mean reduces expected error across the full trader population.

### Trader Heterogeneity

The seven-label classification is grounded in research showing that prediction market liquidity providers (market makers) are informationally neutral, while directional traders split into skilled (positive CLV) and noise (negative CLV) camps. Filtering to only skilled directional traders before computing convergence significantly improves signal precision over naive crowd aggregation.

### Poisson Model for Seismic Markets (Seismic Mode)

Earthquake occurrence in a given region over a time interval follows a Poisson process to a good approximation when earthquakes are independent. The probability of at least one event of magnitude ≥ M in t days is `1 − exp(−λt)` where λ is the expected daily rate derived from the USGS historical record. Aftershock sequences temporarily elevate the rate above the background — the model applies a short-term boost when a significant recent event is detected.

---

*Polymarket Alpha V2 — Built for research and educational purposes. Always start in paper mode. Prediction market trading involves financial risk.*
