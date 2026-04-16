"""
packages/db/database.py
~~~~~~~~~~~~~~~~~~~~~~~
Pure-sqlite3 database layer.  Replaces the former SQLAlchemy stack.

Public API
----------
    init_db()   – create all tables (call once at startup)
    get_db()    – return the shared DB instance
    DB          – thin async wrapper around sqlite3.Connection
    Row         – sqlite3.Row wrapper that supports attribute-style access
                  and automatically deserialises datetime/JSON columns
"""

import json
import sqlite3
import asyncio
import concurrent.futures
import logging
from datetime import datetime
from typing import Any, Optional

from packages.core.config import settings

logger = logging.getLogger(__name__)

# ── DB path ───────────────────────────────────────────────────────────────────
_raw_url: str = settings.database.url          # e.g. "sqlite+aiosqlite:///./polymarket_alpha.db"
_DB_PATH: str = (
    _raw_url
    .replace("sqlite+aiosqlite:///", "")
    .replace("sqlite:///", "")
)

# ── Column sets for automatic type conversion ─────────────────────────────────
_DATETIME_COLS: frozenset = frozenset({
    "created_at", "updated_at", "last_updated", "calculated_at",
    "received_at", "timestamp", "end_date_iso", "snapshot_at", "closed_at",
})
_JSON_COLS: frozenset = frozenset({"top_traders", "raw_payload"})

_DT_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
)


def _parse_dt(value: str) -> Optional[datetime]:
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


# ── Row wrapper ───────────────────────────────────────────────────────────────

class Row:
    """
    Wraps a sqlite3.Row so callers can use either row["col"] or row.col.
    Datetime TEXT columns are automatically parsed to datetime objects.
    JSON TEXT columns are automatically parsed to Python objects.
    """
    __slots__ = ("_d",)

    def __init__(self, raw: sqlite3.Row) -> None:
        d: dict = {}
        for k in raw.keys():
            v = raw[k]
            if v is not None:
                if k in _DATETIME_COLS and isinstance(v, str):
                    v = _parse_dt(v) or v
                elif k in _JSON_COLS and isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        pass
            d[k] = v
        self._d = d

    # dict-style access
    def __getitem__(self, key: str) -> Any:
        return self._d[key]

    # attribute-style access (replaces ORM object attribute access)
    def __getattr__(self, name: str) -> Any:
        try:
            return self._d[name]
        except KeyError:
            raise AttributeError(f"Row has no column '{name}'")

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def keys(self):
        return self._d.keys()

    def __repr__(self) -> str:
        return f"Row({self._d!r})"


# ── Connection factory ────────────────────────────────────────────────────────

def _make_connection() -> sqlite3.Connection:
    # isolation_level=None → autocommit; prevents implicit transactions from
    # piling up across asyncio.to_thread calls and causing SQLITE_LOCKED errors.
    # We call conn.commit() explicitly where we need atomicity.
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    return conn


_connection: Optional[sqlite3.Connection] = None

# Single-thread executor: ensures all sqlite3 calls run on the same OS thread,
# preventing SQLITE_LOCKED errors that occur when asyncio's default thread pool
# dispatches concurrent DB calls to different threads on the same connection.
_db_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="sqlite_worker"
)


def _conn() -> sqlite3.Connection:
    global _connection
    if _connection is None:
        _connection = _make_connection()
    return _connection


def _run_in_db_thread(fn):
    """Schedule fn() on the dedicated sqlite worker thread and return a Future."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_db_executor, fn)


# ── Transaction context manager ───────────────────────────────────────────────

class _TransactionCtx:
    def __init__(self, db: "DB") -> None:
        self._db = db

    async def __aenter__(self):
        await self._db.begin()
        return self._db

    async def __aexit__(self, exc_type, _exc, _tb):
        if exc_type is None:
            await self._db.commit()
        else:
            await self._db.rollback()


# ── DB class ──────────────────────────────────────────────────────────────────

class DB:
    """
    Async wrapper around a shared sqlite3 connection.
    All blocking DB calls run on a single dedicated thread (_db_executor) so
    that sqlite3's internal mutex is never contested across OS threads.
    """

    # ── Writes ────────────────────────────────────────────────────────────────

    async def execute(self, sql: str, params=()) -> sqlite3.Cursor:
        """Execute a single statement; return the cursor (for lastrowid etc.)."""
        def _run():
            return _conn().execute(sql, params)
        return await _run_in_db_thread(_run)

    async def executemany(self, sql: str, seq) -> None:
        """Execute a statement for each item in seq."""
        def _run():
            _conn().executemany(sql, seq)
        await _run_in_db_thread(_run)

    async def commit(self) -> None:
        # With isolation_level=None (autocommit) this is a no-op unless we
        # opened a manual BEGIN; kept for call-site compatibility.
        await _run_in_db_thread(_conn().commit)

    async def rollback(self) -> None:
        await _run_in_db_thread(_conn().rollback)

    async def begin(self) -> None:
        """Start an explicit transaction (needed in autocommit mode)."""
        def _run():
            _conn().execute("BEGIN")
        await _run_in_db_thread(_run)

    async def transaction(self):
        """Async context manager for explicit BEGIN/COMMIT/ROLLBACK."""
        return _TransactionCtx(self)

    # ── Reads ─────────────────────────────────────────────────────────────────

    async def fetchone(self, sql: str, params=()) -> Optional[Row]:
        """Return the first row, or None."""
        def _run():
            raw = _conn().execute(sql, params).fetchone()
            return Row(raw) if raw is not None else None
        return await _run_in_db_thread(_run)

    async def fetchall(self, sql: str, params=()) -> list:
        """Return all rows as a list of Row objects."""
        def _run():
            return [Row(r) for r in _conn().execute(sql, params).fetchall()]
        return await _run_in_db_thread(_run)

    async def fetchval(self, sql: str, params=()) -> Any:
        """Return the first column of the first row, or None."""
        def _run():
            raw = _conn().execute(sql, params).fetchone()
            return raw[0] if raw is not None else None
        return await _run_in_db_thread(_run)


# ── Singleton ─────────────────────────────────────────────────────────────────

_db_instance: Optional[DB] = None


def get_db() -> DB:
    """Return the shared DB instance (created on first call)."""
    global _db_instance
    if _db_instance is None:
        _db_instance = DB()
    return _db_instance


# ── Schema ────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables and indices if they do not already exist."""
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id          TEXT PRIMARY KEY,
            title       TEXT NOT NULL,
            description TEXT,
            category    TEXT,
            active      INTEGER NOT NULL DEFAULT 1,
            closed      INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS markets (
            id                TEXT PRIMARY KEY,
            event_id          TEXT NOT NULL REFERENCES events(id),
            question          TEXT NOT NULL,
            slug              TEXT NOT NULL UNIQUE,
            active            INTEGER NOT NULL DEFAULT 1,
            closed            INTEGER NOT NULL DEFAULT 0,
            resolution_source TEXT,
            end_date_iso      TEXT,
            market_type       TEXT NOT NULL DEFAULT 'binary'
        );

        CREATE TABLE IF NOT EXISTS outcomes (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL REFERENCES markets(id),
            name      TEXT NOT NULL,
            asset_id  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_tags (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL REFERENCES markets(id),
            tag       TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_market_tags_tag ON market_tags(tag);

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id  TEXT    NOT NULL REFERENCES markets(id),
            outcome_id INTEGER NOT NULL REFERENCES outcomes(id),
            best_bid   REAL    NOT NULL,
            best_ask   REAL    NOT NULL,
            mid_price  REAL    NOT NULL,
            timestamp  TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_price_snapshots_timestamp
            ON price_snapshots(timestamp);

        CREATE TABLE IF NOT EXISTS trader_wallets (
            address       TEXT PRIMARY KEY,
            owner_address TEXT,
            ens_name      TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS ix_trader_wallets_owner
            ON trader_wallets(owner_address);

        CREATE TABLE IF NOT EXISTS raw_trade_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT NOT NULL UNIQUE,
            source      TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            received_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_raw_trade_events_ext
            ON raw_trade_events(external_id);

        CREATE TABLE IF NOT EXISTS trades (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id        TEXT    NOT NULL REFERENCES markets(id),
            outcome_id       INTEGER NOT NULL REFERENCES outcomes(id),
            trader_address   TEXT    NOT NULL REFERENCES trader_wallets(address),
            side             TEXT    NOT NULL,
            price            REAL    NOT NULL,
            size             REAL    NOT NULL,
            notional         REAL    NOT NULL,
            transaction_hash TEXT    NOT NULL,
            timestamp        TEXT    NOT NULL,
            is_reprice       INTEGER NOT NULL DEFAULT 0,
            clv_score        REAL
        );
        CREATE INDEX IF NOT EXISTS ix_trades_tx   ON trades(transaction_hash);
        CREATE INDEX IF NOT EXISTS ix_trades_ts   ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS ix_trades_addr ON trades(trader_address);

        CREATE TABLE IF NOT EXISTS trader_profiles (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            address            TEXT    NOT NULL UNIQUE,
            total_trades       INTEGER NOT NULL DEFAULT 0,
            win_rate           REAL    NOT NULL DEFAULT 0.0,
            profit_loss        REAL    NOT NULL DEFAULT 0.0,
            avg_clv            REAL    NOT NULL DEFAULT 0.0,
            median_clv         REAL    NOT NULL DEFAULT 0.0,
            directional_purity REAL    NOT NULL DEFAULT 0.0,
            gamma_score        REAL    NOT NULL DEFAULT 0.0,
            last_updated       TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS ix_trader_profiles_addr
            ON trader_profiles(address);

        CREATE TABLE IF NOT EXISTS trader_classifications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            address    TEXT NOT NULL UNIQUE,
            label      TEXT NOT NULL,
            confidence REAL NOT NULL,
            reasoning  TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS ix_trader_classifications_addr
            ON trader_classifications(address);

        CREATE TABLE IF NOT EXISTS trader_score_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            address          TEXT    NOT NULL REFERENCES trader_wallets(address),
            repricing_score  REAL    NOT NULL,
            resolution_score REAL    NOT NULL,
            composite_score  REAL    NOT NULL,
            topic            TEXT,
            sample_size      INTEGER NOT NULL,
            calculated_at    TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS ix_trader_score_topic
            ON trader_score_snapshots(topic);

        CREATE TABLE IF NOT EXISTS market_signal_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id        TEXT    NOT NULL REFERENCES markets(id),
            outcome_id       INTEGER NOT NULL REFERENCES outcomes(id),
            signal_type      TEXT    NOT NULL,
            signal_strength  REAL    NOT NULL,
            directional_bias TEXT    NOT NULL,
            explanation      TEXT    NOT NULL,
            top_traders      TEXT    NOT NULL DEFAULT '[]',
            created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS position_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_address  TEXT    NOT NULL REFERENCES trader_wallets(address),
            market_id       TEXT    NOT NULL REFERENCES markets(id),
            outcome_id      INTEGER NOT NULL REFERENCES outcomes(id),
            current_size    REAL    NOT NULL,
            avg_entry_price REAL    NOT NULL,
            unrealized_pnl  REAL    NOT NULL DEFAULT 0.0,
            snapshot_at     TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS closed_positions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_address  TEXT    NOT NULL REFERENCES trader_wallets(address),
            market_id       TEXT    NOT NULL REFERENCES markets(id),
            outcome_id      INTEGER NOT NULL REFERENCES outcomes(id),
            buy_size        REAL    NOT NULL,
            buy_avg_price   REAL    NOT NULL,
            sell_size       REAL    NOT NULL,
            sell_avg_price  REAL    NOT NULL,
            realized_pnl    REAL    NOT NULL,
            realized_edge   REAL    NOT NULL,
            closed_at       TEXT    NOT NULL DEFAULT (datetime('now'))
        );
    """)
    c.commit()
    logger.info("Database tables created / verified.")
