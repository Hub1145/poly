import json
import logging
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from eth_account import Account

# Configure default logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("polymarket_alpha")

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
SETTINGS_FILE = PROJECT_ROOT / "settings.json"

class DatabaseSettings(BaseModel):
    url: str = "sqlite+aiosqlite:///./polymarket_alpha.db"
    pool_size: int = 5
    max_overflow: int = 10

class PolymarketSettings(BaseModel):
    private_key: str = ""
    clob_api_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"

    @property
    def wallet_address(self) -> str:
        """Derive the wallet address from the private key."""
        if not self.private_key or self.private_key == "0x" + "0"*64 or self.private_key == "":
            return ""
        try:
            return Account.from_key(self.private_key).address
        except Exception:
            return ""

class AppSettings(BaseModel):
    log_level: str = "INFO"
    paper_mode: bool = True
    execution_enabled: bool = False
    # Trading parameters persisted by the UI Settings tab via /api/config
    trade_amount: float = 10.0      # total ladder budget per city, split across legs
    min_edge: float = 0.12          # raised for higher conviction filter
    scan_interval: int = 30
    paper_balance: float = 1000.0
    max_trades: int = 8             # concentrate capital

# Hardcoded weather strategy constants — not user-configurable.
WEATHER_MIN_GAP_PP: float        = 12.0   # min %-point gap model vs market price
WEATHER_MIN_TEMP_GAP_C: float    = 2.0    # min °C ensemble mean vs market threshold
WEATHER_LADDER_LEGS: int         = 4      # max YES legs per city per day
WEATHER_BURST_SCAN_MINUTES: int  = 90     # burst-scan window after each model run

class WeatherSettings(BaseModel):
    # Phase 1 city whitelist — best daily volume + Open-Meteo coverage
    city_focus: list = ["NYC", "New York", "London", "Seoul", "Buenos Aires"]
    model_run_hours: list = [0, 6, 12, 18]   # UTC hours NOAA/ECMWF/GFS update

class ScoringSettings(BaseModel):
    min_trade_count: int = 10
    clv_weight: float = 0.4
    realized_edge_weight: float = 0.4
    topic_specialist_bonus: float = 0.2

class Settings(BaseSettings):
    strategy: str = "bayesian_ensemble"
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    polymarket: PolymarketSettings = Field(default_factory=PolymarketSettings)
    app: AppSettings = Field(default_factory=AppSettings)
    scoring: ScoringSettings = Field(default_factory=ScoringSettings)
    weather: WeatherSettings = Field(default_factory=WeatherSettings)

    class Config:
        extra = "ignore"
        # All configuration lives in settings.json — never read from .env files
        # or environment variables so there is no ambiguity about the source of truth.
        env_file = None
        env_ignore_empty = True

    @classmethod
    def load(cls) -> "Settings":
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, "r") as f:
                try:
                    raw = json.load(f)
                    # The UI saves a flat dict (strategy, paper_mode, trade_amount …).
                    # Remap flat keys into the nested sub-model structure that Settings expects.
                    _app_keys = {
                        "paper_mode", "trade_amount", "min_edge",
                        "scan_interval", "paper_balance", "max_trades",
                    }
                    _poly_keys = {"private_key"}

                    app_overrides  = {k: raw[k] for k in _app_keys  if k in raw}
                    poly_overrides = {k: raw[k] for k in _poly_keys if k in raw}

                    top_level = {k: v for k, v in raw.items()
                                 if k not in _app_keys and k not in _poly_keys}

                    if app_overrides:
                        existing_app = top_level.pop("app", {})
                        top_level["app"] = {**existing_app, **app_overrides}
                    if poly_overrides:
                        existing_poly = top_level.pop("polymarket", {})
                        top_level["polymarket"] = {**existing_poly, **poly_overrides}

                    return cls(**top_level)
                except Exception as e:
                    logger.error(f"Failed to load settings from {SETTINGS_FILE}: {e}")

        logger.warning(f"Settings file {SETTINGS_FILE} not found or invalid. Using defaults.")
        return cls()

settings = Settings.load()

# Update log level from settings
logging.getLogger().setLevel(settings.app.log_level)
