
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Configure logging
logger = logging.getLogger(__name__)


load_dotenv()

# Get ROOT_PATH from .env
ROOT_PATH = os.getenv("ROOT_PATH")
if not ROOT_PATH:
    logger.error("ROOT_PATH not set in .env file")
    raise ValueError("ROOT_PATH not set in .env file")

ROOT_DIR = Path(ROOT_PATH).resolve()
if not ROOT_DIR.exists():
    logger.error(f"ROOT_PATH directory does not exist: {ROOT_DIR}")
    raise FileNotFoundError(f"ROOT_PATH directory does not exist: {ROOT_DIR}")

# Define paths
CONFIG_FILE = ROOT_DIR / "config_pair_session_bitget.json"
OUTPUT_DIR = ROOT_DIR / "output_bitget"
LOG_DIR = ROOT_DIR / "logs"
BOT_DATA_DIR = ROOT_DIR / "bot_data"
ERROR_FLAGS_PATH = OUTPUT_DIR / "hedge_error_flags.json"
ENV_FILE = ROOT_DIR / ".env"

# File path templates
BITGET_POSITIONS_FILE = lambda account: OUTPUT_DIR / f"bitget_positions_{account}.json"
STRATEGY_STATE_FILE = lambda strategy: OUTPUT_DIR / f"current_state_{strategy}.json"
CURRENT_STATE_FILE = lambda strategy: BOT_DATA_DIR / strategy / "current_state.json"
AGGREGATED_POSITIONS_FILE = lambda account: OUTPUT_DIR / f"aggregated_positions_{account}.json"
BITGET_POSITIONS_FILE= lambda account: OUTPUT_DIR / f"bitget_positions_{account}.json"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
BOT_DATA_DIR.mkdir(parents=True, exist_ok=True)

logger.debug(f"Paths initialized: ROOT_DIR={ROOT_DIR}, CONFIG_FILE={CONFIG_FILE}, OUTPUT_DIR={OUTPUT_DIR}, BOT_DATA_DIR={BOT_DATA_DIR}")