import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import logging

# Configure logging
logger = logging.getLogger(__name__)

load_dotenv()

# Get ROOT_PATH from .env
ROOT_DIR = os.getenv("ROOT_DIR")
if not ROOT_DIR:
    logger.error("ROOT_DIR not set in .env file")
    raise ValueError("ROOT_DIR not set in .env file")

ROOT_DIR = Path(ROOT_DIR).resolve()
if not ROOT_DIR.exists():
    logger.error(f"ROOT_PATH directory does not exist: {ROOT_DIR}")
    raise FileNotFoundError(f"ROOT_PATH directory does not exist: {ROOT_DIR}")

# Add project root to sys.path if not already present
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


ENV_FILE = ROOT_DIR / "microservice" / ".env"

SESSION_CONFIG_FILE = ROOT_DIR / "microservice" / "web_processor.yml"

LOG_DIR = ROOT_DIR / "logs"
DATA_DIR = ROOT_DIR / "data"

PNL_DATA_DIR = DATA_DIR / "pnl_data"
PRICES_DIR = PNL_DATA_DIR / "price_data"


PRICES_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


print(f"Paths initialized: ROOT_DIR={ROOT_DIR}, CONFIG_FILE={SESSION_CONFIG_FILE }, DATA_DIR={DATA_DIR}")