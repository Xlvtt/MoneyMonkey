import os
from pathlib import Path

from dotenv import load_dotenv

_PKG_DIR = Path(__file__).resolve().parent
_REPO_DIR = _PKG_DIR.parent
load_dotenv(_REPO_DIR / ".env")
load_dotenv(_PKG_DIR / ".env")
load_dotenv()


def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "data/moneymonkey.db"))
OAUTH_CLIENT_SECRET_PATH = os.getenv("OAUTH_CLIENT_SECRET_PATH", "")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "")
TOKEN_ENCRYPTION_KEY = os.getenv("TOKEN_ENCRYPTION_KEY", "")
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8080"))


def validate_config() -> None:
    _require("BOT_TOKEN")
    _require("OAUTH_CLIENT_SECRET_PATH")
    _require("OAUTH_REDIRECT_URI")
    _require("TOKEN_ENCRYPTION_KEY")
    oauth_secret = Path(OAUTH_CLIENT_SECRET_PATH)
    if not oauth_secret.is_file():
        raise RuntimeError(f"OAUTH_CLIENT_SECRET_PATH is not a file: {oauth_secret}")
