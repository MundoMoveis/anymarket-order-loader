from dotenv import load_dotenv
import os

load_dotenv()

class Cfg:
    ANY_BACKFILL_MAX_DAYS = int(os.getenv("ANY_BACKFILL_MAX_DAYS", "90"))  # menor que 120
    PORT = int(os.getenv("PORT", "8080"))
    ANY_BASE = os.getenv("ANYMARKET_BASE_URL", "https://api.anymarket.com.br/v2")
    ANY_TOKEN = os.getenv("ANYMARKET_TOKEN") or os.getenv("ANYMARKET_GUNGA_TOKEN", "")
    ANY_AUTH_MODE = os.getenv("ANYMARKET_AUTH_MODE", "bearer").lower()  # 'api_key' ou 'bearer'
    ANY_API_KEY = os.getenv("ANYMARKET_API_KEY", "")
    ANY_API_KEY_HEADER = os.getenv("ANYMARKET_API_KEY_HEADER", "gumgaToken")
    ANY_PAGE = int(os.getenv("ANY_FEED_PAGE_SIZE", "100"))
    ANY_MAX_MS = int(os.getenv("ANY_MAX_MS_PER_CYCLE", "20000"))
    ANY_CRON = os.getenv("ANY_CRON", "*/5 * * * *")

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "3306"))
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASS = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_DATABASE", "anymarket")
    DB_POOL = int(os.getenv("DB_POOL_SIZE", "10"))
    DB_OVER = int(os.getenv("DB_POOL_OVERFLOW", "5"))
