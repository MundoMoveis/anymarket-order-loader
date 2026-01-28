from dotenv import load_dotenv
import os

load_dotenv()

class Cfg:
    # Timezone (para cálculo de janelas)
    TZ = os.getenv("TZ", "America/Sao_Paulo")

    # Backfill
    # tamanho máximo de cada JANELA interna do backfill_full_range
    ANY_BACKFILL_WINDOW_DAYS = int(os.getenv("ANY_BACKFILL_WINDOW_DAYS", "1"))

    # rolling windows (range externo)
    ANY_BACKFILL_RECENT_DAYS = int(os.getenv("ANY_BACKFILL_RECENT_DAYS", "7"))
    ANY_BACKFILL_ROLLING_DAYS = int(os.getenv("ANY_BACKFILL_ROLLING_DAYS", "90"))

    # crons (vazio = desabilita)
    ANY_BACKFILL_RECENT_CRON = os.getenv("ANY_BACKFILL_RECENT_CRON", "10 */6 * * *")
    ANY_BACKFILL_ROLLING_CRON = os.getenv("ANY_BACKFILL_ROLLING_CRON", "30 6,18 * * *")

    # lock
    ANY_BACKFILL_LOCK_NAME = os.getenv("ANY_BACKFILL_LOCK_NAME", "anymarket:backfill")
    ANY_BACKFILL_LOCK_TIMEOUT_SEC = int(os.getenv("ANY_BACKFILL_LOCK_TIMEOUT_SEC", "0"))

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
