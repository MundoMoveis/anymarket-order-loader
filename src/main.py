import asyncio
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import uvicorn

from src.api import router
from src.config import Cfg
from src.jobs import process_feed_cycle, backfill_recent, backfill_rolling
from src.log import log

app = FastAPI()
app.include_router(router)

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def on_startup():
    async def run_feed():
        try:
            out = await process_feed_cycle()
            # loga se teve trabalho
            if out.get("fetched", 0) > 0 or out.get("errors", 0) > 0:
                log.info(f"feed cycle: {out}")
        except Exception as e:
            log.error(f"feed cycle error: {e}")

    async def run_backfill_recent_job():
        try:
            out = await backfill_recent()
            if not out.get("skipped"):
                log.info(f"backfill recent: {out}")
            else:
                log.info(f"backfill recent skipped: {out}")
        except Exception as e:
            log.error(f"backfill recent error: {e}")

    async def run_backfill_rolling_job():
        try:
            out = await backfill_rolling()
            if not out.get("skipped"):
                log.info(f"backfill rolling: {out}")
            else:
                log.info(f"backfill rolling skipped: {out}")
        except Exception as e:
            log.error(f"backfill rolling error: {e}")

    # Feed
    if Cfg.ANY_CRON.strip():
        scheduler.add_job(run_feed, CronTrigger.from_crontab(Cfg.ANY_CRON), id="feed")
        log.info(f"cron feed ready: {Cfg.ANY_CRON}")
    else:
        log.info("cron feed disabled (ANY_CRON vazio)")

    # Backfill recent
    if Cfg.ANY_BACKFILL_RECENT_CRON.strip():
        scheduler.add_job(
            run_backfill_recent_job,
            CronTrigger.from_crontab(Cfg.ANY_BACKFILL_RECENT_CRON),
            id="backfill_recent",
        )
        log.info(f"cron backfill_recent ready: {Cfg.ANY_BACKFILL_RECENT_CRON}")

    # Backfill rolling
    if Cfg.ANY_BACKFILL_ROLLING_CRON.strip():
        scheduler.add_job(
            run_backfill_rolling_job,
            CronTrigger.from_crontab(Cfg.ANY_BACKFILL_ROLLING_CRON),
            id="backfill_rolling",
        )
        log.info(f"cron backfill_rolling ready: {Cfg.ANY_BACKFILL_ROLLING_CRON}")

    scheduler.start()
