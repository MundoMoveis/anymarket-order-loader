import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.api import router
from src.config import Cfg
from src.jobs import process_feed_cycle, backfill_recent, backfill_rolling
from src.log import log

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    async def run_feed():
        try:
            out = await process_feed_cycle()
            if out.get("fetched", 0) > 0 or out.get("errors", 0) > 0:
                log.info(f"feed cycle: {out}")
        except Exception as e:
            log.exception("feed cycle error: %s", e)

    async def run_backfill_recent_job():
        try:
            out = await backfill_recent()
            log.info("backfill recent: %s", out)
        except Exception as e:
            log.exception("backfill recent error: %s", e)

    async def run_backfill_rolling_job():
        try:
            out = await backfill_rolling()
            log.info("backfill rolling: %s", out)
        except Exception as e:
            log.exception("backfill rolling error: %s", e)

    try:
        # Feed
        if Cfg.ANY_CRON.strip():
            scheduler.add_job(run_feed, CronTrigger.from_crontab(Cfg.ANY_CRON), id="feed")
            log.info("cron feed ready: %s", Cfg.ANY_CRON)
        else:
            log.info("cron feed disabled (ANY_CRON vazio)")

        # Backfill recent
        if getattr(Cfg, "ANY_BACKFILL_RECENT_CRON", "").strip():
            scheduler.add_job(
                run_backfill_recent_job,
                CronTrigger.from_crontab(Cfg.ANY_BACKFILL_RECENT_CRON),
                id="backfill_recent",
            )
            log.info("cron backfill_recent ready: %s", Cfg.ANY_BACKFILL_RECENT_CRON)

        # Backfill rolling
        if getattr(Cfg, "ANY_BACKFILL_ROLLING_CRON", "").strip():
            scheduler.add_job(
                run_backfill_rolling_job,
                CronTrigger.from_crontab(Cfg.ANY_BACKFILL_ROLLING_CRON),
                id="backfill_rolling",
            )
            log.info("cron backfill_rolling ready: %s", Cfg.ANY_BACKFILL_ROLLING_CRON)

        scheduler.start()
        yield
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=Cfg.PORT, reload=False)
