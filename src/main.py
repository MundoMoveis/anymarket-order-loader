import asyncio
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import uvicorn

from src.api import router
from src.config import Cfg
from src.jobs import process_feed_cycle
from src.log import log

app = FastAPI()
app.include_router(router)

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def on_startup():
    async def run_cycle():
        try:
            out = await process_feed_cycle()
            if out.get("processed"):
                log.info(f"feed cycle: {out}")
        except Exception as e:
            log.error(f"feed cycle error: {e}")

    # SÓ cria o cron se ANY_CRON não for vazio
    if Cfg.ANY_CRON.strip():
        scheduler.add_job(run_cycle, CronTrigger.from_crontab(Cfg.ANY_CRON), id="feed")
        scheduler.start()
        log.info(f"cron ready: {Cfg.ANY_CRON}")
    else:
        log.info("cron disabled (ANY_CRON vazio)")


if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=Cfg.PORT, reload=False)
