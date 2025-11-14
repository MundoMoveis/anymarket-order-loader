from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from src.jobs import backfill_full_range
from src.log import log

router = APIRouter()

@router.get("/health")
async def health():
    return {"ok": True}

@router.post("/sync/feed")
async def sync_feed():
    try:
        return await process_feed_cycle()
    except Exception as e:
        raise HTTPException(500, str(e))

@router.post("/sync/order/{order_id}")
async def sync_one(order_id: str):
    try:
        return await hydrate_order(order_id)
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/anymarket/backfill")
async def anymarket_backfill(
    start: datetime = Query(..., description="Data/hora inicial"),
    end: datetime = Query(..., description="Data/hora final"),
):
    try:
        result = await backfill_full_range(start, end)
        return result
    except Exception as e:
        log.exception("backfill error")
        raise HTTPException(status_code=500, detail=str(e))
