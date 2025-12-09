import asyncio
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from src.jobs import backfill_full_range, process_feed_cycle, hydrate_order, hydrate_range
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

@router.post("/anymarket/hydrate-range")
async def anymarket_hydrate_range(start: str = Body(..., embed=True)):
    """
    Reprocessa todos os pedidos já existentes na base a partir de `start`
    (ex: "2025-10-01").
    """
    asyncio.create_task(hydrate_range(start))  # se quiser rodar async em background
    return {"ok": True}